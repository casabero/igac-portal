import pandas as pd
import numpy as np
import io
import os
import zipfile
import shutil
import tempfile
try:
    import geopandas as gpd
    import fiona
    HAS_GEO = True
except ImportError:
    HAS_GEO = False

def parse_code(serie):
    """Extrae las partes del código predial de 30 dígitos"""
    return pd.DataFrame({
        'GRUPO_GEO': serie.str[0:17], # Dpto (2) + Mun (3) + Zona (2) + Sector (2) + Comuna (2) + Barrio (2) + Manzana (4)
        'DEPTO_MUN': serie.str[0:5],
        'ZONA': serie.str[5:7],
        'SECTOR': serie.str[7:9],
        'MANZANA': serie.str[13:17],
        'TERRENO': serie.str[17:21],
        'COMPLETO': serie
    })

def es_provisional(serie):
    """Detecta si un código es provisional (empieza por 9 o tiene letras)"""
    return serie.str.startswith('9') | serie.str.contains('[A-Z]', regex=True, na=False)

def procesar_renumeracion(file_stream, tipo_config):
    """
    Fase 1: Auditoría Alfanumérica.
    Retorna errores y un diccionario de referencia de estados.
    """
    if tipo_config == '1':
        col_anterior = 'Número predial CICA'
    else:
        col_anterior = 'Número predial LC_PREDIO'
    
    col_nuevo = 'Número predial SNC'
    col_estado = 'Estado'

    try:
        df_full = pd.read_excel(file_stream, dtype=str)
    except Exception as e:
        raise ValueError(f"Error al leer el archivo Excel: {str(e)}")

    columnas_requeridas = [col_nuevo, col_anterior, col_estado]
    faltantes = [c for c in columnas_requeridas if c not in df_full.columns]
    if faltantes:
        raise ValueError(f"Faltan las columnas requeridas: {', '.join(faltantes)}")

    # Limpieza
    df_full[col_anterior] = df_full[col_anterior].str.strip()
    df_full[col_nuevo] = df_full[col_nuevo].str.strip()
    df_full[col_estado] = df_full[col_estado].str.strip().str.upper()

    # Diccionario de referencia para Fase 2 (TODOS los estados)
    # {CODIGO_SNC: ESTADO}
    diccionario_estados = pd.Series(df_full[col_estado].values, index=df_full[col_nuevo]).to_dict()

    # Filtrar ACTIVOS para auditoría Alfanumérica
    df_audit = df_full[df_full[col_estado] == 'ACTIVO'].copy()
    df_audit = df_audit.sort_values(by=[col_nuevo])

    if len(df_audit) == 0:
        return {
            'total_auditado': 0,
            'errores': [],
            'stats': {},
            'diccionario_estados': diccionario_estados,
            'success': True
        }

    # Parsers
    df_ant = parse_code(df_audit[col_anterior])
    df_nue = parse_code(df_audit[col_nuevo])
    todos_errores = []

    # --- [1] UNICIDAD ---
    duplicados = df_audit[df_audit.duplicated(subset=[col_nuevo], keep=False)]
    for _, row in duplicados.iterrows():
        todos_errores.append({
            'REGLA': '1. UNICIDAD',
            'DETALLE': 'Número duplicado en activos',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # --- [2] PERMANENCIA ---
    es_nuevo_ant = es_provisional(df_ant['MANZANA']) | es_provisional(df_ant['TERRENO'])
    cambios_ilegales = df_audit[(~es_nuevo_ant) & (df_ant['COMPLETO'] != df_nue['COMPLETO'])]
    for _, row in cambios_ilegales.iterrows():
        todos_errores.append({
            'REGLA': '2. PERMANENCIA',
            'DETALLE': 'Predio viejo cambió de número indebidamente',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # --- [3] LIMPIEZA ---
    sucios = (es_provisional(df_nue['ZONA']) | es_provisional(df_nue['SECTOR']) |
              es_provisional(df_nue['MANZANA']) | es_provisional(df_nue['TERRENO']))
    errores_limpieza = df_audit[sucios]
    for _, row in errores_limpieza.iterrows():
        todos_errores.append({
            'REGLA': '3. LIMPIEZA',
            'DETALLE': 'Códigos temporales/letras en definitivo (SNC)',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # --- [4] CONSECUTIVOS ---
    mask_t_nuevo = es_provisional(df_ant['TERRENO']) & ~es_provisional(df_ant['MANZANA'])
    if mask_t_nuevo.any():
        df_revisar = df_audit[mask_t_nuevo].copy()
        df_revisar['GRUPO'] = df_nue.loc[mask_t_nuevo, 'GRUPO_GEO']
        
        for grupo_mza, datos_nuevos in df_revisar.groupby('GRUPO'):
            mask_existentes = (df_nue['GRUPO_GEO'] == grupo_mza) & (~es_nuevo_ant)
            predios_viejos = df_audit[mask_existentes]

            if not predios_viejos.empty:
                max_viejo = int(parse_code(predios_viejos[col_nuevo])['TERRENO'].max())
                for _, row in datos_nuevos.iterrows():
                    try:
                        terr_asignado = int(row[col_nuevo][17:21])
                        if terr_asignado <= max_viejo:
                            todos_errores.append({
                                'REGLA': '4. CONSECUTIVO TERRENO',
                                'DETALLE': f'Asignado {terr_asignado} <= Máx existente {max_viejo}',
                                'ANTERIOR': row[col_anterior],
                                'NUEVO': row[col_nuevo]
                            })
                    except: pass

    # --- [5] REINICIO EN MANZANAS NUEVAS ---
    mask_mza_nueva = es_provisional(df_ant['MANZANA']) & ~es_provisional(df_ant['SECTOR'])
    try:
        errores_mza = df_audit[mask_mza_nueva & (df_nue['TERRENO'].astype(int) > 50)]
        for _, row in errores_mza.iterrows():
            todos_errores.append({
                'REGLA': '5. MANZANA NUEVA',
                'DETALLE': 'Terreno > 50 en manzana nueva. ¿Faltó reiniciar?',
                'ANTERIOR': row[col_anterior],
                'NUEVO': row[col_nuevo]
            })
    except: pass

    # --- [6] REINICIO EN SECTORES NUEVOS ---
    mask_sec_nuevo = es_provisional(df_ant['SECTOR'])
    try:
        errores_sec = df_audit[mask_sec_nuevo & (df_nue['MANZANA'].astype(int) > 20)]
        for _, row in errores_sec.iterrows():
            todos_errores.append({
                'REGLA': '6. SECTOR NUEVO',
                'DETALLE': 'Manzana > 20 en sector nuevo.',
                'ANTERIOR': row[col_anterior],
                'NUEVO': row[col_nuevo]
            })
    except: pass

    # Estadísticas por regla
    df_err = pd.DataFrame(todos_errores) if todos_errores else pd.DataFrame(columns=['REGLA'])
    stats = df_err['REGLA'].value_counts().to_dict() if not df_err.empty else {}

    return {
        'total_auditado': len(df_audit),
        'errores': todos_errores,
        'stats': stats,
        'diccionario_estados': diccionario_estados,
        'df_referencia': df_audit[[col_nuevo, col_anterior]].rename(columns={col_nuevo: 'CODIGO_SNC', col_anterior: 'CODIGO_ANTERIOR'}),
        'success': True
    }

def extraer_datos_gdb(zip_stream, capas_objetivo):
    """Extrae predios de un ZIP que contiene una GDB"""
    if not HAS_GEO:
        return pd.DataFrame(), ["Librerías geoespaciales no instaladas."]
    
    predios = []
    errores = []
    
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "upload.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_stream.read())
        
        extract_path = os.path.join(tmpdir, "extract")
        os.makedirs(extract_path)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(extract_path)
        except Exception as e:
            return pd.DataFrame(), [f"Error al descomprimir ZIP: {str(e)}"]

        gdb_path = None
        for root, dirs, files in os.walk(extract_path):
            for d in dirs:
                if d.endswith(".gdb"):
                    gdb_path = os.path.join(root, d)
                    break
            if gdb_path: break
        
        if not gdb_path:
            return pd.DataFrame(), ["No se encontró ninguna .gdb dentro del ZIP."]

        try:
            layers = fiona.listlayers(gdb_path)
            for capa in capas_objetivo:
                if capa in layers:
                    try:
                        gdf = gpd.read_file(gdb_path, layer=capa)
                        if 'CODIGO' in gdf.columns:
                            predios.extend(gdf['CODIGO'].astype(str).str.strip().tolist())
                    except Exception as e:
                        errores.append(f"Error leyendo capa {capa}: {str(e)}")
        except Exception as e:
            errores.append(f"Error al listar capas de la GDB: {str(e)}")

    return pd.DataFrame({'CODIGO': predios}), errores

def procesar_geografica(zip_formal, zip_informal, set_alfa_activos, diccionario_estados, df_alfa_ref):
    """
    Fase 2: Cruce Geográfico.
    """
    capas_formal = ['U_TERRENO', 'R_TERRENO', 'TERRENO']
    capas_informal = ['U_TERRENO_INFORMAL', 'R_TERRENO_INFORMAL', 'TERRENO_INFORMAL']
    
    list_geo = []
    errores_internos = []
    
    if zip_formal:
        df_f, err_f = extraer_datos_gdb(zip_formal, capas_formal)
        list_geo.append(df_f)
        for e in err_f: errores_internos.append({'TIPO': 'ERROR GDB FORMAL', 'DETALLE': e, 'CODIGO': 'N/A', 'ESTADO_BD': 'N/A', 'ACCION_SUGERIDA': 'Revisar ZIP/GDB'})
    
    if zip_informal:
        df_i, err_i = extraer_datos_gdb(zip_informal, capas_informal)
        list_geo.append(df_i)
        for e in err_i: errores_internos.append({'TIPO': 'ERROR GDB INFORMAL', 'DETALLE': e, 'CODIGO': 'N/A', 'ESTADO_BD': 'N/A', 'ACCION_SUGERIDA': 'Revisar ZIP/GDB'})

    if not list_geo or all(df.empty for df in list_geo):
        return [], errores_internos

    df_geo_total = pd.concat(list_geo).drop_duplicates()
    set_geo = set(df_geo_total['CODIGO'])
    set_alfa = set_alfa_activos
    
    reporte = []
    
    # 1. Faltan en GDB (Están en Excel Activos, no en GDB)
    sin_mapa = set_alfa - set_geo
    for cod in sin_mapa:
        reporte.append({
            'TIPO': 'FALTA EN GDB',
            'DETALLE': 'Predio Activo en Excel no encontrado en Geometría',
            'CODIGO': cod,
            'ESTADO_BD': 'ACTIVO',
            'ACCION_SUGERIDA': 'Dibujar predio o revisar vigencia'
        })
        
    # 2. Sobran en GDB (Están en GDB, no están en Excel Activos)
    sin_alfa = set_geo - set_alfa
    for cod in sin_alfa:
        estado_real = diccionario_estados.get(cod, "NO EXISTE EN BD")
        
        if estado_real in ['CANCELADO', 'HISTORICO', 'INACTIVO']:
            detalle = f"Predio {estado_real} aún dibujado en GDB"
            accion = "BORRAR polígono de la GDB"
        elif estado_real == "NO EXISTE EN BD":
            detalle = "Código en GDB no existe en el reporte Excel"
            accion = "Investigar procedencia / Error digitación"
        else:
            detalle = f"Estado en BD: {estado_real} (Pero no marcado como ACTIVO)"
            accion = "Revisar consistencia de estados"
            
        reporte.append({
            'TIPO': 'SOBRA EN GDB',
            'DETALLE': detalle,
            'CODIGO': cod,
            'ESTADO_BD': estado_real,
            'ACCION_SUGERIDA': accion
        })

    return reporte + errores_internos, []

def generar_excel_renumeracion(errores_alfa, errores_geo=None):
    """Genera el reporte de Excel consolidado"""
    output = io.BytesIO()
    df_alfa = pd.DataFrame(errores_alfa)
    df_geo = pd.DataFrame(errores_geo) if errores_geo else pd.DataFrame()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Pestaña Resumen Alfanumérico
        if not df_alfa.empty:
            resumen = df_alfa.groupby(['REGLA', 'DETALLE']).size().reset_index(name='CANTIDAD')
            resumen.to_excel(writer, sheet_name='RESUMEN_ALFA', index=False)
            df_alfa.to_excel(writer, sheet_name='DETALLE_ALFA', index=False)
        else:
            pd.DataFrame([{'RESULTADO': 'TODO PERFECTO'}]).to_excel(writer, sheet_name='ALFA_OK', index=False)
            
        # Pestaña Geográfica
        if not df_geo.empty:
            df_geo.to_excel(writer, sheet_name='DETALLE_GEO', index=False)
        elif errores_geo is not None:
            pd.DataFrame([{'RESULTADO': 'CONSISTENCIA PERFECTA'}]).to_excel(writer, sheet_name='GEO_OK', index=False)

    output.seek(0)
    return output

