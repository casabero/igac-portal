import pandas as pd
import numpy as np
import io
import os

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
    Refactorización del script original para el backend del portal.
    tipo_config: '1' para CICA (IGAC), '2' para Operadores (Terceros/ANT)
    """
    # 1. Configuración de columnas
    if tipo_config == '1':
        col_anterior = 'Número predial CICA'
    else:
        col_anterior = 'Número predial LC_PREDIO'
    
    col_nuevo = 'Número predial SNC'
    col_estado = 'Estado'

    # 2. Carga y Limpieza
    try:
        df = pd.read_excel(file_stream, dtype=str)
    except Exception as e:
        raise ValueError(f"Error al leer el archivo Excel: {str(e)}")

    # Validar columnas
    columnas_requeridas = [col_nuevo, col_anterior, col_estado]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan las columnas requeridas: {', '.join(faltantes)}")

    # Limpieza básica
    df[col_anterior] = df[col_anterior].str.strip()
    df[col_nuevo] = df[col_nuevo].str.strip()
    df[col_estado] = df[col_estado].str.strip()

    # Filtro de ACTIVOS y Ordenamiento
    df_audit = df[df[col_estado].str.upper() == 'ACTIVO'].copy()
    df_audit = df_audit.sort_values(by=[col_nuevo])

    if len(df_audit) == 0:
        return {
            'total_auditado': 0,
            'errores': [],
            'stats': {},
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
            'DETALLE': 'Número duplicado',
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
            'DETALLE': 'Códigos temporales/letras en definitivo',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # --- [4] CONSECUTIVOS ---
    # Terrenos nuevos en manzanas viejas
    mask_t_nuevo = es_provisional(df_ant['TERRENO']) & ~es_provisional(df_ant['MANZANA'])
    if mask_t_nuevo.any():
        df_revisar = df_audit[mask_t_nuevo].copy()
        df_revisar['GRUPO'] = df_nue.loc[mask_t_nuevo, 'GRUPO_GEO']
        
        # Necesitamos saber cuáles son los máximos del UNIVERSO original (df_nue completo)
        # pero excluyendo los que estamos Auditando como nuevos
        for grupo_mza, datos_nuevos in df_revisar.groupby('GRUPO'):
            # Los que ya existían según la lógica del script: los que NO son provisionales
            mask_existentes = (df_nue['GRUPO_GEO'] == grupo_mza) & (~es_nuevo_ant)
            predios_viejos = df_audit[mask_existentes]

            if not predios_viejos.empty:
                max_viejo = int(parse_code(predios_viejos[col_nuevo])['TERRENO'].max())
                for _, row in datos_nuevos.iterrows():
                    terr_asignado = int(row[col_nuevo][17:21])
                    if terr_asignado <= max_viejo:
                        todos_errores.append({
                            'REGLA': '4. CONSECUTIVO TERRENO',
                            'DETALLE': f'Asignado {terr_asignado} <= Máx existente {max_viejo}',
                            'ANTERIOR': row[col_anterior],
                            'NUEVO': row[col_nuevo]
                        })

    # --- [5] REINICIO EN MANZANAS NUEVAS ---
    mask_mza_nueva = es_provisional(df_ant['MANZANA']) & ~es_provisional(df_ant['SECTOR'])
    errores_mza = df_audit[mask_mza_nueva & (df_nue['TERRENO'].astype(int) > 50)]
    for _, row in errores_mza.iterrows():
        todos_errores.append({
            'REGLA': '5. MANZANA NUEVA',
            'DETALLE': 'Terreno > 50 en manzana nueva. ¿Faltó reiniciar?',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # --- [6] REINICIO EN SECTORES NUEVOS ---
    mask_sec_nuevo = es_provisional(df_ant['SECTOR'])
    errores_sec = df_audit[mask_sec_nuevo & (df_nue['MANZANA'].astype(int) > 20)]
    for _, row in errores_sec.iterrows():
        todos_errores.append({
            'REGLA': '6. SECTOR NUEVO',
            'DETALLE': 'Manzana > 20 en sector nuevo.',
            'ANTERIOR': row[col_anterior],
            'NUEVO': row[col_nuevo]
        })

    # Estadísticas por regla
    df_err = pd.DataFrame(todos_errores) if todos_errores else pd.DataFrame(columns=['REGLA'])
    reconté = df_err['REGLA'].value_counts().to_dict() if not df_err.empty else {}

    return {
        'total_auditado': len(df_audit),
        'errores': todos_errores,
        'stats': reconté,
        'success': True
    }

def generar_excel_renumeracion(errores):
    """Genera el buffer de Excel con el reporte de errores"""
    output = io.BytesIO()
    df_fin = pd.DataFrame(errores)
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if not df_fin.empty:
            # Resumen
            resumen = df_fin.groupby(['REGLA', 'DETALLE']).size().reset_index(name='CANTIDAD')
            resumen.to_excel(writer, sheet_name='RESUMEN', index=False)
            # Detalle
            df_fin.to_excel(writer, sheet_name='DETALLE_ERRORES', index=False)
            
            # Formato headers
            workbook = writer.book
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
            
            for sheetname in ['RESUMEN', 'DETALLE_ERRORES']:
                worksheet = writer.sheets[sheetname]
                # Ajustar anchos (simple)
                worksheet.set_column(0, 5, 30)
        else:
            # Hoja vacía si no hay errores
            pd.DataFrame([{'RESULTADO': 'TODO PERFECTO'}]).to_excel(writer, sheet_name='RESULTADO', index=False)

    output.seek(0)
    return output
