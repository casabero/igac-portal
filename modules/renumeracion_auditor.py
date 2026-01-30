import pandas as pd
import numpy as np
import io
import os
import zipfile
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
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

def procesar_renumeracion(file_stream, tipo_config, col_snc_manual=None, col_ant_manual=None, col_estado_manual=None):
    """
    Fase 1: Auditoría Alfanumérica.
    Retorna errores y un diccionario de referencia de estados.
    """
    try:
        df_full = pd.read_excel(file_stream, dtype=str)
    except Exception as e:
        raise ValueError(f"Error al leer el archivo Excel: {str(e)}")

    if col_snc_manual and col_ant_manual:
        col_nuevo = col_snc_manual
        col_anterior = col_ant_manual
    else:
        # Fallback a lógica automática si no se proveen nombres manuales
        if tipo_config == '1':
            col_anterior = 'Número predial CICA'
        else:
            col_anterior = 'Número predial LC_PREDIO'
        col_nuevo = 'Número predial SNC'

    # Asignar columna de estado (manual o por defecto)
    col_estado = col_estado_manual if col_estado_manual else 'Estado'

    # Lógica para modo Operadores (2) si no hay mapeo manual e intentamos forzar posición
    if tipo_config == '2' and not (col_snc_manual and col_ant_manual):
        if len(df_full.columns) >= 2:
            mapa_cols = {
                df_full.columns[0]: 'Número predial SNC',
                df_full.columns[1]: col_anterior
            }
            df_full = df_full.rename(columns=mapa_cols)
            col_nuevo = 'Número predial SNC'

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
            # Los que ya existían: NO provisinales
            mask_existentes = (df_nue['GRUPO_GEO'] == grupo_mza) & (~es_nuevo_ant)
            predios_viejos = df_audit[mask_existentes]

            if not predios_viejos.empty:
                viejos_codigos = predios_viejos[col_nuevo].apply(lambda x: int(x[17:21]) if x[17:21].isdigit() else 0)
                viejos_fisicos = viejos_codigos[viejos_codigos < 900]
                
                if not viejos_fisicos.empty:
                    max_viejo = viejos_fisicos.max()
                    
                    for _, row in datos_nuevos.iterrows():
                        try:
                            condicion = row[col_nuevo][21] # Caracter 22
                            if condicion != '0':
                                continue # Es informal o especial, no valida consecutivo estricto
                                
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

    # Calcular estadísticas adicionales
    total_errores = len(todos_errores)
    tasa_error = (total_errores / len(df_audit) * 100) if len(df_audit) > 0 else 0
    
    # Top 10 códigos con más errores
    top_problematicos = []
    if todos_errores:
        codigos_problema = {}
        for err in todos_errores:
            cod = err['NUEVO']
            if cod not in codigos_problema:
                codigos_problema[cod] = []
            codigos_problema[cod].append(err['REGLA'])
        
        top_problematicos = sorted(
            [(cod, len(reglas), ', '.join(set(reglas))) for cod, reglas in codigos_problema.items()],
            key=lambda x: x[1],
            reverse=True
        )[:10]

    return {
        'total_auditado': len(df_audit),
        'errores': todos_errores,
        'stats': stats,
        'diccionario_estados': diccionario_estados,
        'df_referencia': df_audit[[col_nuevo, col_anterior]].rename(columns={col_nuevo: 'CODIGO_SNC', col_anterior: 'CODIGO_ANTERIOR'}),
        'success': True,
        'tipo_config': tipo_config,
        'timestamp': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S'),
        'tasa_error': round(tasa_error, 2),
        'top_problematicos': top_problematicos
    }

def extraer_datos_gdb(zip_stream, capas_objetivo):
    """Extrae predios de un ZIP que contiene una GDB"""
    if not HAS_GEO:
        return pd.DataFrame(), ["Librerías geoespaciales no instaladas."]
    
    predios = []
    errores = []
    
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "upload.zip")
        zip_stream.seek(0)
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
    
    # 1. Faltan en GDB
    sin_mapa = set_alfa - set_geo
    for cod in sin_mapa:
        reporte.append({
            'TIPO': 'FALTA EN GDB',
            'DETALLE': 'Predio Activo en Excel no encontrado en Geometría',
            'CODIGO': cod,
            'ESTADO_BD': 'ACTIVO',
            'ACCION_SUGERIDA': 'Dibujar predio o revisar vigencia'
        })
        
    # 2. Sobran en GDB
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

    # 3. Coincidencias (Muestra)
    coincidencias = set_alfa & set_geo
    coincidencias_sample = list(coincidencias)[:20]
    
    stats_geo = {
        'total_alfa': len(set_alfa),
        'total_geo': len(set_geo),
        'coincidencias': len(coincidencias),
        'sin_mapa': len(sin_mapa),
        'sobran_gdb': len(sin_alfa)
    }

    return reporte + errores_internos, {
        'coincidencias_sample': coincidencias_sample,
        'stats_geo': stats_geo
    }

def generar_excel_renumeracion(errores_alfa, errores_geo=None, fase=1):
    """Genera el reporte de Excel consolidado"""
    output = io.BytesIO()
    df_alfa = pd.DataFrame(errores_alfa)
    df_geo = pd.DataFrame(errores_geo) if errores_geo else pd.DataFrame()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        fase_txt = "Fase 1: Alfanumérica" if fase == 1 else "Fase 2: Geográfica (+ Base Fase 1)"
        df_meta = pd.DataFrame([
            {'REPORTE': 'Reporte de Auditoría de Renumeración', 'FRIO': ''},
            {'CAMPO': 'Fase Ejecutada', 'VALOR': fase_txt},
            {'CAMPO': 'Fecha de Generación', 'VALOR': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S')}
        ])
        if fase == 2:
            df_meta = pd.concat([df_meta, pd.DataFrame([{'CAMPO': 'Nota', 'VALOR': 'Se asume validación Fase 1 aprobada'}])], ignore_index=True)
        df_meta.to_excel(writer, sheet_name='METADATOS', index=False)

        if not df_alfa.empty:
            resumen = df_alfa.groupby(['REGLA', 'DETALLE']).size().reset_index(name='CANTIDAD')
            resumen.to_excel(writer, sheet_name='RESUMEN_ALFA', index=False)
            df_alfa.to_excel(writer, sheet_name='DETALLE_ALFA', index=False)
        else:
            pd.DataFrame([{'RESULTADO': 'TODO PERFECTO'}]).to_excel(writer, sheet_name='ALFA_OK', index=False)
            
        if not df_geo.empty:
            df_geo.to_excel(writer, sheet_name='DETALLE_GEO', index=False)
        elif errores_geo is not None:
            pd.DataFrame([{'RESULTADO': 'CONSISTENCIA PERFECTA'}]).to_excel(writer, sheet_name='GEO_OK', index=False)

    output.seek(0)
    return output

from fpdf import FPDF
import matplotlib.pyplot as plt

class AuditoriaRenumeracionPDF(FPDF):
    def header(self):
        self.set_fill_color(17, 24, 39)
        self.rect(0, 0, 216, 35, 'F') 
        self.set_y(12)
        self.set_font('Helvetica', 'B', 16)
        self.set_text_color(255, 255, 255)
        self.cell(0, 10, 'REPORTE DE RENOMERACIÓN - IGAC', 0, 1, 'C')
        self.set_font('Helvetica', '', 8)
        self.set_text_color(156, 163, 175)
        self.cell(0, 5, 'SISTEMA DE AUDITORÍA CATASTRAL MULTIPROPÓSITO', 0, 1, 'C')
        self.ln(15)

    def footer(self):
        self.set_y(-20)
        self.set_draw_color(229, 231, 235)
        self.line(20, self.get_y(), 196, self.get_y())
        self.ln(2)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(107, 114, 128)
        self.cell(0, 10, 'by casabero quien se hace llamar joseph.gari', 0, 0, 'L')
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'R')

def generar_pdf_renumeracion(resultados):
    """Genera un reporte PDF detallado"""
    pdf = AuditoriaRenumeracionPDF(format='Letter')
    pdf.set_margins(20, 20, 20)
    pdf.add_page()
    
    tipo_config = resultados.get('tipo_config', '1')
    fase = resultados.get('fase_ejecutada', 1)
    
    label_comparacion = 'CICA vs SNC' if tipo_config == '1' else 'Operadores vs SNC'
    label_anterior = 'CICA' if tipo_config == '1' else 'Operadores'
    
    pdf.ln(2)
    pdf.set_font('Helvetica', '', 10)
    pdf.set_text_color(107, 114, 128)
    fecha = resultados.get('timestamp', 'N/A')
    pdf.cell(0, 6, f"Auditoría: {label_comparacion}  |  Fecha: {fecha}", 0, 1, 'L')
    pdf.ln(5)
    
    pdf.set_font('Helvetica', 'B', 14); pdf.cell(0, 10, 'Resumen Ejecutivo', 0, 1); pdf.ln(2)
    
    total_auditado = resultados.get('total_auditado', 0)
    total_errores = len(resultados.get('errores', []))
    tasa_error = resultados.get('tasa_error', 0)
    
    def add_meta(label, val, color=(0,0,0)):
        pdf.set_font('Helvetica', '', 10); pdf.cell(70, 8, label, 0)
        pdf.set_font('Helvetica', 'B', 10); pdf.set_text_color(*color); pdf.cell(0, 8, val, 0, 1); pdf.set_text_color(0,0,0)

    add_meta('Total Predios Auditados:', f"{total_auditado:,} predios")
    c_err = (220, 38, 38) if total_errores > 0 else (22, 163, 74)
    add_meta('Alertas de Datos Encontradas:', f"{total_errores:,}", c_err)
    c_tasa = (220, 38, 38) if tasa_error > 5 else (22, 163, 74) if tasa_error == 0 else (234, 179, 8)
    add_meta('Tasa de Error:', f"{tasa_error}%", c_tasa)
    
    if 'errores_geo' in resultados:
        total_geo = len(resultados.get('errores_geo', []))
        add_meta('Alertas de Mapas Encontradas:', f"{total_geo:,}", (220, 38, 38) if total_geo > 0 else (22, 163, 74))

    pdf.ln(5)
    
    if fase == 1:
        stats = resultados.get('stats', {})
        if stats:
            try:
                plt.figure(figsize=(6, 4))
                short_rules = [r.split('.')[0] for r in stats.keys()]
                plt.bar(short_rules, stats.values(), color='#cbd5e1', edgecolor='#94a3b8') 
                plt.title('Distribución de Alertas por Regla', fontsize=11, color='#475569')
                plt.grid(axis='y', linestyle=':', alpha=0.5)
                img_buf = io.BytesIO(); plt.savefig(img_buf, format='png', dpi=150); plt.close(); img_buf.seek(0)
                pdf.image(img_buf, x=45, w=120); pdf.ln(5)
            except Exception as e: print(f"Error gráfico: {e}")
    else:
        logs_geo = resultados.get('logs_geo', {})
        stats_geo = logs_geo.get('stats_geo', {}) if isinstance(logs_geo, dict) else {}
        if stats_geo:
            try:
                plt.figure(figsize=(6, 4))
                labels = ['Consistentes', 'Faltan GDB', 'Sobran GDB']
                sizes = [stats_geo['coincidencias'], stats_geo['sin_mapa'], stats_geo['sobran_gdb']]
                plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=['#22c55e', '#ef4444', '#f59e0b'])
                plt.title('Consistencia Datos vs Mapas', fontsize=11, color='#475569')
                img_buf = io.BytesIO(); plt.savefig(img_buf, format='png', dpi=150); plt.close(); img_buf.seek(0)
                pdf.image(img_buf, x=45, w=120); pdf.ln(5)
            except Exception as e: print(f"Error gráfico geo: {e}")

    # Detalle de alertas (Top 10)
    top_problematicos = resultados.get('top_problematicos', [])
    if fase == 1 and top_problematicos:
        pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Top 10 Códigos con Más Alertas', 0, 1); pdf.ln(2)
        pdf.set_font('Helvetica', 'B', 8); pdf.cell(10, 8, '#', 1, 0, 'C'); pdf.cell(45, 8, 'Código Predial', 1, 0, 'C'); pdf.cell(20, 8, 'Alertas', 1, 0, 'C'); pdf.cell(100, 8, 'Reglas Incumplidas', 1, 1, 'C')
        pdf.set_font('Helvetica', '', 7)
        for idx, (codigo, num, reglas) in enumerate(top_problematicos, 1):
            pdf.cell(10, 6, str(idx), 1, 0, 'C'); pdf.cell(45, 6, str(codigo), 1, 0, 'C'); pdf.set_text_color(220, 38, 38); pdf.cell(20, 6, str(num), 1, 0, 'C'); pdf.set_text_color(0, 0, 0); pdf.cell(100, 6, reglas[:70], 1, 1)

    pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Detalle de Alertas Alfanuméricas', 0, 1); pdf.ln(2)
    pdf.set_font('Helvetica', 'B', 8); pdf.cell(45, 8, 'Regla', 1); pdf.cell(55, 8, 'Detalle', 1); pdf.cell(37.5, 8, f'{label_anterior}', 1); pdf.cell(37.5, 8, 'Nuevo (SNC)', 1); pdf.ln()
    pdf.set_font('Helvetica', '', 7)
    for err in resultados.get('errores', [])[:100]:
        if pdf.get_y() > 260: pdf.add_page()
        pdf.cell(45, 6, str(err['REGLA']), 1); pdf.cell(55, 6, str(err['DETALLE'])[:40], 1); pdf.cell(37.5, 6, str(err['ANTERIOR']), 1); pdf.cell(37.5, 6, str(err['NUEVO']), 1); pdf.ln()

    return bytes(pdf.output())
