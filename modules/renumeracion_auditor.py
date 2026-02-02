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

class AuditoriaSNC:
    """Motor de auditoría lógica para re-numeración (Portado de notebook)"""
    def __init__(self):
        self.df = None
        self.errores = []
        self.warnings = []
        # Estadísticas vivas
        self.stats = {'total_rows': 0, 'processed_batches': 0, 'errors': 0, 'warnings': 0, 'ok_records': 0}
        
        # Memoria de Estado (State Management)
        self.memoria = {
            'terreno': {}, # Key: Mpio-Zona-Sect-Manz -> Val: Int
            'manzana': {}, # Key: Mpio-Zona-Sect      -> Val: Int
            'sector':  {}, # Key: Mpio-Zona           -> Val: Int
            'zona':    {}  # Key: Mpio                -> Val: Int
        }
        self.col_ant = ''
        self.col_new = ''
        self.df_clean = pd.DataFrame()

    def log_error(self, regla, escenario, ubicacion, detalle, severidad='ERROR'):
        registro = {
            'TIPO': severidad,
            'REGLA': regla,
            'ESCENARIO': escenario,
            'UBICACION': ubicacion,
            'DETALLE': detalle,
            'ANTERIOR': ubicacion.split('|')[0] if '|' in ubicacion else ubicacion, # Fallback
            'NUEVO': ubicacion.split('|')[1] if '|' in ubicacion else ''
        }
        if severidad == 'ERROR':
            self.errores.append(registro)
            self.stats['errors'] += 1
        else:
            self.warnings.append(registro)
            self.stats['warnings'] += 1

    def cargar_dataframe(self, df, tipo_config, col_snc_man=None, col_ant_man=None):
        # Configurar columnas
        if col_snc_man and col_ant_man:
            col_nc, col_ant = col_snc_man, col_ant_man
        else:
            col_ant = 'NÚMERO_PREDIAL_CICA' if tipo_config == "1" else 'NÚMERO_PREDIAL_LC PREDIO'
            col_nc = 'NÚMERO_PREDIAL_SNC'
        
        self.col_ant = col_ant
        self.col_new = col_nc

        # Normalización básica de nombres
        self.df = df.copy()
        # Intentar mapear si no existen exactas (manejo insensible a mayúsculas/espacios)
        col_map = {c: c for c in self.df.columns}
        
        # Búsqueda fuzzy simple para columnas standard
        for c in self.df.columns:
            c_up = c.upper().strip()
            if col_nc.upper().strip() in c_up: self.col_new = c
            if col_ant.upper().replace('_', ' ').strip() in c_up.replace('_', ' '): self.col_ant = c

        if self.col_new not in self.df.columns or self.col_ant not in self.df.columns:
             # Fallback posicional para operativa rapida
            if len(self.df.columns) >= 2 and tipo_config == '2': # Asumir orden para operadores si falla nombre
                 self.col_new = self.df.columns[0]
                 self.col_ant = self.df.columns[1]

        # Filtrar activos si existe estado
        col_estado = next((c for c in self.df.columns if 'ESTADO' in c.upper()), None)
        if col_estado:
             self.df = self.df[self.df[col_estado].astype(str).str.upper().str.contains('ACTIVO', na=False)].copy()

        self.stats['total_rows'] = len(self.df)
        return True

    def parsear_y_limpiar(self):
        """Refactor 1: Parsing estricto y segregación de basura"""
        
        def validar_estructura(npn):
            s = str(npn).strip()
            if pd.isna(npn) or s.lower() == 'nan' or s == '': return (False, None, 'NULO/VACIO')
            if len(s) != 30: return (False, None, f'LONGITUD INVALIDA ({len(s)})')
            if not s.isdigit(): return (False, None, 'CARACTERES NO NUMERICOS EN SNC')
            
            # Estructura LADM (Mpio 5, Zona 2, Sect 2, Manz 4, Terr 4)
            return (True, [s[0:5], s[5:7], s[7:9], s[13:17], s[17:21]], 'OK')

        # Procesamos columna nueva (SNC) que debe ser perfecta
        temp_data = self.df[self.col_new].apply(validar_estructura)
        
        # Separamos filas válidas de inválidas
        self.df['VALID_SNC'] = [t[0] for t in temp_data]
        self.df['SNC_PARTS'] = [t[1] for t in temp_data]
        
        # Reportar Inconsistencias Estructurales
        invalidos = self.df[~self.df['VALID_SNC']]
        for idx, r in invalidos.iterrows():
            msg = temp_data.loc[idx][2]
            # Usar formato combinado para UBICACION para compatibilidad con reporte
            loc = f"{r[self.col_ant]}|{r[self.col_new]}"
            self.log_error('ESTRUCTURA_NPN', 'PRE-PROCESO', loc, msg)
            
        # Nos quedamos solo con lo procesable
        self.df_clean = self.df[self.df['VALID_SNC']].copy()
        
        if self.df_clean.empty: return

        # Expandimos columnas para aritmética
        cols_n = ['M_N', 'Z_N', 'S_N', 'MZ_N', 'T_N']
        self.df_clean[cols_n] = pd.DataFrame(self.df_clean['SNC_PARTS'].tolist(), index=self.df_clean.index)
        for c in cols_n:
            self.df_clean[f"{c}_INT"] = self.df_clean[c].astype(int)

        # Parseo del Anterior (que puede ser imperfecto/alfanumérico)
        def parse_ant(s):
            s = str(s).strip()
            if len(s) < 20: return ['0']*5 # Dummy filler
            return [s[0:5], s[5:7], s[7:9], s[13:17], s[17:21]]
            
        cols_a = ['M_A', 'Z_A', 'S_A', 'MZ_A', 'T_A']
        try:
             self.df_clean[cols_a] = pd.DataFrame(self.df_clean[self.col_ant].apply(parse_ant).tolist(), index=self.df_clean.index)
        except:
             # Fallback si falla el parseo masivo
             self.df_clean[cols_a] = pd.DataFrame([['0']*5]*len(self.df_clean), index=self.df_clean.index, columns=cols_a)


    def inicializar_memoria(self):
        """Carga el estado inicial basado en registros que PERMANECEN"""
        if self.df_clean.empty: return

        # Definir escenarios (necesario antes de memoria)
        def get_escenario(row):
            if row[self.col_ant] == row[self.col_new]: return 'PERMANENCIA'
            # Detección de temporales (9xxx o Letras)
            ant_parts = [str(row['Z_A']), str(row['S_A']), str(row['MZ_A']), str(row['T_A'])]
            is_temp = any((x.isdigit() and 9000 <= int(x) <= 9999) or any(c.isalpha() for c in x) for x in ant_parts)
            
            if not is_temp and row[self.col_ant] != row[self.col_new]: return 'CAMBIO_ATIPICO' 
            
            # Jerarquía
            if row['Z_A'] != row['Z_N']: return 'NUEVO_CENTRO_POBLADO'
            if row['S_A'] != row['S_N']: return 'NUEVO_SECTOR'
            if row['MZ_A'] != row['MZ_N']: return 'NUEVA_MANZANA'
            return 'NUEVO_TERRENO'

        self.df_clean['ESCENARIO'] = self.df_clean.apply(get_escenario, axis=1)
        
        # Cargar Memoria solo con Permanencias
        hist = self.df_clean[self.df_clean['ESCENARIO'] == 'PERMANENCIA']
        
        # Llenado optimizado con diccionarios
        # 1. Terrenos
        for k, v in hist.groupby(['M_N','Z_N','S_N','MZ_N'])['T_N_INT'].max().items():
            self.memoria['terreno'][f"{k[0]}-{k[1]}-{k[2]}-{k[3]}"] = v
        # 2. Manzanas
        for k, v in hist.groupby(['M_N','Z_N','S_N'])['MZ_N_INT'].max().items():
            self.memoria['manzana'][f"{k[0]}-{k[1]}-{k[2]}"] = v
        # 3. Sectores
        for k, v in hist.groupby(['M_N','Z_N'])['S_N_INT'].max().items():
            self.memoria['sector'][f"{k[0]}-{k[1]}"] = v

    def validar_lotes(self):
        if self.df_clean.empty: return
        
        # Filtramos nuevos
        df_proc = self.df_clean[self.df_clean['ESCENARIO'] != 'PERMANENCIA'].copy()
        
        if df_proc.empty: return

        # Refactor 2: Sorting seguro con token __NULL__ por si hay NAs en partes antiguas
        sort_cols = ['M_A', 'Z_A', 'S_A', 'MZ_A']
        for c in sort_cols:
             df_proc[c] = df_proc[c].astype(str).replace('nan', '__INVALID__')
        
        # Agrupamos por el padre provisional (Lote de origen)
        # Esto asume que el input viene algo ordenado o que el loteo es coherente
        # Ordenamos por manzana origen para procesar "lotes de trabajo"
        df_proc = df_proc.sort_values(by=sort_cols)

        for (mpio, zona_a, sect_a, manz_a), lote in df_proc.groupby(sort_cols):
            self.stats['processed_batches'] += 1
            
            escenario = lote['ESCENARIO'].iloc[0]
            
            # Identificar destino (SNC Final)
            # Tomamos el primero del lote como referencia de destino principal
            # (Aunque un lote podria partirse a varios destinos, validamos dispersion despues)
            ref = lote.iloc[0]
            mpio_n, zona_n, sect_n, manz_n = ref['M_N'], ref['Z_N'], ref['S_N'], ref['MZ_N']
            loc_ref = f"{ref[self.col_ant]}|{ref[self.col_new]}"

            # ---------------------------------------------------------
            # VALIDACIÓN DE PADRES
            # ---------------------------------------------------------
            
            # A. VALIDAR ZONA/SECTOR SI ES NUEVO
            if escenario == 'NUEVO_SECTOR':
                key_z = f"{mpio_n}-{zona_n}"
                last_sect = self.memoria['sector'].get(key_z, 0) # Si no existe, asume 0
                actual_sect = int(ref['S_N_INT'])
                
                # Check consecutividad sector
                if actual_sect != last_sect + 1:
                    self.log_error('CONSECUTIVIDAD_SECTOR', escenario, loc_ref, 
                                   f"Zona {zona_n}: Sector saltó de {last_sect} a {actual_sect}")
                
                # Actualizar memoria
                self.memoria['sector'][key_z] = max(last_sect, actual_sect)

            # B. VALIDAR MANZANA SI ES NUEVA
            if escenario in ['NUEVO_SECTOR', 'NUEVA_MANZANA']:
                key_s = f"{mpio_n}-{zona_n}-{sect_n}"
                last_manz = self.memoria['manzana'].get(key_s, 0)
                actual_manz = int(ref['MZ_N_INT'])
                
                # Regla de reinicio
                msg_m = None
                if actual_manz > last_manz + 1 and actual_manz != 1:
                     msg_m = f"Manzana saltó de {last_manz} a {actual_manz}"
                
                if msg_m:
                     self.log_error('CONSECUTIVIDAD_MANZANA', escenario, loc_ref, msg_m)
                
                self.memoria['manzana'][key_s] = max(last_manz, actual_manz)

            # ---------------------------------------------------------
            # VALIDACIÓN DE HIJOS (TERRENOS) - BATCH
            # ---------------------------------------------------------
            
            # 1. Check Dispersión
            destinos = lote[['M_N', 'Z_N', 'S_N', 'MZ_N']].drop_duplicates()
            if len(destinos) > 1:
                self.log_error('DISPERSION_LOTE', escenario, loc_ref, 
                               f"Origen {manz_a} se dispersó en {len(destinos)} manzanas diferentes", severidad='WARNING')

            # 2. Check Continuidad Terrenos (Por cada sub-destino)
            for _, dest in destinos.iterrows():
                sub_lote = lote[lote['MZ_N'] == dest['MZ_N']]
                vals = sorted(sub_lote['T_N_INT'].unique())
                
                min_t, max_t, count_t = min(vals), max(vals), len(vals)
                dest_loc = f"{dest['M_N']}-{dest['Z_N']}-{dest['S_N']}-{dest['MZ_N']}"
                
                sub_ref = sub_lote.iloc[0]
                sub_loc_str = f"{sub_ref[self.col_ant]}|{sub_ref[self.col_new]}"

                # Check Huecos Matemáticos dentro del lote
                if (max_t - min_t + 1) != count_t:
                    self.log_error('HUECOS_NUMERACION', escenario, sub_loc_str,
                                   f"Mz {dest['MZ_N']}: Rango {min_t}-{max_t} tiene {count_t} regs (Faltan predios)")
                else:
                    self.stats['ok_records'] += count_t

                # Check Inicio de Secuencia
                key_m = dest_loc 
                last_t = self.memoria['terreno'].get(key_m, 0)
                
                expected_start = last_t + 1
                # En escenarios de Manzana Nueva, si es el primer lote para esta manzana, esperamos 1
                # (OJO: Podria ser un segundo lote para la MIISMA manzana nueva, entonces last_t ya se actualizó?
                #  Aqui entra la secuencialidad del loop. Si Lote 1 llenó mz 1-10, memoria es 10. Lote 2 debe empezar 11.)
                
                if escenario in ['NUEVA_MANZANA', 'NUEVO_SECTOR'] and last_t == 0:
                    expected_start = 1
                
                if min_t != expected_start:
                    # Relajar severidad si es cambio atipico
                    sev = 'WARNING' if escenario == 'CAMBIO_ATIPICO' else 'ERROR'
                    self.log_error('INICIO_SECUENCIA', escenario, sub_loc_str,
                                   f"Mz {dest['MZ_N']}: Inició en {min_t}, se esperaba {expected_start} (Ultimo Conocido: {last_t})", severidad=sev)

                # Actualizar Memoria Terreno
                self.memoria['terreno'][key_m] = max(last_t, max_t)


def procesar_renumeracion(file_stream, tipo_config, col_snc_manual=None, col_ant_manual=None, col_estado_manual=None):
    """
    Fase 1: Auditoría Lógica (REFACTORIZADO CON MOTOR STATEFUL)
    """
    try:
        df_full = pd.read_excel(file_stream, dtype=str)
    except Exception as e:
        raise ValueError(f"Error al leer el archivo Excel: {str(e)}")

    engine = AuditoriaSNC()
    
    # 1. Cargar
    if not engine.cargar_dataframe(df_full, tipo_config, col_snc_manual, col_ant_manual):
        raise ValueError("No se pudieron identificar las columnas necesarias automaticamente.")
    
    # 2. Parsear
    engine.parsear_y_limpiar()
    
    # 3. Inicializar Estado
    engine.inicializar_memoria()
    
    # 4. Validar Lotes
    engine.validar_lotes()
    
    # Adaptar salida al formato esperado por el PDF
    # engine.errores tiene dicts con keys TIPO, REGLA, ESCENARIO, UBICACION, DETALLE, ANTERIOR, NUEVO
    
    todos_errores = engine.errores + engine.warnings
    # Filtrar solo errores para el conteo critico si se quiere, o pasar todo.
    # El reporte PDF actual usa keys: REGLA, DETALLE, ANTERIOR, NUEVO.
    # Agregamos TIPO al detalle o regla para visibilidad
    
    final_errors = []
    for e in todos_errores:
        prefix = "[ADVERTENCIA] " if e['TIPO'] != 'ERROR' else ""
        final_errors.append({
            'REGLA': f"{prefix}{e['REGLA']}",
            'DETALLE': f"{e['ESCENARIO']}: {e['DETALLE']}",
            'ANTERIOR': e['ANTERIOR'],
            'NUEVO': e['NUEVO']
        })
    
    # Metricas
    stats = {k: v for k,v in engine.stats.items()}
    t_err = (len(engine.errores) / len(df_full) * 100) if len(df_full) > 0 else 0
    
    # Top problematicos
    c_p = {}
    for e in final_errors:
        c = e['NUEVO']
        if c not in c_p: c_p[c] = []
        c_p[c].append(e['REGLA'])
    top_p = sorted([(c, len(r), ', '.join(set(r))) for c, r in c_p.items()], key=lambda x: x[1], reverse=True)[:10]

    return {
        'total_auditado': len(df_full), 
        'errores': final_errors, 
        'stats': stats, 
        'diccionario_estados': {}, # Se pierde la feature de estados por ahora al no ser critica en logica nueva
        'df_referencia': engine.df[[engine.col_new, engine.col_ant]].rename(columns={engine.col_new: 'CODIGO_SNC', engine.col_ant: 'CODIGO_ANTERIOR'}),
        'success': True, 
        'tipo_config': tipo_config, 
        'timestamp': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S'),
        'tasa_error': round(t_err, 2), 
        'top_problematicos': top_p
    }

def extraer_datos_gdb(zip_stream, capas_objetivo):
    """Extrae predios de un ZIP que contiene una GDB"""
    if not HAS_GEO: return pd.DataFrame(), ["Librerías geoespaciales no instaladas."]
    predios, errores = [], []
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "upload.zip")
        zip_stream.seek(0); f = open(zip_path, "wb"); f.write(zip_stream.read()); f.close()
        extract_path = os.path.join(tmpdir, "extract"); os.makedirs(extract_path)
        try:
            with zipfile.ZipFile(zip_path, 'r') as z: z.extractall(extract_path)
        except Exception as e: return pd.DataFrame(), [f"Error al descomprimir ZIP: {str(e)}"]
        gdb_path = None
        for root, dirs, files in os.walk(extract_path):
            for d in dirs:
                if d.endswith(".gdb"): gdb_path = os.path.join(root, d); break
            if gdb_path: break
        if not gdb_path: return pd.DataFrame(), ["No se encontró ninguna .gdb dentro del ZIP."]
        try:
            layers = fiona.listlayers(gdb_path)
            for capa in capas_objetivo:
                if capa in layers:
                    try:
                        gdf = gpd.read_file(gdb_path, layer=capa)
                        if 'CODIGO' in gdf.columns: predios.extend(gdf['CODIGO'].astype(str).str.strip().tolist())
                    except Exception as e: errores.append(f"Error leyendo capa {capa}: {str(e)}")
        except Exception as e: errores.append(f"Error al listar capas: {str(e)}")
    return pd.DataFrame({'CODIGO': predios}), errores

def procesar_geografica(zip_formal, zip_informal, set_alfa_activos, diccionario_estados, df_alfa_ref):
    """Fase 2: Cruce Geográfico."""
    capas_f, capas_i = ['U_TERRENO', 'R_TERRENO', 'TERRENO'], ['U_TERRENO_INFORMAL', 'R_TERRENO_INFORMAL', 'TERRENO_INFORMAL']
    list_geo, e_int = [], []
    if zip_formal:
        df, err = extraer_datos_gdb(zip_formal, capas_f); list_geo.append(df)
        for e in err: e_int.append({'TIPO': 'ERROR GDB FORMAL', 'DETALLE': e, 'CODIGO': 'N/A', 'ESTADO_BD': 'N/A', 'ACCION_SUGERIDA': 'Revisar ZIP/GDB'})
    if zip_informal:
        df, err = extraer_datos_gdb(zip_informal, capas_i); list_geo.append(df)
        for e in err: e_int.append({'TIPO': 'ERROR GDB INFORMAL', 'DETALLE': e, 'CODIGO': 'N/A', 'ESTADO_BD': 'N/A', 'ACCION_SUGERIDA': 'Revisar ZIP/GDB'})
    if not list_geo or all(df.empty for df in list_geo): return [], e_int
    df_geo_total = pd.concat(list_geo).drop_duplicates()
    set_geo = set(df_geo_total['CODIGO'])
    set_alfa = set_alfa_activos
    reporte = []
    sin_mapa = set_alfa - set_geo
    for cod in sin_mapa: reporte.append({'TIPO': 'FALTA EN GDB', 'DETALLE': 'Predio Activo en Excel no encontrado en Geometría', 'CODIGO': cod, 'ESTADO_BD': 'ACTIVO', 'ACCION_SUGERIDA': 'Dibujar predio o revisar vigencia'})
    sin_alfa = set_geo - set_alfa
    for cod in sin_alfa:
        st = diccionario_estados.get(cod, "NO EXISTE EN BD")
        det = f"Predio {st} aún dibujado en GDB" if st in ['CANCELADO', 'HISTORICO', 'INACTIVO'] else "Código en GDB no existe en el reporte Excel" if st == "NO EXISTE EN BD" else f"Estado en BD: {st} (Pero no marcado como ACTIVO)"
        acc = "BORRAR polígono de la GDB" if st in ['CANCELADO', 'HISTORICO', 'INACTIVO'] else "Investigar procedencia / Error digitación" if st == "NO EXISTE EN BD" else "Revisar consistencia de estados"
        reporte.append({'TIPO': 'SOBRA EN GDB', 'DETALLE': det, 'CODIGO': cod, 'ESTADO_BD': st, 'ACCION_SUGERIDA': acc})
    coincidencias = set_alfa & set_geo
    c_sample = list(coincidencias)[:20]
    stats_g = {'total_alfa': len(set_alfa), 'total_geo': len(set_geo), 'coincidencias': len(coincidencias), 'sin_mapa': len(sin_mapa), 'sobran_gdb': len(sin_alfa)}
    return reporte + e_int, {'coincidencias_sample': c_sample, 'stats_geo': stats_g}

def generar_excel_renumeracion(errores_alfa, errores_geo=None, fase=1):
    """Genera el reporte de Excel consolidado"""
    output = io.BytesIO()
    df_alfa, df_geo = pd.DataFrame(errores_alfa), pd.DataFrame(errores_geo) if errores_geo else pd.DataFrame()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        f_txt = "Fase 1: Alfanumérica" if fase == 1 else "Fase 2: Geográfica (+ Base Fase 1)"
        df_meta = pd.DataFrame([{'REPORTE': 'Reporte de Auditoría de Renumeración', 'FRIO': ''}, {'CAMPO': 'Fase Ejecutada', 'VALOR': f_txt}, {'CAMPO': 'Fecha de Generación', 'VALOR': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S')}])
        if fase == 2: df_meta = pd.concat([df_meta, pd.DataFrame([{'CAMPO': 'Nota', 'VALOR': 'Se asume validación Fase 1 aprobada'}])], ignore_index=True)
        df_meta.to_excel(writer, sheet_name='METADATOS', index=False)
        if not df_alfa.empty:
            resumen = df_alfa.groupby(['REGLA', 'DETALLE']).size().reset_index(name='CANTIDAD')
            resumen.to_excel(writer, sheet_name='RESUMEN_ALFA', index=False)
            df_alfa.to_excel(writer, sheet_name='DETALLE_ALFA', index=False)
        else: pd.DataFrame([{'RESULTADO': 'TODO PERFECTO'}]).to_excel(writer, sheet_name='ALFA_OK', index=False)
        if not df_geo.empty: df_geo.to_excel(writer, sheet_name='DETALLE_GEO', index=False)
        elif errores_geo is not None: pd.DataFrame([{'RESULTADO': 'CONSISTENCIA PERFECTA'}]).to_excel(writer, sheet_name='GEO_OK', index=False)
    output.seek(0); return output

from fpdf import FPDF
import matplotlib.pyplot as plt

class AuditoriaRenumeracionPDF(FPDF):
    def header(self):
        self.set_fill_color(249, 250, 251); self.rect(0, 0, 216, 35, 'F'); self.set_y(12); self.set_font('Helvetica', 'B', 16); self.set_text_color(17, 17, 17); self.cell(0, 10, 'REPORTE_RENUMERACIÓN // IGAC', 0, 1, 'C')
        self.set_font('Helvetica', '', 8); self.set_text_color(156, 163, 175); self.cell(0, 5, 'SIS_AUDITORÍA_CATASTRAL :: MULTIPROPÓSITO', 0, 1, 'C'); self.ln(15)

    def footer(self):
        self.set_y(-15); self.set_draw_color(243, 244, 246); self.line(20, self.get_y(), 196, self.get_y()); self.ln(2)
        self.set_font('Helvetica', '', 7); self.set_text_color(156, 163, 175)
        self.cell(0, 10, 'SISTEMA DE GESTIÓN CATASTRAL - PORTAL IGAC 2026', 0, 0, 'L'); self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'R')

def generar_pdf_renumeracion(resultados):
    """Genera un reporte PDF detallado"""
    pdf = AuditoriaRenumeracionPDF(format='Letter'); pdf.set_margins(20, 20, 20); pdf.add_page()
    t_c, f = resultados.get('tipo_config', '1'), resultados.get('fase_ejecutada', 1)
    l_comp, l_ant = ('CICA vs SNC', 'CICA') if t_c == '1' else ('Operadores vs SNC', 'Operadores')
    pdf.ln(2); pdf.set_font('Helvetica', '', 10); pdf.set_text_color(107, 114, 128)
    pdf.cell(0, 6, f"Auditoría: {l_comp}  |  Fecha: {resultados.get('timestamp', 'N/A')}", 0, 1, 'L'); pdf.ln(5)
    pdf.set_font('Helvetica', 'B', 14); pdf.cell(0, 10, 'Resumen Ejecutivo', 0, 1); pdf.ln(2)
    def add_meta(l, v, c=(0,0,0)):
        pdf.set_font('Helvetica', '', 10); pdf.cell(70, 8, l, 0); pdf.set_font('Helvetica', 'B', 10); pdf.set_text_color(*c); pdf.cell(0, 8, v, 0, 1); pdf.set_text_color(0,0,0)
    add_meta('Total Predios Auditados:', f"{resultados.get('total_auditado', 0):,} predios")
    t_err_count = len(resultados.get('errores', []))
    add_meta('Alertas de Datos Encontradas:', f"{t_err_count:,}", (220, 38, 38) if t_err_count > 0 else (22, 163, 74))
    t_err_rate = resultados.get('tasa_error', 0)
    add_meta('Tasa de Error:', f"{t_err_rate}%", (220, 38, 38) if t_err_rate > 5 else (22, 163, 74) if t_err_rate == 0 else (234, 179, 8))
    if 'errores_geo' in resultados: add_meta('Alertas de Mapas Encontradas:', f"{len(resultados.get('errores_geo', [])):,}", (220, 38, 38) if len(resultados.get('errores_geo', [])) > 0 else (22, 163, 74))
    pdf.ln(5)

    # --- EXPLICACIÓN DE REGLAS (ACTUALIZADO A LOGICA NUEVA) ---
    pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Protocolo de Validación Lógica', 0, 1); pdf.ln(1)
    pdf.set_font('Helvetica', '', 9); pdf.multi_cell(0, 4, 'Validación secuencial de integridad de datos LADM:'); pdf.ln(2)
    
    rules_desc = [
        ("ESTRUCTURA_NPN", "Longitud (30) o caracteres inválidos en el NPN nuevo."),
        ("CONSECUTIVIDAD_SECTOR", "Salto numérico injustificado en Sectores Nuevos."),
        ("CONSECUTIVIDAD_MANZANA", "Salto numérico en Manzanas Nuevas o falta de reinicio a 0001."),
        ("HUECOS_NUMERACION", "Existencia de predios faltantes dentro de una secuencia de lote (ej. 1, 2, 4 -> falta 3)."),
        ("INICIO_SECUENCIA", "El lote nuevo no inicia en el consecutivo esperado segun el último predio existente."),
        ("DISPERSION_LOTE", "Un lote de origen se dispersó en múltiples manzanas destino (posible error operativo).")
    ]
    
    for r_title, r_text in rules_desc:
        pdf.set_font('Helvetica', 'B', 9)
        title_w = pdf.get_string_width(r_title + ":") + 5
        pdf.cell(title_w, 5, r_title + ":", 0, 0)
        start_x = pdf.get_x()
        pdf.set_left_margin(start_x)
        pdf.set_font('Helvetica', '', 9)
        pdf.multi_cell(0, 5, r_text)
        pdf.set_left_margin(20)
        pdf.ln(1)

    if f == 2:
        l_g = resultados.get('logs_geo', {}); s_g = l_g.get('stats_geo', {}) if isinstance(l_g, dict) else {}
        if s_g:
            try:
                plt.figure(figsize=(6, 4)); plt.pie([s_g['coincidencias'], s_g['sin_mapa'], s_g['sobran_gdb']], labels=['Consistentes', 'Faltan GDB', 'Sobran GDB'], autopct='%1.1f%%', startangle=140, colors=['#111111', '#555555', '#999999']); plt.title('Consistencia Datos vs Mapas', fontsize=11, color='#111111'); i_b = io.BytesIO(); plt.savefig(i_b, format='png', dpi=150); plt.close(); i_b.seek(0); pdf.add_page(); pdf.image(i_b, x=45, w=120); pdf.ln(5)
            except Exception: pass

    top_p = resultados.get('top_problematicos', [])
    if top_p:
        pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Top 10 Códigos con Más Alertas', 0, 1); pdf.ln(2); pdf.set_font('Helvetica', 'B', 8); pdf.cell(10, 8, '#', 1, 0, 'C')
        pdf.cell(65, 8, 'Código Predial', 1, 0, 'C'); pdf.cell(20, 8, 'Alertas', 1, 0, 'C'); pdf.cell(80, 8, 'Reglas Incumplidas', 1, 1, 'C'); pdf.set_font('Helvetica', '', 7)
        for idx, (cod, n, r) in enumerate(top_p, 1):
            pdf.cell(10, 6, str(idx), 1, 0, 'C'); pdf.cell(65, 6, str(cod), 1, 0, 'C'); pdf.set_text_color(220, 38, 38); pdf.cell(20, 6, str(n), 1, 0, 'C'); pdf.set_text_color(0, 0, 0); pdf.cell(80, 6, r[:65], 1, 1)

    pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Detalle de Alertas Lógicas', 0, 1); pdf.ln(2); pdf.set_font('Helvetica', 'B', 8)
    pdf.cell(50, 8, 'Regla', 1); pdf.cell(60, 8, 'Detalle', 1); pdf.cell(35, 8, f'{l_ant}', 1); pdf.cell(35, 8, 'Nuevo (SNC)', 1); pdf.ln()
    pdf.set_font('Helvetica', '', 6)
    for e in resultados.get('errores', [])[:150]:
        if pdf.get_y() > 260: pdf.add_page(); pdf.set_font('Helvetica', 'B', 8); pdf.cell(50, 8, 'Regla', 1); pdf.cell(60, 8, 'Detalle', 1); pdf.cell(35, 8, f'{l_ant}', 1); pdf.cell(35, 8, 'Nuevo (SNC)', 1); pdf.ln(); pdf.set_font('Helvetica', '', 6)
        pdf.cell(50, 6, str(e['REGLA'])[:48], 1); pdf.cell(60, 6, str(e['DETALLE'])[:55], 1); pdf.cell(35, 6, str(e['ANTERIOR']), 1); pdf.cell(35, 6, str(e['NUEVO']), 1); pdf.ln()

    return bytes(pdf.output())
