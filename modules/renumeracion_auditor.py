import pandas as pd
import numpy as np
import io
import os
import zipfile
import tempfile
from datetime import datetime, timezone, timedelta

# =============================================================================
# CLASE PRINCIPAL: AUDITORÍA SNC (VERSIÓN 3.1 - PRODUCTION READY)
# =============================================================================
class AuditoriaSNC:
    def __init__(self):
        self.df = None
        self.df_clean = None
        self.errores = []
        self.warnings = []
        self.col_ant = ''
        self.col_new = ''
        self.col_estado = 'ESTADO'
        
        # Estadísticas de ejecución
        self.stats = {
            'total_filas': 0,
            'lotes_procesados': 0,
            'predios_ok': 0,
            'errores_criticos': 0,
            'advertencias': 0
        }
        
        # Memoria de Estado (State Management) para validar consecutividad
        self.memoria = {
            'terreno': {}, # Key: Mpio-Zona-Sect-Manz -> Val: Int (Último terreno asignado)
            'manzana': {}, # Key: Mpio-Zona-Sect      -> Val: Int (Última manzana asignada)
            'sector':  {}, # Key: Mpio-Zona           -> Val: Int (Último sector asignado)
        }

    def log_error(self, regla, escenario, ubicacion, detalle, severidad='ERROR'):
        """Registra hallazgos en las listas correspondientes"""
        registro = {
            'TIPO': severidad,
            'REGLA': regla,
            'ESCENARIO': escenario,
            'UBICACION': ubicacion,
            'DETALLE': detalle,
            # Campos extra para compatibilidad con reporte PDF legacy
            'ANTERIOR': ubicacion.split('|')[0] if '|' in str(ubicacion) else ubicacion,
            'NUEVO': ubicacion.split('|')[1] if '|' in str(ubicacion) else ''
        }
        if severidad == 'ERROR':
            self.errores.append(registro)
            self.stats['errores_criticos'] += 1
        else:
            self.warnings.append(registro)
            self.stats['advertencias'] += 1

    # =========================================================================
    # 1. CARGA DE DATOS (Adaptado para WEB)
    # =========================================================================
    def cargar_datos(self, file_stream, tipo_config):
        # Mapeo de columnas segun config
        self.col_ant = 'NÚMERO_PREDIAL_CICA' if tipo_config == "1" else 'NÚMERO_PREDIAL_LC PREDIO'
        self.col_new = 'NÚMERO_PREDIAL_SNC'
        self.col_estado = 'ESTADO'

        try:
            # Cargar como string para preservar ceros a la izquierda
            self.df = pd.read_excel(file_stream, dtype=str)
            
            # Normalización de cabeceras (Trim + Upper)
            self.df.columns = [c.strip().upper() for c in self.df.columns]
            
            # Ajuste de nombres si el Excel trae variaciones
            col_map = {c: c for c in self.df.columns}
            
            # Busqueda fuzzy para col_ant si no match exacto
            if self.col_ant not in col_map:
                possible = [c for c in col_map if self.col_ant.replace('_',' ') in c.replace('_',' ')]
                if possible: self.col_ant = possible[0]
                elif len(self.df.columns) > 1 and tipo_config == "2": # Fallback posicional operadores
                     self.col_ant = self.df.columns[1]

            if self.col_new not in col_map:
                possible = [c for c in col_map if 'SNC' in c]
                if possible: self.col_new = possible[0]
                elif len(self.df.columns) > 0:
                     self.col_new = self.df.columns[0]
            
            # Filtro de activos (Solo procesamos lo vigente)
            col_est_real = next((c for c in self.df.columns if 'ESTADO' in c), None)
            if col_est_real:
                self.col_estado = col_est_real
                self.df = self.df[self.df[self.col_estado].astype(str).str.upper().str.contains('ACTIVO', na=False)].copy()
            
            self.stats['total_filas'] = len(self.df)
            return True
        except Exception as e:
            print(f"❌ Error crítico al leer el archivo: {e}")
            return False

    # =========================================================================
    # 2. PARSING Y LIMPIEZA
    # =========================================================================
    def parsear_y_limpiar(self):
        
        # A. Validar columna SNC (Debe ser perfecta: 30 dígitos numéricos)
        def validar_estructura_snc(npn):
            s = str(npn).strip()
            if pd.isna(npn) or s.lower() in ['nan', ''] or len(s) < 5: 
                return (False, None, 'NULO/VACIO')
            if not s.isdigit(): 
                return (False, None, 'ALFANUMERICO EN CAMPO NUMERICO')
            if len(s) != 30: 
                return (False, None, f'LONGITUD INVALIDA ({len(s)} chars)')
            
            # Descomposición LADM: Mpio(5), Zona(2), Sect(2), Manz(4), Terr(4)
            return (True, [s[0:5], s[5:7], s[7:9], s[13:17], s[17:21]], 'OK')

        temp_data = self.df[self.col_new].apply(validar_estructura_snc)
        
        self.df['VALID_SNC'] = [t[0] for t in temp_data]
        self.df['SNC_PARTS'] = [t[1] for t in temp_data]
        
        # Reportar basura estructural inmediatamente
        invalidos = self.df[~self.df['VALID_SNC']]
        for idx, r in invalidos.iterrows():
            msg = temp_data.loc[idx][2]
            loc = f"{r[self.col_ant]}|{r[self.col_new]}"
            self.log_error('ESTRUCTURA_NPN', 'PRE-PROCESO', loc, msg)
            
        # Filtrar solo válidos para la lógica de negocio
        self.df_clean = self.df[self.df['VALID_SNC']].copy()

        if self.df_clean.empty: return

        # Generar columnas numéricas para validación matemática
        cols_n = ['M_N', 'Z_N', 'S_N', 'MZ_N', 'T_N']
        self.df_clean[cols_n] = pd.DataFrame(self.df_clean['SNC_PARTS'].tolist(), index=self.df_clean.index)
        for c in cols_n:
            self.df_clean[f"{c}_INT"] = self.df_clean[c].astype(int)

        # B. Parsear columna ANTERIOR (Puede ser imperfecta/alfanumérica)
        def parse_ant(s):
            s = str(s).strip()
            # Si es muy corto o nulo, devolvemos tokens seguros para que el groupby no falle
            if len(s) < 15: return ['UNK', '00', '00', '0000', '0000']
            try:
                # Intento de extracción estándar
                return [s[0:5], s[5:7], s[7:9], s[13:17], s[17:21]]
            except:
                return ['UNK', '00', '00', '0000', '0000']

        cols_a = ['M_A', 'Z_A', 'S_A', 'MZ_A', 'T_A']
        self.df_clean[cols_a] = pd.DataFrame(self.df_clean[self.col_ant].apply(parse_ant).tolist(), index=self.df_clean.index)

    # =========================================================================
    # 3. UNICIDAD ABSOLUTA
    # =========================================================================
    def validar_unicidad_absoluta(self):
        if self.df_clean.empty: return
        # Busca si el mismo NPN de salida se generó para más de un predio
        duplicados = self.df_clean[self.df_clean.duplicated(subset=[self.col_new], keep=False)]
        
        if not duplicados.empty:
            # Agrupar para reporte limpio
            for npn, grupo in duplicados.groupby(self.col_new):
                origenes = grupo[self.col_ant].unique().tolist()
                loc = f"VARIOUS|{npn}"
                self.log_error(
                    regla='UNICIDAD_SNC',
                    escenario='CRITICO',
                    ubicacion=loc,
                    detalle=f"NPN Duplicado. Asignado a {len(grupo)} orígenes distintos: {origenes}"
                )

    # =========================================================================
    # 4. INICIALIZACIÓN DE MEMORIA (PERMANENCIAS)
    # =========================================================================
    def inicializar_memoria(self):
        if self.df_clean.empty: return
        
        # Función para determinar escenario fila a fila
        def determinar_escenario(row):
            # 1. Permanencia estricta
            if row[self.col_ant] == row[self.col_new]: 
                return 'PERMANENCIA'
            
            # 2. Detección de Temporalidad (9xxx o Alfanumérico) en componentes clave
            parts_check = [str(row['Z_A']), str(row['S_A']), str(row['MZ_A']), str(row['T_A'])]
            es_temporal = any((x.isdigit() and 9000 <= int(x) <= 9999) or any(c.isalpha() for c in x) for x in parts_check)
            
            if not es_temporal:
                # Si cambia pero no era temporal, es una novedad/corrección
                return 'CAMBIO_ATIPICO' 

            # 3. Jerarquía de Novedad (¿Qué nivel geográfico cambió?)
            if row['Z_A'] != row['Z_N']: return 'NUEVO_CENTRO_POBLADO'
            if row['S_A'] != row['S_N']: return 'NUEVO_SECTOR'
            if row['MZ_A'] != row['MZ_N']: return 'NUEVA_MANZANA'
            return 'NUEVO_TERRENO'

        self.df_clean['ESCENARIO'] = self.df_clean.apply(determinar_escenario, axis=1)

        # Cargar diccionarios con los máximos de los predios que NO cambiaron (La base histórica)
        hist = self.df_clean[self.df_clean['ESCENARIO'] == 'PERMANENCIA']
        
        # A. Memoria Terrenos (Mpio-Zona-Sect-Manz)
        for k, v in hist.groupby(['M_N','Z_N','S_N','MZ_N'])['T_N_INT'].max().items():
            self.memoria['terreno'][f"{k[0]}-{k[1]}-{k[2]}-{k[3]}"] = v
            
        # B. Memoria Manzanas (Mpio-Zona-Sect)
        for k, v in hist.groupby(['M_N','Z_N','S_N'])['MZ_N_INT'].max().items():
            self.memoria['manzana'][f"{k[0]}-{k[1]}-{k[2]}"] = v
            
        # C. Memoria Sectores (Mpio-Zona)
        for k, v in hist.groupby(['M_N','Z_N'])['S_N_INT'].max().items():
            self.memoria['sector'][f"{k[0]}-{k[1]}"] = v

    # =========================================================================
    # 5. VALIDACIÓN POR LOTES (EL MOTOR PRINCIPAL)
    # =========================================================================
    def validar_lotes(self):
        if self.df_clean.empty: return
        
        # Filtramos solo lo que requiere validación (Excluyendo permanencias)
        df_proc = self.df_clean[self.df_clean['ESCENARIO'].isin([
            'NUEVO_TERRENO', 'NUEVA_MANZANA', 'NUEVO_SECTOR', 'NUEVO_CENTRO_POBLADO'
        ])].copy()
        
        if df_proc.empty: return

        # Ordenamos por la jerarquía anterior para procesar en orden de "llegada"
        sort_cols = ['M_A', 'Z_A', 'S_A', 'MZ_A']
        for c in sort_cols:
             df_proc[c] = df_proc[c].astype(str).replace('nan', 'UNK')
        
        df_proc = df_proc.sort_values(by=sort_cols)
        
        # AGRUPACIÓN: Todos los predios que vienen de la misma manzana provisional se validan juntos
        for (mpio, zona_a, sect_a, manz_a), lote in df_proc.groupby(sort_cols):
            self.stats['lotes_procesados'] += 1
            escenario = lote['ESCENARIO'].iloc[0]
            
            # Tomamos el primer registro del lote como referencia del destino
            ref = lote.iloc[0]
            mpio_n, zona_n, sect_n = ref['M_N'], ref['Z_N'], ref['S_N']
            loc_ref = f"{ref[self.col_ant]}|{ref[self.col_new]}"
            
            # -----------------------------------------------------------------
            # A. VALIDACIONES DE PADRES (Zona / Sector / Manzana)
            # -----------------------------------------------------------------
            
            # REGLA: CENTRO POBLADO NUEVO (Zona Nueva)
            if escenario == 'NUEVO_CENTRO_POBLADO':
                # Instructivo: "El sector se inicia como 00"
                if int(sect_n) != 0:
                    self.log_error('NORMA_CP_SECTOR_00', escenario, loc_ref, 
                                   f"Primer sector de Zona Nueva debe ser 00. Se halló: {sect_n}")
                
                # Instructivo: Manzana inicia en 0001
                if int(ref['MZ_N_INT']) != 1:
                    self.log_error('NORMA_CP_MANZANA_01', escenario, loc_ref, 
                                   f"Primera manzana de Zona Nueva debe ser 0001. Se halló: {ref['MZ_N']}")

            # REGLA: SECTOR NUEVO (En Zona Existente)
            elif escenario == 'NUEVO_SECTOR':
                key_z = f"{mpio_n}-{zona_n}"
                last_sect = self.memoria['sector'].get(key_z, -1) # -1 = Sin histórico
                actual_sect = int(ref['S_N_INT'])
                
                # Validar Consecutividad (Last + 1)
                # OJO: Si last_sect es -1, significa que no habia nada antes, entonces 00 o 01 es aceptable segun zona
                if last_sect != -1 and actual_sect != last_sect + 1:
                    self.log_error('CONSECUTIVIDAD_SECTOR', escenario, loc_ref, 
                                   f"Salto de sector indebido en Zona {zona_n}. Anterior: {last_sect}, Nuevo: {actual_sect}")
                
                # Actualizar Memoria
                self.memoria['sector'][key_z] = max(last_sect, actual_sect)
                
                # En sector nuevo, la manzana debería iniciar o reiniciar secuencia
                if int(ref['MZ_N_INT']) != 1:
                     self.log_error('INICIO_MANZANA_SECTOR', escenario, loc_ref,
                                    f"Manzana en sector nuevo inició en {ref['MZ_N']} (se esperaba 0001)", severidad='WARNING')

            # REGLA: MANZANA NUEVA
            # Validamos salto de manzana dentro del sector
            if escenario in ['NUEVA_MANZANA', 'NUEVO_SECTOR']:
                key_s = f"{mpio_n}-{zona_n}-{sect_n}"
                last_manz = self.memoria['manzana'].get(key_s, 0)
                actual_manz = int(ref['MZ_N_INT'])
                
                # Si es manzana nueva en sector viejo, debe ser Last + 1
                if escenario == 'NUEVA_MANZANA' and actual_manz > last_manz + 1 and actual_manz != 1:
                    self.log_error('CONSECUTIVIDAD_MANZANA', escenario, loc_ref,
                                   f"Salto de manzana. Anterior {last_manz}, Nueva {actual_manz}")
                
                self.memoria['manzana'][key_s] = max(last_manz, actual_manz)

            # -----------------------------------------------------------------
            # B. VALIDACIÓN DE HIJOS (TERRENOS) - MATEMÁTICA DE LOTES
            # -----------------------------------------------------------------
            
            # Detectar si la manzana temporal se dispersó en varias definitivas (Dispersión)
            destinos = lote[['M_N', 'Z_N', 'S_N', 'MZ_N']].drop_duplicates()
            if len(destinos) > 1:
                self.log_error('DISPERSION_LOTE', escenario, loc_ref,
                               f"Predios de un mismo lote temporal terminaron en {len(destinos)} manzanas definitivas distintas.", severidad='WARNING')

            # Iterar cada manzana de destino para validar sus terrenos internos
            for _, dest in destinos.iterrows():
                sub_lote = lote[lote['MZ_N'] == dest['MZ_N']]
                vals = sorted(sub_lote['T_N_INT'].unique())
                
                min_t, max_t, count_t = min(vals), max(vals), len(vals)
                
                sub_ref = sub_lote.iloc[0]
                sub_loc = f"{sub_ref[self.col_ant]}|{sub_ref[self.col_new]}"

                # 1. CHEQUEO DE HUECOS (GAPS)
                if (max_t - min_t + 1) != count_t:
                    self.log_error('HUECOS_NUMERACION', escenario, sub_loc,
                                   f"Mz {dest['MZ_N']}: Secuencia interrumpida. Rango {min_t}-{max_t} ({max_t-min_t+1} espacios) para {count_t} predios.")
                else:
                    self.stats['predios_ok'] += count_t

                # 2. CHEQUEO DE PUNTO DE INICIO
                key_m = f"{dest['M_N']}-{dest['Z_N']}-{dest['S_N']}-{dest['MZ_N']}"
                last_t = self.memoria['terreno'].get(key_m, 0)
                
                expected_start = last_t + 1
                
                # Si es manzana nueva (o CP/Sector nuevo), y no hay historia, esperamos 1
                if escenario != 'NUEVO_TERRENO' and last_t == 0:
                    expected_start = 1
                
                if min_t != expected_start:
                    self.log_error('INICIO_SECUENCIA', escenario, sub_loc,
                                   f"Mz {dest['MZ_N']}: Terrenos iniciaron en {min_t}, se esperaba {expected_start} (basado en historia {last_t})", severidad='WARNING')
                
                # Actualizar Memoria Terreno
                self.memoria['terreno'][key_m] = max(last_t, max_t)

    # =========================================================================
    # 6. REPORTES (Web Adapter)
    # =========================================================================
    def generar_reporte_excel(self):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Hoja 1: Dashboard
            dash = pd.DataFrame([
                {'METRICA': 'TOTAL REGISTROS', 'VALOR': self.stats['total_filas']},
                {'METRICA': 'LOTES PROCESADOS', 'VALOR': self.stats['lotes_procesados']},
                {'METRICA': 'PREDIOS SECUENCIA OK', 'VALOR': self.stats['predios_ok']},
                {'METRICA': 'ERRORES CRÍTICOS', 'VALOR': self.stats['errores_criticos']},
                {'METRICA': 'ADVERTENCIAS', 'VALOR': self.stats['advertencias']}
            ])
            dash.to_excel(writer, sheet_name='DASHBOARD', index=False)
            
            # Hoja 2: Errores
            if self.errores:
                pd.DataFrame(self.errores).to_excel(writer, sheet_name='ERRORES', index=False)
            else:
                pd.DataFrame({'ESTADO': ['SIN ERRORES']}).to_excel(writer, sheet_name='ERRORES', index=False)
                
            # Hoja 3: Advertencias
            if self.warnings:
                pd.DataFrame(self.warnings).to_excel(writer, sheet_name='ADVERTENCIAS', index=False)
                
            # Hoja 4: Muestra de Datos (con escenario identificado)
            if self.df_clean is not None and not self.df_clean.empty:
                cols = [self.col_ant, self.col_new, 'ESCENARIO']
                self.df_clean[cols].head(3000).to_excel(writer, sheet_name='DATA_TAGGED', index=False)
        output.seek(0)
        return output

# =============================================================================
# FUNCIONES WRAPPER (INTEGRACIÓN FLASK)
# =============================================================================

def procesar_renumeracion(file_stream, tipo_config, col_snc_manual=None, col_ant_manual=None, col_estado_manual=None):
    """Interfaz principal requerida por app.py"""
    
    engine = AuditoriaSNC()
    
    # 1. Cargar
    if not engine.cargar_datos(file_stream, tipo_config):
        raise ValueError("Error leyendo el archivo. Verifique formato Excel.")
    
    # 2. Procesar Pipeline
    engine.parsear_y_limpiar()
    engine.validar_unicidad_absoluta()
    engine.inicializar_memoria()
    engine.validar_lotes()
    
    # Adaptar para PDF antiguo
    todos_errores = engine.errores + engine.warnings
    final_errors = []
    for e in todos_errores:
        prefix = "[ADVERTENCIA] " if e['TIPO'] != 'ERROR' else ""
        final_errors.append({
            'REGLA': f"{prefix}{e['REGLA']}",
            'DETALLE': f"{e['ESCENARIO']}: {e['DETALLE']}",
            'ANTERIOR': e['ANTERIOR'],
            'NUEVO': e['NUEVO']
        })
        
    t_err = (len(engine.errores) / engine.stats['total_filas'] * 100) if engine.stats['total_filas'] > 0 else 0
    
    c_p = {}
    for e in final_errors:
        c = e['NUEVO']
        if c not in c_p: c_p[c] = []
        c_p[c].append(e['REGLA'])
    top_p = sorted([(c, len(r), ', '.join(set(r))) for c, r in c_p.items()], key=lambda x: x[1], reverse=True)[:10]

    return {
        'total_auditado': engine.stats['total_filas'],
        'errores': final_errors,
        'stats': engine.stats,
        'diccionario_estados': {},
        'success': True,
        'tipo_config': tipo_config,
        'timestamp': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S'),
        'tasa_error': round(t_err, 2),
        'top_problematicos': top_p,
        'engine_instance': engine # Devolvemos instancia para acceder a generar_reporte_excel
    }

def generar_excel_renumeracion(errores_ad, errores_geo=None, fase=1):
    """
    IMPORTANTE: Si se dispone de la instancia del engine, usar engine.generar_reporte_excel().
    Esta función se mantiene por compatibilidad si se llama sin instancia completa,
    pero idealmente procesar_renumeracion devuelve el objeto.
    """
    # Si errores_ad viene vacío o es lista, hacemos fallback.
    # Pero app.py llama a procesar, y luego con results llama aqui?
    # Modificaremos app.py para usar el metodo de clase si es posible, o reconstruimos aqui.
    # Por ahora mantenemos la compatibilidad básica de volcar la lista de dicts.
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df = pd.DataFrame(errores_ad)
        if not df.empty:
            df.to_excel(writer, sheet_name='REPORT_LEGACY', index=False)
        else:
            pd.DataFrame(['OK']).to_excel(writer, sheet_name='REPORT_LEGACY', index=False)
    output.seek(0)
    return output


from fpdf import FPDF
import matplotlib.pyplot as plt

class AuditoriaRenumeracionPDF(FPDF):
    def header(self):
        self.set_fill_color(249, 250, 251); self.rect(0, 0, 216, 35, 'F'); self.set_y(12); self.set_font('Helvetica', 'B', 16); self.set_text_color(17, 17, 17); self.cell(0, 10, 'REPORTE_RENUMERACIÓN // IGAC v3.1', 0, 1, 'C')
        self.set_font('Helvetica', '', 8); self.set_text_color(156, 163, 175); self.cell(0, 5, 'SIS_AUDITORÍA_CATASTRAL :: MULTIPROPÓSITO', 0, 1, 'C'); self.ln(15)

    def footer(self):
        self.set_y(-15); self.set_draw_color(243, 244, 246); self.line(20, self.get_y(), 196, self.get_y()); self.ln(2)
        self.set_font('Helvetica', '', 7); self.set_text_color(156, 163, 175)
        self.cell(0, 10, 'SISTEMA DE GESTIÓN CATASTRAL - PORTAL IGAC 2026', 0, 0, 'L'); self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'R')

def generar_pdf_renumeracion(resultados):
    """Genera un reporte PDF detallado"""
    pdf = AuditoriaRenumeracionPDF(format='Letter'); pdf.set_margins(20, 20, 20); pdf.add_page()
    t_c = resultados.get('tipo_config', '1')
    l_comp, l_ant = ('CICA vs SNC', 'CICA') if t_c == '1' else ('Operadores vs SNC', 'Operadores')
    pdf.ln(2); pdf.set_font('Helvetica', '', 10); pdf.set_text_color(107, 114, 128)
    pdf.cell(0, 6, f"Auditoría: {l_comp}  |  Fecha: {resultados.get('timestamp', 'N/A')}", 0, 1, 'L'); pdf.ln(5)
    
    pdf.set_font('Helvetica', 'B', 14); pdf.cell(0, 10, 'Resumen Ejecutivo (V3.1)', 0, 1); pdf.ln(2)
    def add_meta(l, v, c=(0,0,0)):
        pdf.set_font('Helvetica', '', 10); pdf.cell(70, 8, l, 0); pdf.set_font('Helvetica', 'B', 10); pdf.set_text_color(*c); pdf.cell(0, 8, v, 0, 1); pdf.set_text_color(0,0,0)
    
    add_meta('Total Predios Auditados:', f"{resultados.get('total_auditado', 0):,} predios")
    t_err_count = len(resultados.get('errores', []))
    add_meta('Alertas Encontradas:', f"{t_err_count:,}", (220, 38, 38) if t_err_count > 0 else (22, 163, 74))
    
    pdf.ln(5)

    # --- EXPLICACIÓN DE REGLAS (Nueva sección solicitada) ---
    pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Protocolo v3.1 (Validación LADM)', 0, 1); pdf.ln(1)
    pdf.set_font('Helvetica', '', 9); pdf.multi_cell(0, 4, 'Reglas de lógica espacial y secuencial:'); pdf.ln(2)
    
    rules_desc = [
        ("UNICIDAD_SNC", "Duplicidad absoluta de NPN nuevo. Crítico."),
        ("NORMA_CP", "Violación de normas de creación de Centros Poblados (Sector 00, Manzana 0001)."),
        ("CONSECUTIVIDAD_SECTOR", "Salto numérico injustificado en Sectores Nuevos."),
        ("CONSECUTIVIDAD_MANZANA", "Salto numérico en Manzanas Nuevas."),
        ("HUECOS_NUMERACION", "Existencia de predios faltantes (gaps) dentro de un lote."),
        ("INICIO_SECUENCIA", "El lote nuevo no inicia en el consecutivo esperado."),
        ("DISPERSION_LOTE", "Un lote de origen se dispersó en múltiples manzanas destino.")
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

    top_p = resultados.get('top_problematicos', [])
    if top_p:
        pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Top 10 Códigos con Más Alertas', 0, 1); pdf.ln(2); pdf.set_font('Helvetica', 'B', 8); pdf.cell(10, 8, '#', 1, 0, 'C')
        pdf.cell(65, 8, 'Código Predial', 1, 0, 'C'); pdf.cell(20, 8, 'Alertas', 1, 0, 'C'); pdf.cell(80, 8, 'Reglas Incumplidas', 1, 1, 'C'); pdf.set_font('Helvetica', '', 7)
        for idx, (cod, n, r) in enumerate(top_p, 1):
            pdf.cell(10, 6, str(idx), 1, 0, 'C'); pdf.cell(65, 6, str(cod), 1, 0, 'C'); pdf.set_text_color(220, 38, 38); pdf.cell(20, 6, str(n), 1, 0, 'C'); pdf.set_text_color(0, 0, 0); pdf.cell(80, 6, r[:65], 1, 1)

    pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Detalle de Alertas Lógicas', 0, 1); pdf.ln(2); pdf.set_font('Helvetica', 'B', 8)
    pdf.cell(50, 8, 'Regla', 1); pdf.cell(60, 8, 'Detalle', 1); pdf.cell(35, 8, f'{l_ant}', 1); pdf.cell(35, 8, 'Nuevo (SNC)', 1); pdf.ln()
    pdf.set_font('Helvetica', '', 6)
    for e in resultados.get('errores', [])[:200]:
        if pdf.get_y() > 260: pdf.add_page(); pdf.set_font('Helvetica', 'B', 8); pdf.cell(50, 8, 'Regla', 1); pdf.cell(60, 8, 'Detalle', 1); pdf.cell(35, 8, f'{l_ant}', 1); pdf.cell(35, 8, 'Nuevo (SNC)', 1); pdf.ln(); pdf.set_font('Helvetica', '', 6)
        pdf.cell(50, 6, str(e['REGLA'])[:48], 1); pdf.cell(60, 6, str(e['DETALLE'])[:55], 1); pdf.cell(35, 6, str(e['ANTERIOR']), 1); pdf.cell(35, 6, str(e['NUEVO']), 1); pdf.ln()

    return bytes(pdf.output())

# Stub para funciones geo que no estaban en V3 pero quizas se requieran para no romper imports
def procesar_geografica(*args, **kwargs):
    return [], {'stats_geo': {}}
