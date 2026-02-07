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

    def log_error(self, regla, escenario, ubicacion, detalle, severidad='ERROR', zona=None, sector=None, manzana=None, estado='N/A'):
        """Registra hallazgos en las listas correspondientes con jerarquía"""
        registro = {
            'TIPO': severidad,
            'REGLA': regla,
            'ESCENARIO': escenario,
            'UBICACION': ubicacion,
            'DETALLE': detalle,
            'ZONA': zona,
            'SECTOR': sector,
            'MANZANA': manzana,
            'ESTADO': estado,
            # Campos extra para compatibilidad
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
    def cargar_datos(self, file_stream, tipo_config, col_snc_manual=None, col_ant_manual=None, col_estado_manual=None):
        # Mapeo inicial
        self.col_ant = col_ant_manual or ('NÚMERO_PREDIAL_CICA' if tipo_config == "1" else 'NÚMERO_PREDIAL_LC PREDIO')
        self.col_new = col_snc_manual or 'NÚMERO_PREDIAL_SNC'
        self.col_estado = col_estado_manual or 'ESTADO'

        try:
            # Cargar como string para preservar ceros a la izquierda
            self.df = pd.read_excel(file_stream, dtype=str)
            
            # Normalización de cabeceras (Trim + Upper)
            self.df.columns = [c.strip().upper() for c in self.df.columns]
            col_map = {c: c for c in self.df.columns}

            # Si no hay manuales, aplicamos logica fuzzy/auto
            if not col_ant_manual:
                if self.col_ant not in col_map:
                    possible = [c for c in col_map if self.col_ant.replace('_',' ') in c.replace('_',' ')]
                    if possible: self.col_ant = possible[0]
                    elif len(self.df.columns) > 1 and tipo_config == "2":
                         self.col_ant = self.df.columns[1]

            if not col_snc_manual:
                if self.col_new not in col_map:
                    possible = [c for c in col_map if 'SNC' in c]
                    if possible: self.col_new = possible[0]
                    elif len(self.df.columns) > 0:
                         self.col_new = self.df.columns[0]
            
            # Filtro de activos (COMENTADO: Ahora procesamos todo el universo de datos)
            if not col_estado_manual:
                col_est_real = next((c for c in self.df.columns if 'ESTADO' in c), None)
                if col_est_real: self.col_estado = col_est_real
            
            # if self.col_estado in self.df.columns:
            #     self.df = self.df[self.df[self.col_estado].astype(str).str.upper().str.contains('ACTIVO', na=False)].copy()
            
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
            
            # Descomposición LADM: Mpio(5), Zona(2), Sect(2), Manz(4), Terr(4), CondProp(1) -> Pos 22 (Index 21)
            # Indices: 0-5 (M), 5-7 (Z), 7-9 (S), 13-17 (Mz), 17-21 (T), 21 (CondProp)
            return (True, [s[0:5], s[5:7], s[7:9], s[13:17], s[17:21], s[21]], 'OK')

        temp_data = self.df[self.col_new].apply(validar_estructura_snc)
        
        self.df['VALID_SNC'] = [t[0] for t in temp_data]
        self.df['SNC_PARTS'] = [t[1] for t in temp_data]
        
        # Reportar basura estructural inmediatamente
        invalidos = self.df[~self.df['VALID_SNC']]
        for idx, r in invalidos.iterrows():
            msg = temp_data.loc[idx][2]
            loc = f"{r[self.col_ant]}|{r[self.col_new]}"
            st = str(r.get(self.col_estado, 'N/A'))
            self.log_error('ESTRUCTURA_NPN', 'PRE-PROCESO', loc, msg, estado=st)
            
        # Filtrar solo válidos para la lógica de negocio
        self.df_clean = self.df[self.df['VALID_SNC']].copy()

        if self.df_clean.empty: return

        # Generar columnas numéricas para validación matemática
        cols_n = ['M_N', 'Z_N', 'S_N', 'MZ_N', 'T_N', 'COND_PROP']
        self.df_clean[cols_n] = pd.DataFrame(self.df_clean['SNC_PARTS'].tolist(), index=self.df_clean.index)
        for c in ['M_N_INT', 'Z_N_INT', 'S_N_INT', 'MZ_N_INT', 'T_N_INT']:
            base_col = c[:-4] # Remove _INT
            self.df_clean[c] = self.df_clean[base_col].astype(int)

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
                st = ", ".join(grupo[self.col_estado].unique().astype(str))
                self.log_error(
                    regla='UNICIDAD_SNC',
                    escenario='CRITICO',
                    ubicacion=loc,
                    detalle=f"NPN Duplicado. Asignado a {len(grupo)} orígenes distintos: {origenes}",
                    estado=st
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
            
            # Contexto Jerárquico para Logs
            ctx = {'zona': zona_n, 'sector': sect_n, 'manzana': ref['MZ_N']}
            
            # -----------------------------------------------------------------
            # A. VALIDACIONES DE PADRES (Zona / Sector / Manzana)
            # -----------------------------------------------------------------
            
            if escenario == 'NUEVO_CENTRO_POBLADO':
                st_ref = str(ref.get(self.col_estado, 'N/A'))
                # Instructivo: "El sector se inicia como 00"
                if int(sect_n) != 0:
                    self.log_error('NORMA_CP_SECTOR_00', escenario, loc_ref, 
                                   f"Primer sector de Zona Nueva debe ser 00. Se halló: {sect_n}", estado=st_ref, **ctx)
                
                # Instructivo: Manzana inicia en 0001
                if int(ref['MZ_N_INT']) != 1:
                    self.log_error('NORMA_CP_MANZANA_01', escenario, loc_ref, 
                                   f"Primera manzana de Zona Nueva debe ser 0001. Se halló: {ref['MZ_N']}", estado=st_ref, **ctx)

            elif escenario == 'NUEVO_SECTOR':
                key_z = f"{mpio_n}-{zona_n}"
                last_sect = self.memoria['sector'].get(key_z, -1) # -1 = Sin histórico
                actual_sect = int(ref['S_N_INT'])
                st_ref = str(ref.get(self.col_estado, 'N/A'))
                
                # Validar Consecutividad (Last + 1)
                if last_sect != -1 and actual_sect != last_sect + 1:
                    self.log_error('CONSECUTIVIDAD_SECTOR', escenario, loc_ref, 
                                   f"Salto de sector indebido en Zona {zona_n}. Anterior: {last_sect}, Nuevo: {actual_sect}", estado=st_ref, **ctx)
                
                # Actualizar Memoria
                self.memoria['sector'][key_z] = max(last_sect, actual_sect)
                
                # En sector nuevo, la manzana debería iniciar o reiniciar secuencia
                if int(ref['MZ_N_INT']) != 1:
                     self.log_error('INICIO_MANZANA_SECTOR', escenario, loc_ref,
                                    f"Manzana en sector nuevo inició en {ref['MZ_N']} (se esperaba 0001)", severidad='WARNING', estado=st_ref, **ctx)

            # REGLA: MANZANA NUEVA
            # Validamos salto de manzana dentro del sector
            if escenario in ['NUEVA_MANZANA', 'NUEVO_SECTOR']:
                key_s = f"{mpio_n}-{zona_n}-{sect_n}"
                last_manz = self.memoria['manzana'].get(key_s, 0)
                actual_manz = int(ref['MZ_N_INT'])
                
                # Si es manzana nueva en sector viejo, debe ser Last + 1
                if escenario == 'NUEVA_MANZANA' and actual_manz > last_manz + 1 and actual_manz != 1:
                    st_ref = str(ref.get(self.col_estado, 'N/A'))
                    self.log_error('CONSECUTIVIDAD_MANZANA', escenario, loc_ref,
                                   f"Salto de manzana. Anterior {last_manz}, Nueva {actual_manz}", estado=st_ref, **ctx)
                
                self.memoria['manzana'][key_s] = max(last_manz, actual_manz)

            # -----------------------------------------------------------------
            # B. VALIDACIÓN DE HIJOS (TERRENOS) - MATEMÁTICA DE LOTES
            # -----------------------------------------------------------------
            
            # Detectar si la manzana temporal se dispersó en varias definitivas (Dispersión)
            destinos = lote[['M_N', 'Z_N', 'S_N', 'MZ_N']].drop_duplicates()
            if len(destinos) > 1:
                st_ref = str(ref.get(self.col_estado, 'N/A'))
                self.log_error('DISPERSION_LOTE', escenario, loc_ref,
                               f"Predios de un mismo lote temporal terminaron en {len(destinos)} manzanas definitivas distintas.", severidad='WARNING', estado=st_ref, **ctx)

            # Iterar cada manzana de destino para validar sus terrenos internos
            for _, dest in destinos.iterrows():
                sub_lote = lote[lote['MZ_N'] == dest['MZ_N']]
                vals = sorted(sub_lote['T_N_INT'].unique())
                
                min_t, max_t, count_t = min(vals), max(vals), len(vals)
                
                sub_ref = sub_lote.iloc[0]
                sub_loc = f"{sub_ref[self.col_ant]}|{sub_ref[self.col_new]}"
                
                # Update Context for sub-loop if needed (though usually same zone/sect)
                sub_ctx = {'zona': dest['Z_N'], 'sector': dest['S_N'], 'manzana': dest['MZ_N']}

                sub_st = str(sub_ref.get(self.col_estado, 'N/A'))
                # 1. CHEQUEO DE HUECOS (GAPS)
                if (max_t - min_t + 1) != count_t:
                    self.log_error('HUECOS_NUMERACION', escenario, sub_loc,
                                   f"Mz {dest['MZ_N']}: Secuencia interrumpida. Rango {min_t}-{max_t} ({max_t-min_t+1} espacios) para {count_t} predios.", estado=sub_st, **sub_ctx)
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
                    salto = min_t - expected_start
                    severidad = 'ERROR' if salto > 1000 else 'WARNING'
                    self.log_error('INICIO_SECUENCIA', escenario, sub_loc,
                                   f"Mz {dest['MZ_N']}: Terrenos iniciaron en {min_t}, se esperaba {expected_start} (basado en historia {last_t}). Salto: {salto}", 
                                   severidad=severidad, estado=sub_st, **sub_ctx)
                
                # Actualizar Memoria Terreno
                self.memoria['terreno'][key_m] = max(last_t, max_t)

    # =========================================================================
    # 5.1. SUGERENCIA DE RENUMERACIÓN (NUEVO v3.3 - SOPORTE 9xxx)
    # =========================================================================
    def generar_sugerencias(self):
        """
        Genera una columna SUGGESTED_SNC corrigiendo la secuencia.
        Maneja tres niveles:
        1. FORMAL Mz: Si Manzana es 9xxx -> Asignar Siguiente Disponble en Sector.
        2. FORMAL Terr: Secuencia por Manzana (Terreno 1..N).
        3. INFORMAL (No Ley 14): Secuencia por Predio Matriz (Parent 21 chars + 200000 + xxx).
        """
        if self.df_clean.empty: return

        # Inicializar columna
        self.df_clean['SUGGESTED_SNC'] = ''
        self.df_clean['ES_INFORMAL'] = self.df_clean['COND_PROP'] == '2'

        # --- FASE 1: PRE-CALENTAMIENTO DE MEMORIA DE MANZANAS ---
        # Necesitamos saber cuál es la última manzana REAL (no 9xxx) de cada sector
        # para que si aparece una 9001, le asignemos la siguiente correcta (ej 0015).
        
        mem_manzana = {} # Key: M-Z-S -> MaxMz(int)
        
        # Filtramos solo las definitivas (< 9000)
        definitivas = self.df_clean[self.df_clean['MZ_N_INT'] < 9000]
        if not definitivas.empty:
            # Agrupamos por Sector (M-Z-S) y buscamos el maximo
            max_mz_per_sector = definitivas.groupby(['M_N', 'Z_N', 'S_N'])['MZ_N_INT'].max()
            for idx, val in max_mz_per_sector.items():
                key = f"{idx[0]}-{idx[1]}-{idx[2]}" # M-Z-S
                mem_manzana[key] = int(val)

        # Mapa para mantener consistencia: Si reasignamos la Mz 9001 a la 0015,
        # todos los predios de la 9001 deben ir a la 0015.
        map_manzana_temp = {} # Key: M-Z-S-MzOld -> MzNew(str)

        # --- FASE 2: PROCESAMIENTO SECUENCIAL ---
        
        # Memoria para Terrenos (Formal) e Informales
        mem_formal_terr = {} # Key: M-Z-S-Mz(Nueva) -> LastTerr(int)
        mem_informal = {}    # Key: Parent21 -> LastSeq(int)

        # Ordenar por Predio Anterior para respetar orden de llegada
        sort_cols = ['M_A', 'Z_A', 'S_A', 'MZ_A', 'T_A']
        for c in sort_cols:
             if c not in self.df_clean.columns: continue
             self.df_clean[c] = self.df_clean[c].astype(str).replace('nan', '00')
        
        df_sorted = self.df_clean.sort_values(by=sort_cols)
        suggested_list = []
        
        for idx, row in df_sorted.iterrows():
            suggested = ''
            
            # Contexto original
            mpio = row['M_N']
            zona = row['Z_N']
            sect = row['S_N']
            manz_orig = row['MZ_N']
            manz_int = row['MZ_N_INT']
            cond = row['COND_PROP']
            full_snc = str(row[self.col_new])
            
            # --- LÓGICA DE MANZANA (NIVEL 1) ---
            target_mz_str = manz_orig
            key_sector = f"{mpio}-{zona}-{sect}"
            
            # Solo aplicamos renumeracion de manzana si es NO INFORMAL (Formal)
            # o si el usuario quiere que la geografia informal tambien se corrija.
            # Asumiremos que la correccion de Manzana aplica a TODOS (Formales e Informales)
            # porque la ubicacion geografica es la base.
            
            if manz_int >= 9000:
                key_temp = f"{key_sector}-{manz_orig}"
                if key_temp in map_manzana_temp:
                    target_mz_str = map_manzana_temp[key_temp]
                else:
                    # Asignar nueva manzana
                    last_mz = mem_manzana.get(key_sector, 0)
                    new_mz = last_mz + 1
                    mem_manzana[key_sector] = new_mz # Actualizar max global
                    target_mz_str = str(new_mz).zfill(4)
                    map_manzana_temp[key_temp] = target_mz_str
            
            # --- CONSTRUCCION DE LA SUGERENCIA ---
            
            # Recuperar partes "ocultas" del string original (Comuna/Barrio)
            # 0-5(M), 5-7(Z), 7-9(S), 9-13(Gap: Comuna+Barrio), 13-17(Mz)
            gap_chunk = full_snc[9:13] 
            
            # Nuevo Prefijo con Manzana Corregida
            # M(5)+Z(2)+S(2)+Gap(4)+MzNueva(4) = 17 chars
            prefix_geo = f"{mpio}{zona}{sect}{gap_chunk}{target_mz_str}"

            # --- CASO INFORMAL (NO LEY 14) ---
            if cond == '2':
                # Parent = M(5)+Z(2)+S(2)+Gap(4)+Mz(4)+Terr(4). Total 21.
                # Usamos el target_mz_str corregido, pero mantenemos el Terreno original?
                # Si la manzana cambió (era 9000 -> 0015), el terreno original (ej 0001) se mantiene
                # pero ahora bajo la nueva manzana.
                
                # OJO: Si es informal, el 'Parent' incluye el Terreno.
                # Debemos usar el terreno original row['T_N']?
                # Si estamos renumerando Manzana, el Parent ID cambia (porque la Mz cambia).
                
                terr_orig = row['T_N']
                parent_id = f"{prefix_geo}{terr_orig}" # 17 + 4 = 21 chars
                
                # Secuencia Informal
                last_seq = mem_informal.get(parent_id, 0)
                next_seq = last_seq + 1
                mem_informal[parent_id] = next_seq
                
                # Sufijo: '200000' + seq(3) -> 9 chars
                suggested = f"{parent_id}200000{str(next_seq).zfill(3)}"

            # --- CASO FORMAL ---
            else:
                # Renumeracion de Terrenos dentro de la (Nueva) Manzana
                # Key para memoria de terrenos: Prefijo Geo (hasta Manzana)
                key_manzana_terr = prefix_geo # 17 chars
                
                last_terr = mem_formal_terr.get(key_manzana_terr, 0)
                next_terr = last_terr + 1
                mem_formal_terr[key_manzana_terr] = next_terr
                
                terr_str = str(next_terr).zfill(4)
                
                # Sufijo original (Cond + Resto) -> Desde char 21
                suffix_original = full_snc[21:]
                
                # Sugerencia: Geo(17) + Terr(4) + SuffixOrig
                suggested = f"{prefix_geo}{terr_str}{suffix_original}"

            suggested_list.append(suggested)
            
        # Asignar al DF principal
        self.df_clean.loc[df_sorted.index, 'SUGGESTED_SNC'] = suggested_list
        
        # Flag de match
        self.df_clean['MATCH_SUGGESTION'] = self.df_clean[self.col_new] == self.df_clean['SUGGESTED_SNC']

    # =========================================================================
    # 6. REPORTES (Web Adapter)
    # =========================================================================
    # =========================================================================
    # 6. REPORTES (Web Adapter v3.2)
    # =========================================================================
    def generar_reporte_excel(self):
        output = io.BytesIO()
        # Usamos xlsxwriter que es robusto para escritura nueva
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
            
            # Hoja 2: Errores Globales
            if self.errores:
                pd.DataFrame(self.errores).to_excel(writer, sheet_name='ERRORES_GLOBAL', index=False)
            else:
                pd.DataFrame({'ESTADO': ['SIN ERRORES']}).to_excel(writer, sheet_name='ERRORES_GLOBAL', index=False)
                
            # Hoja 3: Advertencias Globales
            if self.warnings:
                pd.DataFrame(self.warnings).to_excel(writer, sheet_name='ADVERTENCIAS_GLOBAL', index=False)
                
            # Hojas por ZONA (La gran mejora de v3.2)
            if self.df_clean is not None and not self.df_clean.empty and 'Z_N' in self.df_clean.columns:
                zonas_unicas = sorted(self.df_clean['Z_N'].unique())
                for zona in zonas_unicas:
                    # Logs de esta zona
                    err_zona = [e for e in self.errores if e.get('ZONA') == zona]
                    warn_zona = [w for w in self.warnings if w.get('ZONA') == zona]
                    
                    if not err_zona and not warn_zona:
                        continue
                        
                    # Resumen por Sector/Manzana
                    data_z = []
                    
                    # Convert log list to DF for aggregation
                    if err_zona:
                        df_ez = pd.DataFrame(err_zona)
                        # Agregamos counts
                        grp_e = df_ez.groupby(['SECTOR', 'MANZANA', 'REGLA']).size().reset_index(name='COUNT_ERR')
                        data_z.append(grp_e)
                        
                    if warn_zona:
                        df_wz = pd.DataFrame(warn_zona)
                        grp_w = df_wz.groupby(['SECTOR', 'MANZANA', 'REGLA']).size().reset_index(name='COUNT_WARN')
                        data_z.append(grp_w)
                    
                    if data_z:
                         # Merge or concat? Concat is simpler for a summary list
                         full_z = pd.concat(data_z, ignore_index=True)
                         full_z.to_excel(writer, sheet_name=f'ZONA_{zona}_RESUMEN', index=False)

            # Hoja Final: Data Tagged
            if self.df_clean is not None and not self.df_clean.empty:
                cols = [self.col_ant, self.col_new, 'SUGGESTED_SNC', 'MATCH_SUGGESTION', self.col_estado, 'COND_PROP', 'ESCENARIO', 'Z_N', 'S_N', 'MZ_N']
                # Filtrar cols que existen
                cols = [c for c in cols if c in self.df_clean.columns]
                self.df_clean[cols].head(5000).to_excel(writer, sheet_name='DATA_TAGGED', index=False)
                
        output.seek(0)
        return output

# =============================================================================
# FUNCIONES WRAPPER (INTEGRACIÓN FLASK)
# =============================================================================

def procesar_renumeracion(file_stream, tipo_config, col_snc_manual=None, col_ant_manual=None, col_estado_manual=None):
    """Interfaz principal requerida por app.py"""
    
    engine = AuditoriaSNC()
    
    # 1. Cargar
    if not engine.cargar_datos(file_stream, tipo_config, col_snc_manual, col_ant_manual, col_estado_manual):
        raise ValueError("Error leyendo el archivo. Verifique formato Excel.")
    
    # 2. Procesar Pipeline
    engine.parsear_y_limpiar()
    engine.validar_unicidad_absoluta()
    engine.inicializar_memoria()
    engine.validar_lotes()
    engine.generar_sugerencias()
    
    # Adaptar para PDF antiguo y Dashboard
    todos_errores = engine.errores + engine.warnings
    final_errors = []
    for e in todos_errores:
        prefix = "[ADVERTENCIA] " if e['TIPO'] != 'ERROR' else ""
        final_errors.append({
            'REGLA': f"{prefix}{e['REGLA']}",
            'DETALLE': f"{e['ESCENARIO']} ({e.get('ESTADO', 'N/A')}): {e['DETALLE']}",
            'ANTERIOR': e['ANTERIOR'],
            'NUEVO': e['NUEVO'],
            'ZONA': e.get('ZONA'),
            'SECTOR': e.get('SECTOR'),
            'MANZANA': e.get('MANZANA'),
            'ESTADO': e.get('ESTADO', 'N/A'),
            'TIPO_REAL': e['TIPO']
        })
        
    t_err = (len(engine.errores) / engine.stats['total_filas'] * 100) if engine.stats['total_filas'] > 0 else 0
    
    # Crear Mapa de Sugerencias para UI
    suggestion_map = {}
    if engine.df_clean is not None and not engine.df_clean.empty and 'SUGGESTED_SNC' in engine.df_clean.columns:
        # Asegurarse de que las claves sean strings para el lookup
        suggestion_map = dict(zip(engine.df_clean[engine.col_new].astype(str), engine.df_clean['SUGGESTED_SNC'].astype(str)))

    c_p = {}
    for e in final_errors:
        c = e['NUEVO']
        # Attach suggestion if available
        e['SUGGESTED'] = suggestion_map.get(str(c), 'N/A')
        
        if c not in c_p: c_p[c] = []
        c_p[c].append(e['REGLA'])
    top_p = sorted([(c, len(r), ', '.join(set(r))) for c, r in c_p.items()], key=lambda x: x[1], reverse=True)[:10]

    # Calcular contadores para el dashboard (Para evitar problemas de scope en Jinja2)
    counts = {
        'unicidad': len([e for e in final_errors if 'UNICIDAD' in e['REGLA']]),
        'estructura': len([e for e in final_errors if 'ESTRUCTURA' in e['REGLA']]),
        'consecutividad': len([e for e in final_errors if any(x in e['REGLA'] for x in ['CONSECUTIVIDAD', 'INICIO', 'NORMA'])]),
        'huecos': len([e for e in final_errors if 'HUECOS' in e['REGLA']])
    }

    return {
        'total_auditado': engine.stats['total_filas'],
        'errores': final_errors,
        'stats': engine.stats,
        'counts': counts,
        'diccionario_estados': {},
        'success': True,
        'tipo_config': tipo_config,
        'timestamp': datetime.now(timezone(timedelta(hours=-5))).strftime('%Y-%m-%d %H:%M:%S'),
        'tasa_error': round(t_err, 2),
        'top_problematicos': top_p
        # No enviamos engine_instance porque falla al serializar JSON
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
    """Genera un reporte PDF detallado v3.2 con desglose por Zona"""
    pdf = AuditoriaRenumeracionPDF(format='Letter'); pdf.set_margins(20, 20, 20); pdf.add_page()
    t_c = resultados.get('tipo_config', '1')
    l_comp, l_ant = ('CICA vs SNC', 'CICA') if t_c == '1' else ('Operadores vs SNC', 'Operadores')
    
    # Header Info
    pdf.ln(2); pdf.set_font('Helvetica', '', 10); pdf.set_text_color(107, 114, 128)
    pdf.cell(0, 6, f"Auditoría: {l_comp}  |  Fecha: {resultados.get('timestamp', 'N/A')}", 0, 1, 'L'); pdf.ln(5)
    
    # Resumen Ejecutivo
    pdf.set_font('Helvetica', 'B', 14); pdf.set_text_color(17, 17, 17); pdf.cell(0, 10, 'Resumen Ejecutivo (V3.2)', 0, 1); pdf.ln(2)
    def add_meta(l, v, c=(0,0,0)):
        pdf.set_font('Helvetica', '', 10); pdf.set_text_color(107, 114, 128); pdf.cell(70, 8, l, 0); pdf.set_font('Helvetica', 'B', 10); pdf.set_text_color(*c); pdf.cell(0, 8, v, 0, 1); pdf.set_text_color(0,0,0)
    
    # Intentar obtener estadísticas
    engine_stats = resultados.get('stats', {})
    errores_list = resultados.get('errores', [])
    
    # Separar errores de advertencias si es posible
    # Nota: errores_list ya tiene el prefijo [ADVERTENCIA] del wrapper procesar_renumeracion
    count_err = engine_stats.get('errores_criticos', len([e for e in errores_list if '[ADVERTENCIA]' not in str(e.get('REGLA', ''))]))
    count_warn = engine_stats.get('advertencias', len([e for e in errores_list if '[ADVERTENCIA]' in str(e.get('REGLA', ''))]))
    
    add_meta('Errores Críticos:', f"{count_err:,}", (220, 38, 38) if count_err > 0 else (22, 163, 74))
    add_meta('Advertencias:', f"{count_warn:,}", (202, 138, 4) if count_warn > 0 else (22, 163, 74))
    
    pdf.ln(5)

    # --- DESGLOSE POR ZONA (V3.2) ---
    if errores_list:
        pdf.set_font('Helvetica', 'B', 12); pdf.set_text_color(17, 17, 17); pdf.cell(0, 10, 'Hallazgos por Zona', 0, 1); pdf.ln(2)
        
        # Agrupar hallazgos por zona
        hallazgos_zona = {} # Zona -> {'E': 0, 'W': 0}
        for f in errores_list:
            z = f.get('ZONA', 'UNK')
            if z is None or str(z).lower() == 'nan': z = 'UNK'
            if z not in hallazgos_zona: hallazgos_zona[z] = {'E': 0, 'W': 0}
            if '[ADVERTENCIA]' not in str(f.get('REGLA', '')):
                hallazgos_zona[z]['E'] += 1
            else:
                hallazgos_zona[z]['W'] += 1
            
        # Tabla de Zonas
        pdf.set_font('Helvetica', 'B', 9); pdf.set_fill_color(243, 244, 246)
        pdf.cell(40, 8, 'Zona', 1, 0, 'C', 1)
        pdf.cell(50, 8, 'Errores Críticos', 1, 0, 'C', 1)
        pdf.cell(50, 8, 'Advertencias', 1, 1, 'C', 1)
        
        pdf.set_font('Helvetica', '', 9); pdf.set_fill_color(255, 255, 255)
        for z in sorted(hallazgos_zona.keys()):
            e_c = hallazgos_zona[z]['E']
            w_c = hallazgos_zona[z]['W']
            pdf.cell(40, 7, f"ZONA {z}", 1, 0, 'C')
            
            # Color rojo si hay errores
            if e_c > 0: pdf.set_text_color(220, 38, 38)
            pdf.cell(50, 7, str(e_c), 1, 0, 'C')
            pdf.set_text_color(0, 0, 0)
            
            if w_c > 0: pdf.set_text_color(202, 138, 4)
            pdf.cell(50, 7, str(w_c), 1, 1, 'C')
            pdf.set_text_color(0, 0, 0)
        
        pdf.ln(5)


    # --- DETALLE DE ALERTAS (MUESTRA) ---
    pdf.add_page(); pdf.set_font('Helvetica', 'B', 12); pdf.cell(0, 10, 'Detalle de Alertas Lógicas (Muestra)', 0, 1); pdf.ln(2)
    
    # Definición de Columnas (Ancho total ~176)
    w_reg = 38; w_det = 62; w_cica = 38; w_snc = 38
    lh = 3.5 # Altura de cada línea (interlineado)
    
    # Header
    pdf.set_font('Helvetica', 'B', 8); pdf.set_fill_color(243, 244, 246)
    pdf.cell(w_reg, 8, 'Regla', 1, 0, 'C', 1)
    pdf.cell(w_det, 8, 'Detalle', 1, 0, 'C', 1)
    pdf.cell(w_cica, 8, f'{l_ant}', 1, 0, 'C', 1)
    pdf.cell(w_snc, 8, 'Nuevo (SNC)', 1, 1, 'C', 1)
    
    # Usamos la lista combinada
    lista_final = resultados.get('errores', []) 
    for e in lista_final[:200]:
        # Calculamos número de líneas aproximado por columna
        t_reg = str(e.get('REGLA', ''))
        t_det = str(e.get('DETALLE', ''))
        t_ant = str(e.get('ANTERIOR', ''))
        t_snc = str(e.get('NUEVO', ''))
        
        # Estimación de líneas (basado en caracteres aprox por ancho)
        n_reg = max(1, (len(t_reg) // 22) + 1)
        n_det = max(1, (len(t_det) // 50) + 1)
        n_ant = max(1, (len(t_ant) // 28) + 1)
        n_snc = max(1, (len(t_snc) // 28) + 1)
        
        max_n = max(n_reg, n_det, n_ant, n_snc)
        h_row = max_n * lh + 2 # Altura total de la fila con pequeño padding
        
        # Salto de página si no cabe
        if pdf.get_y() + h_row > 260: 
            pdf.add_page(); 
            pdf.set_font('Helvetica', 'B', 8); pdf.set_fill_color(243, 244, 246)
            pdf.cell(w_reg, 8, 'Regla', 1, 0, 'C', 1)
            pdf.cell(w_det, 8, 'Detalle', 1, 0, 'C', 1)
            pdf.cell(w_cica, 8, f'{l_ant}', 1, 0, 'C', 1)
            pdf.cell(w_snc, 8, 'Nuevo (SNC)', 1, 1, 'C', 1)
        
        # Dibujar fila celda por celda
        x, y = pdf.get_x(), pdf.get_y()
        
        # 1. REGLA
        pdf.set_font('Helvetica', '', 6)
        pdf.multi_cell(w_reg, lh, t_reg, border=0, align='L')
        pdf.rect(x, y, w_reg, h_row) # Borde manual para que todas midan h_row
        
        # 2. DETALLE
        pdf.set_xy(x + w_reg, y)
        pdf.multi_cell(w_det, lh, t_det, border=0, align='L')
        pdf.rect(x + w_reg, y, w_det, h_row)
        
        # 3. ANTERIOR (CICA)
        pdf.set_xy(x + w_reg + w_det, y)
        pdf.set_font('Helvetica', '', 5.5)
        pdf.multi_cell(w_cica, h_row if n_ant == 1 else lh, t_ant, border=0, align='C')
        pdf.rect(x + w_reg + w_det, y, w_cica, h_row)
        
        # 4. NUEVO (SNC)
        pdf.set_xy(x + w_reg + w_det + w_cica, y)
        pdf.multi_cell(w_snc, h_row if n_snc == 1 else lh, t_snc, border=0, align='C')
        pdf.rect(x + w_reg + w_det + w_cica, y, w_snc, h_row)
        
        pdf.set_xy(x, y + h_row)

    return bytes(pdf.output())

# Stub para funciones geo que no estaban en V3 pero quizas se requieran para no romper imports
def procesar_geografica(*args, **kwargs):
    return [], {'stats_geo': {}}
