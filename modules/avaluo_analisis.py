import pandas as pd
import numpy as np

# Intentar importar ftfy para arreglar encoding
try:
    import ftfy
except ImportError:
    ftfy = None


# Configuración de cortes Registro 1 (SNC Estándar)
CORTES_R1 = [0, 2, 5, 30, 31, 34, 37, 137, 138, 139, 151, 251, 252, 253, 268, 274, 289, 297, 312]
COLS_R1 = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno", "AreaConstruida", "Avaluo", "Vigencia", "NoPredialAnterior", "Espacio_Final"]

def generar_colspecs(cortes):
    colspecs = []
    for i in range(len(cortes) - 1):
        colspecs.append((cortes[i], cortes[i+1]))
    colspecs.append((cortes[-1], None))
    return colspecs



    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico']]

    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico']]

def cargar_snc(stream):
    """Carga data desde archivo plano (Fixed Width) o Excel (.xlsx)"""
    # Detectar tipo por extensión (si disponible) o magic bytes si es necesario.
    # Asumimos que stream tiene atributo filename si viene de Flask/Werkzeug
    filename = getattr(stream, 'filename', '').lower()
    
    # 1. Carga
    if filename.endswith(('.xlsx', '.xls')):
        # Carga Excel
        # Asumimos que el Excel tiene encabezados en la fila 0 y nombres de columnas similares
        # O si no tiene headers, usamos header=None y asignamos. 
        # Para compatibilidad, intentaremos leer y luego normalizar columnas.
        # Si el usuario guarda el TXT como Excel, probablemente tenga las columnas separadas o todo en una.
        # Asumiremos un Excel "bien formado" con columnas separadas.
        try:
            df = pd.read_excel(stream, dtype=str)
             # Normalización básica de nombres de columnas si vienen del Excel
            # Si el excel no tiene headers, asumimos el orden estándar
            if len(df.columns) == len(COLS_R1):
                 df.columns = COLS_R1
            
            # Limpieza básica
            df = df.fillna('')
        except Exception as e:
             raise ValueError(f"Error leyendo Excel: {str(e)}")
    else:
        # Carga Archivo Plano (Lógica Original)
        # validar_archivo(stream) # Ya no validamos extensiones restrictivamente, o solo bloqueamos binarios NO excel
        colspecs = generar_colspecs(CORTES_R1)
        
        # INTENTO 1: Leer como UTF-8 (Estándar moderno)
        try:
            # Necesitamos 'seek(0)' si el stream ya fue leído, pero aquí es la primera vez (para pandas)
            # Pero pd.read_fwf puede consumir el stream. Hacemos copia o reiniciamos stream si falla?
            # Stream de Flask suele ser seekable.
            pos = stream.tell() if hasattr(stream, 'tell') else 0
            df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='utf-8', dtype=str)
        except (UnicodeDecodeError, Exception):
            # INTENTO 2: Fallback a Latin-1 (Legacy común)
            if hasattr(stream, 'seek'): stream.seek(pos)
            df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)

        if len(df.columns) == len(COLS_R1):
            df.columns = COLS_R1
        else:
             df.columns = COLS_R1[:len(df.columns)]
    
    # 2. Procesamiento Común
    # Limpieza
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # FIX ENCODING: Reparar Mojibake en Nombres (ej: PEÃ‘A -> PEÑA)
    if 'Nombre' in df.columns:
        if ftfy:
            df['Nombre'] = df['Nombre'].apply(lambda x: ftfy.fix_text(str(x)))
        else:
            # Reparación manual básica si no hay FTFY
            # Detecta patrones UTF-8 interpretados como Latin-1
            # Ã‘ -> Ñ
            replacements = {
                'Ã‘': 'Ñ', 'Ã±': 'ñ',
                'Ã ': 'Á', 'Ã¡': 'á',
                'Ã‰': 'É', 'Ã©': 'é',
                'Ã ': 'Í', 'Ãed': 'í', 
                'Ã“': 'Ó', 'Ã³': 'ó',
                'Ãš': 'Ú', 'Ãº': 'ú',
                'Ãœ': 'Ü', 'Ã¼': 'ü'
            }
            # Un replace simple puede no cubrir todo, pero ayuda.
            for bad, good in replacements.items():
                df['Nombre'] = df['Nombre'].str.replace(bad, good, regex=False)
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Construcción Llave 30 Dígitos
    # Aseguramos que existan las columnas necesarias
    required_cols = ['Departamento', 'Municipio', 'NoPredial']
    if not all(col in df.columns for col in required_cols):
         pass 

    # IMPORTANTE: NoPredial suele tener 25 dígitos. Los primeros 2 son la ZONA.
    # Si viene sin ceros a la izquierda (ej: Excel), zfill(25) los pone.
    # Ej: ZONA 01 -> "1..." -> zfill -> "...01..." (MAL)
    # Ej: ZONA 01 -> "01..." -> zfill -> "01..." (BIEN)
    # Asumiremos que si es EXCEL, el usuario debe cuidar el formato texto, pero intentaremos LJUST si parece corto?
    # No, estándar catastral es ceros a la IZQUIERDA. 
    # Si el usuario sube Excel y el excel le quitó los ceros -> "1000022" (Zona 01?) No se sabe.
    # Asumiremos zfill(2) para Depto, zfill(3) para Mun, zfill(25) para Predial.
    
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(2) + 
        df['Municipio'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(3) + 
        df['NoPredial'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(25)
    )
    
    # Eliminar duplicados
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    
    # Retornar columnas clave y el NoPredial limpio para filtrar
    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico', 'Municipio']]

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural, sample_pct=100, zona_filter='TODOS'):
    # Leer Dataframes
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post) # Aquí sample_pct y zona_filter se aplican DESPUÉS para simplificar, 
                                    # o podemos inyectar el filtro antes si cargamos todo.
                                    # Dado que cargar_snc retorna todo, filtramos en memoria.
    
    # 1.1 FILTRO ZONAL
    # Zona está en posiciones 5:7 del Predial Nacional (30 char).
    # 0-2 (Depto), 2-5 (Mun), 5-7 (ZONA). 
    # Ejemplo: 25 204 01... (Urbano) -> 2520401...
    if zona_filter != 'TODOS':
        # Definimos códigos válidos para cada selección
        target_zones = []
        if zona_filter == 'URBANO': target_zones = ['01']
        elif zona_filter == 'RURAL': target_zones = ['00']
        
        # Filtramos DF Pre y Post
        # slice(5,7) toma caracteres 5 y 6.
        if zona_filter in ['URBANO', 'RURAL']:
            df_pre = df_pre[df_pre['Predial_Nacional'].str.slice(5, 7).isin(target_zones)]
            df_post = df_post[df_post['Predial_Nacional'].str.slice(5, 7).isin(target_zones)]
        elif zona_filter == 'CORREG':
             # Todo lo que NO sea 00 ni 01
             df_pre = df_pre[~df_pre['Predial_Nacional'].str.slice(5, 7).isin(['00', '01'])]
             df_post = df_post[~df_post['Predial_Nacional'].str.slice(5, 7).isin(['00', '01'])]

    # 1.2 MUESTREO (MOVED AFTER MERGE/CALCS to allow Universe Stats)
    # Anteriormente aquí se hacía sampling antes del merge.
    # AHORA: Haremos merge del universo filtrado, calculamos todo, y al final hacemos sampling para la vista 'data'.
    
    # 2. Unión Total (Outer Join) del Universo Filtrado
    df_final = pd.merge(
        df_pre, 
        df_post, 
        on='Predial_Nacional', 
        how='outer', 
        suffixes=('_pre', '_post'), 
        indicator=True
    )
    
    # 3. Normalización de Datos Fusionados
    df_final['Avaluo_pre'] = df_final['Avaluo_pre'].fillna(0)
    df_final['Avaluo_post'] = df_final['Avaluo_post'].fillna(0)
    
    # Si es nuevo, toma nombre del post; si es viejo, del pre
    df_final['Nombre'] = df_final['Nombre_pre'].combine_first(df_final['Nombre_post']).fillna('SIN NOMBRE')
    df_final['Destino'] = df_final['DestinoEconomico_pre'].combine_first(df_final['DestinoEconomico_post']).fillna('-')
    df_final['Municipio'] = df_final['Municipio_pre'].combine_first(df_final['Municipio_post']).fillna('000')

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    # --- LOGICA VECTORIZADA ---
    
    # 1. Detectar Zona (Rural si 5-6 caracteres son '00')
    # Convertimos a string y sacamos slice. Ojo con NaN o cortos.
    # Zfill ya garantizó 30 dígitos en 'Predial_Nacional' en cargar_snc, así que es seguro.
    zona_vals = df_final['Predial_Nacional'].astype(str).str.slice(5, 7)
    is_rural = (zona_vals == '00')
    
    df_final['Zona'] = np.where(is_rural, 'RURAL', 'URBANO')
    
    # 2. Factores y Pct Teorico
    # Si es rural: pct_rur_decimal, sino pct_urb_decimal
    df_final['Pct_Teorico'] = np.where(is_rural, pct_rur_decimal, pct_urb_decimal)
    
    # factor n = 1 + pct
    factors = 1 + df_final['Pct_Teorico']
    
    # 3. Calculado
    # Logica: redondear_excel(avaluo_base * factor)
    # redondear_excel usa ROUND_HALF_UP a miles (1E3).
    # En numpy: floor(x/1000 + 0.5) * 1000 imita ROUND_HALF_UP para positivos.
    
    vals_teoricos = df_final['Avaluo_pre'] * factors
    # Evitar problemas con float, sumamos un epsilon muy pequeño si fuera necesario, 
    # pero +0.5 suele bastar para "half up".
    df_final['Calculado'] = np.floor((vals_teoricos / 1000) + 0.5) * 1000
    
    # Caso 'right_only' (NUEVO) -> Calculado = 0
    df_final['Calculado'] = np.where(df_final['_merge'] == 'right_only', 0, df_final['Calculado'])
    
    # 4. Diferencia
    # left_only (DESAPARECIDO): 0 - Calculado
    # right_only (NUEVO): Avaluo_post
    # both: Calculado - Avaluo_post
    
    # Usamos np.select
    cond_diff = [
        df_final['_merge'] == 'left_only',
        df_final['_merge'] == 'right_only'
    ]
    choice_diff = [
        0 - df_final['Calculado'],
        df_final['Avaluo_post']
    ]
    # default (both)
    default_diff = df_final['Calculado'] - df_final['Avaluo_post']
    
    df_final['Diferencia'] = np.select(cond_diff, choice_diff, default=default_diff)
    
    # 5. Porcentaje Real
    # (Avaluo_sistema / Avaluo_base) - 1
    # Cuidar división por cero
    with np.errstate(divide='ignore', invalid='ignore'):
        df_final['Pct_Real'] = (df_final['Avaluo_post'] / df_final['Avaluo_pre']) - 1
    
    # Si Base es 0 o NaN, Pct_Real queda inf/nan, lo ponemos en 0 o '-' (luego en json se maneja)
    df_final['Pct_Real'] = df_final['Pct_Real'].fillna(0)
    df_final['Pct_Real'] = np.where(np.isinf(df_final['Pct_Real']), 0, df_final['Pct_Real'])


    # 6. Estados
    # Prioridad:
    # 1. left_only -> DESAPARECIDO
    # 2. right_only -> NUEVO
    # 3. Diferencia == 0 -> OK
    # 4. Base == Calculado -> SIN_AUMENTO (el redondeo anuló el aumento)
    # 5. else -> INCONSISTENCIA
    
    conds_estado = [
        df_final['_merge'] == 'left_only',
        df_final['_merge'] == 'right_only',
        df_final['Diferencia'] == 0,
        df_final['Avaluo_pre'] == df_final['Calculado']
    ]
    choices_estado = [
        'DESAPARECIDO',
        'NUEVO',
        'OK',
        'SIN_AUMENTO'
    ]
    
    df_final['Estado'] = np.select(conds_estado, choices_estado, default='INCONSISTENCIA')
    
    # Limpieza final de columnas numéricas
    df_final['Calculado'] = df_final['Calculado'].astype(int)
    df_final['Diferencia'] = df_final['Diferencia'].astype(int)
    
    # 4. Estadísticas Generales (KPIs)
    # 4. Estadísticas Generales (KPIs)
    
    # 4. Estadísticas Generales (KPIs) (Sobre el Universo para cálculos globales)
    avaluos_sist_full = df_final['Avaluo_post']
    mean_val = avaluos_sist_full.mean()
    median_val = avaluos_sist_full.median()
    std_val = avaluos_sist_full.std()
    mode_series = avaluos_sist_full.mode()
    mode_val = mode_series.iloc[0] if not mode_series.empty else 0

    # COMPARADOR GLOBAL: Extraer listas completas del universo FILTRADO por zona
    nuevos_full = df_final[df_final['_merge'] == 'right_only'][['Predial_Nacional', 'Nombre', 'Zona', 'Avaluo_post']].rename(columns={'Avaluo_post': 'Sistema'}).to_dict(orient='records')
    desaparecidos_full = df_final[df_final['_merge'] == 'left_only'][['Predial_Nacional', 'Nombre', 'Zona', 'Avaluo_pre']].rename(columns={'Avaluo_pre': 'Base'}).to_dict(orient='records')

    # 6. Preparar Data Detallada
    cols = ['Predial_Nacional', 'Nombre', 'Destino', 'Zona', 'Municipio',
            'Avaluo_pre', 'Calculado', 'Avaluo_post', 
            'Estado', 'Pct_Teorico', 'Pct_Real', 'Diferencia']
            
    df_export = df_final[cols].rename(columns={
        'Avaluo_pre': 'Base',
        'Avaluo_post': 'Sistema'
    })
    
    # APLICAR MUESTREO AQUÍ (SOLO PARA LA VISTA DE TABLA)
    sample_pct_val = float(sample_pct)
    if sample_pct_val < 100:
        n_sample = int(len(df_export) * (sample_pct_val / 100))
        if n_sample < 1: n_sample = 1
        if n_sample < len(df_export):
             df_export = df_export.sample(n=n_sample)
    
    # Estadísticas de la MUESTRA (Para los KPIs del Header)
    stats = {
        'sample_pct': sample_pct,
        'zona_filter': zona_filter,
        'total_registros_muestra': int(len(df_export)),
        'total_registros_universo': int(len(df_final)),
        'ok': int((df_export['Estado'] == 'OK').sum()),
        'nuevos': int((df_export['Estado'] == 'NUEVO').sum()),
        'desaparecidos': int((df_export['Estado'] == 'DESAPARECIDO').sum()),
        'inconsistencias': int((df_export['Estado'] == 'INCONSISTENCIA').sum()),
        # Stats Avanzados (Siguen siendo del Universo o de la Muestra? El usuario no especificó para estos, 
        # pero usualmente stats financieras son del universo. Los mantendremos del universo pero los KPIs de conteo de la muestra.)
        'mean': float(mean_val) if not np.isnan(mean_val) else 0,
        'median': float(median_val) if not np.isnan(median_val) else 0,
        'mode': float(mode_val) if not np.isnan(mode_val) else 0,
        'std': float(std_val) if not np.isnan(std_val) else 0
    }

    records = df_export.to_dict(orient='records')

    # 7. OUTLIERS (Análisis Estadístico)
    # Definición: Usaremos IQR (Rango Intercuartílico) sobre 'Pct_Real'.
    # Q1 = p25, Q3 = p75
    # IQR = Q3 - Q1
    # Lower Bound = Q1 - 1.5 * IQR
    # Upper Bound = Q3 + 1.5 * IQR
    # Solo consideramos outliers aquellos que tengan Base > 0 (para evitar división por cero infinita afectando)
    
    outliers_list = []
    
    # Filtramos data con Base > 0 para análisis de outliers
    df_analysis = df_final[df_final['Avaluo_pre'] > 0].copy()
    
    if len(df_analysis) > 5: # Mínimo de datos para estadística robusta
        try:
            Q1 = df_analysis['Pct_Real'].quantile(0.25)
            Q3 = df_analysis['Pct_Real'].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Identificar outliers
            # Nos interesan principalmente los aumentos EXTREMOS (Upper Bound) o disminuciones extremas (Lower Bound)
            outliers_df = df_analysis[
                (df_analysis['Pct_Real'] < lower_bound) | 
                (df_analysis['Pct_Real'] > upper_bound)
            ]
            
            # Ordenar por magnitud de desviación (absoluta)
            outliers_df['deviation_mag'] = (outliers_df['Pct_Real'] - df_analysis['Pct_Real'].median()).abs()
            outliers_df = outliers_df.sort_values('deviation_mag', ascending=False).head(10) # Top 10 outliers
            
            # Seleccionar columnas
            cols_out = ['Predial_Nacional', 'Nombre', 'Zona', 'Avaluo_pre', 'Avaluo_post', 'Pct_Real']
            outliers_list = outliers_df[cols_out].rename(columns={
                'Avaluo_pre': 'Base', 
                'Avaluo_post': 'Sistema'
            }).to_dict(orient='records')
            
        except Exception as e:
            print(f"Error calculando outliers: {e}")
            outliers_list = []

    return {'stats': stats, 'ade_stats': [], 'data': records, 'outliers': outliers_list, 'nuevos_full': nuevos_full, 'desaparecidos_full': desaparecidos_full}