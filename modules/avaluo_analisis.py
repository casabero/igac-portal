import pandas as pd
import numpy as np


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
        df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
        if len(df.columns) == len(COLS_R1):
            df.columns = COLS_R1
        else:
             df.columns = COLS_R1[:len(df.columns)]
    
    # 2. Procesamiento Común
    # Limpieza
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Construcción Llave 30 Dígitos
    # Aseguramos que existan las columnas necesarias
    required_cols = ['Departamento', 'Municipio', 'NoPredial']
    if not all(col in df.columns for col in required_cols):
         # Intentar mapping si los nombres son diferentes en Excel?
         # Por ahora asumimos que si es Excel, tiene los encabezados O el orden correcto.
         pass 

    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(2) + 
        df['Municipio'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(3) + 
        df['NoPredial'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(25)
    )
    
    # Eliminar duplicados
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    
    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico']]

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural, sample_pct=100, zona_filter='TODOS'):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 1.1 FILTRO ZONAL (Optimización Previa)
    # Zona está en posiciones 5:7 del Predial Nacional (Depto 2 + Mun 3 + ZONA 2 ...)
    if zona_filter != 'TODOS':
        # Función auxiliar para filtrar
        def filtrar_por_zona(df, codigo_zona):
            # Extraer slice 5:7
            zonas = df['Predial_Nacional'].astype(str).str.slice(5, 7)
            if codigo_zona == 'URBANO':
                return df[zonas == '01']
            elif codigo_zona == 'RURAL':
                return df[zonas == '00']
            elif codigo_zona == 'CORREG':
                return df[~zonas.isin(['00', '01'])]
            return df

        df_pre = filtrar_por_zona(df_pre, zona_filter)
        df_post = filtrar_por_zona(df_post, zona_filter)

    # 1.2 MUESTREO POR PORCENTAJE
    sample_pct = float(sample_pct)
    is_sample = (sample_pct < 100)
    
    if is_sample:
        # Universo de llaves (Unión de ambos archivos)
        keys_pre = set(df_pre['Predial_Nacional'].unique())
        keys_post = set(df_post['Predial_Nacional'].unique())
        all_keys = list(keys_pre.union(keys_post))
        
        total_keys = len(all_keys)
        n_sample = int(total_keys * (sample_pct / 100))
        
        # Mínimo 1 registro si hay data
        if total_keys > 0 and n_sample == 0: 
            n_sample = 1
            
        if n_sample < total_keys:
            sample_keys = np.random.choice(all_keys, size=n_sample, replace=False)
            
            # Filtrar DataFrames
            df_pre = df_pre[df_pre['Predial_Nacional'].isin(sample_keys)]
            df_post = df_post[df_post['Predial_Nacional'].isin(sample_keys)]
    
    # 2. Unión Total (Outer Join) para detectar novedades
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
    
    # Calculo Estadísticos Avanzados (Sobre Avaluo Sistema)
    avaluos_sist = df_final['Avaluo_post']
    
    mean_val = avaluos_sist.mean()
    median_val = avaluos_sist.median()
    std_val = avaluos_sist.std()
    
    # Moda (puede haber múltiples, tomamos la primera o 0 si vacía)
    mode_series = avaluos_sist.mode()
    mode_val = mode_series.iloc[0] if not mode_series.empty else 0

    stats = {
        'sample_pct': sample_pct,
        'zona_filter': zona_filter,
        'total_registros': int(len(df_final)),
        'ok': int((df_final['Estado'] == 'OK').sum()),
        'nuevos': int((df_final['Estado'] == 'NUEVO').sum()),
        'desaparecidos': int((df_final['Estado'] == 'DESAPARECIDO').sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum()),
        # Advanced Stats
        'mean': float(mean_val) if not np.isnan(mean_val) else 0,
        'median': float(median_val) if not np.isnan(median_val) else 0,
        'mode': float(mode_val) if not np.isnan(mode_val) else 0,
        'std': float(std_val) if not np.isnan(std_val) else 0
    }

    # 5. Estadísticas ADE (ELIMINADO por solicitud, reemplazado por Stats Avanzados)
    ade_stats = [] # Se mantiene vacío para compatibilidad o se elimina uso en front

    # 6. Preparar Data Detallada
    cols = ['Predial_Nacional', 'Nombre', 'Destino', 'Zona', 
            'Avaluo_pre', 'Calculado', 'Avaluo_post', 
            'Estado', 'Pct_Teorico', 'Pct_Real', 'Diferencia']
            
    df_export = df_final[cols].rename(columns={
        'Avaluo_pre': 'Base',
        'Avaluo_post': 'Sistema'
    })
    
    records = df_export.to_dict(orient='records')

    return {'stats': stats, 'ade_stats': ade_stats, 'data': records}