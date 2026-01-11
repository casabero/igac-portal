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



def cargar_snc(stream):
    """Carga, limpieza y generación de llave única de 30 dígitos"""
    colspecs = generar_colspecs(CORTES_R1)
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    # Limpieza
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Construcción Llave 30 Dígitos (Depto 2 + Mun 3 + Predial 25)
    # .str.replace('.0', '') previene errores si pandas lee "70" como "70.0"
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(2) + 
        df['Municipio'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(3) + 
        df['NoPredial'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(25)
    )
    
    # Eliminar duplicados para tener 1 registro por predio
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    
    # Retornar columnas clave
    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico']]

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural, sample_mode=False):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 1.1 MUESTREO ALEATORIO (Si es solicitado)
    if sample_mode:
        # Universo de llaves (Unión de ambos archivos)
        keys_pre = set(df_pre['Predial_Nacional'].unique())
        keys_post = set(df_post['Predial_Nacional'].unique())
        all_keys = list(keys_pre.union(keys_post))
        
        # Seleccionar muestra (ej: 5000)
        # Usamos np.random.choice para eficiencia
        N_SAMPLE = 5000
        if len(all_keys) > N_SAMPLE:
            sample_keys = np.random.choice(all_keys, size=N_SAMPLE, replace=False)
            
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
    stats = {
        'sample_mode': sample_mode,
        'total_registros': int(len(df_final)),
        'ok': int((df_final['Estado'] == 'OK').sum()),
        'nuevos': int((df_final['Estado'] == 'NUEVO').sum()),
        'desaparecidos': int((df_final['Estado'] == 'DESAPARECIDO').sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum())
    }

    # 5. Estadísticas ADE (Resumen por Destino Económico)
    # Agrupamos por Destino y sumamos avalúo sistema (actual) y contamos predios
    ade_group = df_final.groupby('Destino')['Avaluo_post'].agg(['count', 'sum']).reset_index()
    ade_group.columns = ['Destino', 'Cantidad', 'Total_Avaluo']
    ade_stats = ade_group.to_dict(orient='records')

    # 6. Preparar Data Detallada
    cols = ['Predial_Nacional', 'Nombre', 'Destino', 'Zona', 
            'Avaluo_pre', 'Avaluo_Calc', 'Avaluo_post', 
            'Estado', 'Pct_Teorico', 'Pct_Real', 'Diferencia']
            
    df_export = df_final[cols].rename(columns={
        'Avaluo_pre': 'Base',
        'Avaluo_post': 'Sistema',
        'Avaluo_Calc': 'Calculado'
    })
    
    records = df_export.to_dict(orient='records')

    return {'stats': stats, 'ade_stats': ade_stats, 'data': records}