import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Configuración de lectura SNC
CORTES_R1 = [0, 2, 5, 30, 31, 34, 37, 137, 138, 139, 151, 251, 252, 253, 268, 274, 289, 297, 312]
COLS_R1 = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno", "AreaConstruida", "Avaluo", "Vigencia", "NoPredialAnterior", "Espacio_Final"]

def generar_colspecs(cortes):
    colspecs = []
    for i in range(len(cortes) - 1):
        colspecs.append((cortes[i], cortes[i+1]))
    colspecs.append((cortes[-1], None))
    return colspecs

def redondear_excel(valor):
    """Redondeo aritmético estricto tipo Excel"""
    if pd.isna(valor) or valor == 0: return 0
    d = Decimal(str(valor))
    return int(d.quantize(Decimal("1E3"), rounding=ROUND_HALF_UP))

def cargar_snc(stream):
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
    
    # Construcción Llave 30 Dígitos (Vital para que el cruce funcione)
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.zfill(2) + 
        df['Municipio'].astype(str).str.zfill(3) + 
        df['NoPredial'].astype(str).str.zfill(25)
    )
    
    # 2. TU LÓGICA: Eliminar duplicados para tener 1 registro por predio
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # Preparamos las columnas necesarias del postcierre
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # 3. TU LÓGICA: OUTER JOIN con INDICATOR
    # Esto nos dice automáticamente si está en ambos, solo en izquierda o solo derecha
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='outer', indicator=True)
    
    # Rellenar NaNs con 0 para poder calcular
    df_final['Avaluo'] = df_final['Avaluo'].fillna(0)      # Avaluo Pre
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(0) # Avaluo Post

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def auditar_fila(row):
        merge_status = row['_merge']
        avaluo_base = row['Avaluo']
        avaluo_post = row['Avaluo_Post']
        predial = str(row['Predial_Nacional'])
        
        # Lógica de Zona (Posición 5-6 en cadena de 30)
        # Si es un predio nuevo (right_only), el predial viene del dataset derecho, igual funciona.
        if len(predial) >= 7 and predial[5:7] == '00':
            factor = 1 + pct_rur_decimal
            pct_teorico = pct_rur_decimal
            zona = 'RURAL'
        else:
            factor = 1 + pct_urb_decimal
            pct_teorico = pct_urb_decimal
            zona = 'URBANO'

        # --- ANÁLISIS DE CASOS ---
        
        # CASO 1: DESAPARECIDO (Estaba en Pre, no en Post)
        if merge_status == 'left_only':
            return 0, zona, 'DESAPARECIDO', 0, 0, 0

        # CASO 2: NUEVO (No estaba en Pre, apareció en Post)
        if merge_status == 'right_only':
            return 0, zona, 'NUEVO_PREDIO', 0, 0, 0

        # CASO 3: CRUCE (Both) - Aquí auditamos matemáticas
        calculado = redondear_excel(avaluo_base * factor)
        diferencia = calculado - avaluo_post
        
        pct_sistema = 0
        if avaluo_base > 0:
            pct_sistema = (avaluo_post / avaluo_base) - 1
            
        if diferencia == 0:
            estado = 'OK'
        elif avaluo_base == calculado:
            estado = 'SIN_AUMENTO' # Redondeo lo dejó igual
        else:
            estado = 'INCONSISTENCIA'
            
        return calculado, zona, estado, diferencia, pct_teorico, pct_sistema

    # Aplicar auditoría
    res = df_final.apply(auditar_fila, axis=1, result_type='expand')
    df_final[['Avaluo_Calc', 'Zona', 'Estado', 'Diferencia', 'Pct_Teorico', 'Pct_Sistema']] = res
    
    # 6. Estadísticas para Dashboard
    stats = {
        'total_analizados': int(len(df_final)),
        'nuevos': int((df_final['Estado'] == 'NUEVO_PREDIO').sum()),
        'desaparecidos': int((df_final['Estado'] == 'DESAPARECIDO').sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum()),
        'ok': int((df_final['Estado'] == 'OK').sum())
    }

    # Datos Web
    # Llenamos Nombres vacíos para evitar error en JS
    df_final['Nombre'] = df_final['Nombre'].fillna('SIN DATOS')
    df_final['DestinoEconomico'] = df_final['DestinoEconomico'].fillna('-')
    
    cols = ['Predial_Nacional', 'Nombre', 'DestinoEconomico', 'Zona', 
            'Avaluo', 'Avaluo_Calc', 'Avaluo_Post', 
            'Estado', 'Pct_Teorico', 'Pct_Sistema', 'Diferencia']
            
    records = df_final[cols].to_dict(orient='records')

    return {'stats': stats, 'data': records}