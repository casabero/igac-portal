import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Configuración de cortes Registro 1
CORTES_R1 = [0, 2, 5, 30, 31, 34, 37, 137, 138, 139, 151, 251, 252, 253, 268, 274, 289, 297, 312]
COLS_R1 = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno", "AreaConstruida", "Avaluo", "Vigencia", "NoPredialAnterior", "Espacio_Final"]

def generar_colspecs(cortes):
    colspecs = []
    for i in range(len(cortes) - 1):
        colspecs.append((cortes[i], cortes[i+1]))
    colspecs.append((cortes[-1], None))
    return colspecs

def redondear_excel(valor):
    if pd.isna(valor) or valor == 0: return 0
    d = Decimal(str(valor))
    return int(d.quantize(Decimal("1E3"), rounding=ROUND_HALF_UP))

def cargar_snc(stream):
    colspecs = generar_colspecs(CORTES_R1)
    # dtype=str es fundamental para no perder ceros
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    # Limpieza de espacios
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Limpieza numérica del Avalúo
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # --- CONSTRUCCIÓN RIGIDA DE LA LLAVE (30 DÍGITOS) ---
    # Depto (2) + Mpio (3) + Predio (25)
    # Usamos zfill para rellenar con ceros a la izquierda si hiciera falta
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.zfill(2) + 
        df['Municipio'].astype(str).str.zfill(3) + 
        df['NoPredial'].astype(str).str.zfill(25)
    )
    
    # Eliminamos duplicados exactos de llave para no ensuciar la data
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar Base Maestra (Precierre)
    df_pre = cargar_snc(file_pre)
    
    # 2. Cargar Referencia (Postcierre)
    df_post = cargar_snc(file_post)
    # Solo nos interesa la llave y el avaluo
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # 3. EMPAREJAMIENTO (LEFT JOIN)
    # "Traeme todo lo de Precierre, y si encuentras el precio en Postcierre, pónmelo al lado"
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    
    # Si no encontró el avalúo en Postcierre, ponemos 0 (pero el registro EXISTE)
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(0)

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def aplicar_logica(row):
        avaluo_base = row['Avaluo']
        # Usamos el fragmento original de 25 dígitos para la zona
        # El SNC estándar tiene el sector en los primeros dígitos de la columna NoPredial
        fragmento = str(row['NoPredial']).zfill(25)
        
        # Lógica de Zona (00 = Rural)
        if fragmento.startswith('00'):
            factor = 1 + pct_rur_decimal
            pct_teorico = pct_rur_decimal
            zona = 'RURAL'
        else:
            factor = 1 + pct_urb_decimal
            pct_teorico = pct_urb_decimal
            zona = 'URBANO'
            
        calculado = redondear_excel(avaluo_base * factor)
        avaluo_post = row['Avaluo_Post']

        # Cálculo de diferencias
        diferencia = calculado - avaluo_post
        
        # Cálculo de % Sistema Real
        if avaluo_base > 0:
            pct_sistema = (avaluo_post / avaluo_base) - 1
        else:
            pct_sistema = 0

        # Estados simplificados
        if diferencia == 0: 
            estado = 'OK'
        elif avaluo_base == calculado and pct_teorico > 0: 
            estado = 'SIN_AUMENTO' # El redondeo se comió el aumento
        else: 
            estado = 'DIFERENCIA' # Hay discrepancia

        return calculado, zona, estado, diferencia, pct_teorico, pct_sistema

    # Aplicamos lógica
    res = df_final.apply(aplicar_logica, axis=1, result_type='expand')
    df_final[['Avaluo_Calc', 'Zona', 'Estado', 'Diferencia', 'Pct_Teorico', 'Pct_Sistema']] = res
    
    # Estadísticas Limpias
    stats = {
        'total_predios': int(len(df_pre)), # Universo total
        'total_post': int(len(df_post)),   # Referencia
        'diferencias': int((df_final['Estado'] == 'DIFERENCIA').sum())
    }

    # Datos Web
    cols = ['Predial_Nacional', 'Nombre', 'DestinoEconomico', 'Zona', 
            'Avaluo', 'Avaluo_Calc', 'Avaluo_Post', 
            'Estado', 'Pct_Teorico', 'Pct_Sistema', 'Diferencia']
            
    records = df_final[cols].to_dict(orient='records')

    return {'stats': stats, 'data': records}