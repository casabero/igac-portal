import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Configuración de cortes para Registro 1
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
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    # Limpiamos caracteres de moneda si existen y convertimos
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Crear Predial Nacional (30 dígitos)
    df['Predial_Nacional'] = df['Departamento'] + df['Municipio'] + df['NoPredial']
    
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 2. Preparar Postcierre
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # 3. Cruce
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(0)

    # 4. Cálculo
    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def aplicar_logica(row):
        avaluo_base = row['Avaluo']
        fragmento = str(row['NoPredial'])
        
        # Lógica de Zona (00 = Rural)
        if fragmento.startswith('00'):
            factor = 1 + pct_rur_decimal
            zona = 'RURAL'
        else:
            factor = 1 + pct_urb_decimal
            zona = 'URBANO'
            
        calculado = redondear_excel(avaluo_base * factor)
        
        diferencia = calculado - row['Avaluo_Post']
        
        estado = 'OK'
        if row['Avaluo_Post'] == 0: estado = 'NO_CRUZA'
        elif diferencia != 0: estado = 'INCONSISTENCIA'
        elif avaluo_base == calculado: estado = 'SIN_AUMENTO'

        return calculado, zona, estado, diferencia

    res = df_final.apply(aplicar_logica, axis=1, result_type='expand')
    df_final['Avaluo_Calc'] = res[0]
    df_final['Zona'] = res[1]
    df_final['Estado'] = res[2]
    df_final['Diferencia'] = res[3]

    # 5. Generar Estructura para la Web (JSON)
    # Convertimos a lista de diccionarios para enviarlo a Javascript
    
    # Estadísticas Globales
    stats = {
        'total_predios': int(len(df_final)),
        'total_avaluo_pre': float(df_final['Avaluo'].sum()),
        'total_avaluo_post': float(df_final['Avaluo_Post'].sum()),
        'total_avaluo_calc': float(df_final['Avaluo_Calc'].sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum())
    }

    # Datos Detallados (Solo columnas necesarias para no saturar el navegador)
    cols = ['Predial_Nacional', 'Direccion', 'Zona', 'Avaluo', 'Avaluo_Calc', 'Avaluo_Post', 'Estado', 'Diferencia']
    records = df_final[cols].to_dict(orient='records')

    return {'stats': stats, 'data': records}