import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Configuración de cortes para Registro 1 (Estandar IGAC)
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
    # Leemos todo como String para no perder ceros
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    # 1. Limpieza General
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # 2. Limpieza Avalúo
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # 3. Construcción Robustez de Llave (Predial Nacional)
    # Aseguramos que Dept sea 2 digitos, Mun 3 digitos, Predial 25 digitos
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.zfill(2) + 
        df['Municipio'].astype(str).str.zfill(3) + 
        df['NoPredial'].astype(str).str.zfill(25)
    )
    
    # Eliminar duplicados de llave
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # Cargar
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # Preparar Postcierre (Llave y Valor)
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # Cruce (Left Join)
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    
    # Marcar los no encontrados con -1 para distinguirlos de valor 0
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(-1)

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
def aplicar_logica(row):
        avaluo_base = row['Avaluo']
        fragmento = str(row['NoPredial']).zfill(25) 
        
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

        if avaluo_post == -1:
            estado = 'NO_CRUZA'
            diferencia = 0
            pct_sistema = 0
            avaluo_post_real = 0 
        else:
            avaluo_post_real = avaluo_post
            diferencia = calculado - avaluo_post_real
            
            if avaluo_base > 0:
                pct_sistema = (avaluo_post_real / avaluo_base) - 1
            else:
                pct_sistema = 0

            if diferencia == 0: estado = 'OK'
            elif avaluo_base == calculado: estado = 'SIN_AUMENTO'
            else: estado = 'INCONSISTENCIA'
            
        return calculado, zona, estado, diferencia, pct_teorico, pct_sistema, avaluo_post_real

    # Nota: Hacemos el calculo fila por fila
    # Optimización: Vectorización parcial
    # Pero mantenemos apply para legibilidad de la lógica de Zona
    res = df_final.apply(aplicar_logica, axis=1, result_type='expand')
    
    df_final[['Avaluo_Calc', 'Zona', 'Estado', 'Diferencia', 'Pct_Teorico', 'Pct_Sistema', 'Avaluo_Post_Final']] = res
    
    # Estadísticas
    stats = {
        'count_pre': int(len(df_pre)),
        'count_post': int(len(df_post)),
        'count_cruce': int((df_final['Avaluo_Post'] != -1).sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum())
    }

    # Datos para Web (Incluimos Nombre y Destino)
    cols = ['Predial_Nacional', 'Nombre', 'DestinoEconomico', 'Zona', 
            'Avaluo', 'Avaluo_Calc', 'Avaluo_Post_Final', 
            'Estado', 'Pct_Teorico', 'Pct_Sistema', 'Diferencia']
            
    # Renombrar para JSON
    df_export = df_final[cols].rename(columns={'Avaluo_Post_Final': 'Avaluo_Post'})
    records = df_export.to_dict(orient='records')

    return {'stats': stats, 'data': records}