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
    # dtype=str es VITAL para que no se pierdan ceros a la izquierda
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    # LIMPIEZA EXTREMA: Quitar espacios de TODAS las columnas de texto
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Limpieza de Avalúo
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Generar Llave Única (Concatenación Limpia)
    df['Predial_Nacional'] = df['Departamento'].astype(str) + df['Municipio'].astype(str) + df['NoPredial'].astype(str)
    
    # Eliminar duplicados de llave (por si acaso el archivo trae basura)
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 2. Preparar Postcierre (Solo llave y valor)
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # 3. Cruce (Left Join)
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    
    # Rellenar con 0 si no cruza, pero mantener registro para mostrar "NO CRUZA"
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(-1) # Usamos -1 para distinguir del valor 0 real

    # 4. Cálculo Matemático
    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def aplicar_logica(row):
        avaluo_base = row['Avaluo']
        predial_fragmento = str(row['NoPredial']) # Este es el fragmento de 25 digitos
        avaluo_post = row['Avaluo_Post']
        
        # Lógica Zona: Si empieza en 00 es Rural
        if predial_fragmento.startswith('00'):
            factor = 1 + pct_rur_decimal
            pct_teorico = pct_rur_decimal
            zona = 'RURAL'
        else:
            factor = 1 + pct_urb_decimal
            pct_teorico = pct_urb_decimal
            zona = 'URBANO'
            
        calculado = redondear_excel(avaluo_base * factor)
        
        # Calcular % Efectivo (Calculado vs Base)
        pct_calc_efectivo = 0
        if avaluo_base > 0:
            pct_calc_efectivo = (calculado / avaluo_base) - 1
            
        # Calcular % Sistema (Post vs Base)
        pct_sistema_real = 0
        if avaluo_post > 0 and avaluo_base > 0:
            pct_sistema_real = (avaluo_post / avaluo_base) - 1

        # Estados
        if avaluo_post == -1:
            estado = 'NO_CRUZA'
            diferencia = 0
        else:
            diferencia = calculado - avaluo_post
            if diferencia == 0: estado = 'OK'
            elif avaluo_base == calculado: estado = 'SIN_AUMENTO' # Redondeo lo dejó igual
            else: estado = 'INCONSISTENCIA'

        return calculado, zona, estado, diferencia, pct_teorico, pct_calc_efectivo, pct_sistema_real

    res = df_final.apply(aplicar_logica, axis=1, result_type='expand')
    df_final['Avaluo_Calc'] = res[0]
    df_final['Zona'] = res[1]
    df_final['Estado'] = res[2]
    df_final['Diferencia'] = res[3]
    df_final['Pct_Teorico'] = res[4]
    df_final['Pct_Calc'] = res[5]
    df_final['Pct_Sistema'] = res[6]
    
    # Corregir el -1 visual de Post
    df_final.loc[df_final['Avaluo_Post'] == -1, 'Avaluo_Post'] = 0

    # 5. Generar JSON para Web
    
    # Estadísticas Avanzadas
    stats = {
        'count_pre': int(len(df_pre)),
        'count_post': int(len(df_post)),
        'count_cruce': int((df_final['Estado'] != 'NO_CRUZA').sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum()),
        'total_avaluo_pre': float(df_final['Avaluo'].sum()),
        'total_avaluo_post': float(df_final['Avaluo_Post'].sum())
    }

    # Datos para la tabla (JSON ligero)
    cols = ['Predial_Nacional', 'Direccion', 'Zona', 'Avaluo', 'Avaluo_Calc', 'Avaluo_Post', 
            'Estado', 'Pct_Teorico', 'Pct_Calc', 'Pct_Sistema', 'Diferencia']
    records = df_final[cols].to_dict(orient='records')

    return {'stats': stats, 'data': records}