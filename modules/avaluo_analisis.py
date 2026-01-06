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
    df = pd.read_fwf(stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    if len(df.columns) == len(COLS_R1):
        df.columns = COLS_R1
    else:
        df.columns = COLS_R1[:len(df.columns)]
    
    # Limpieza
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df['Avaluo'] = df['Avaluo'].astype(str).str.replace(r'[$,]', '', regex=True)
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # --- CONSTRUCCIÓN INTELIGENTE DE LA LLAVE (30 DÍGITOS) ---
    def construir_llave(row):
        d = str(row['Departamento']).zfill(2)
        m = str(row['Municipio']).zfill(3)
        p = str(row['NoPredial'])
        
        # Si el predial ya es largo (ej. 30 digitos), asumimos que ya tiene Dept+Mun
        if len(p) >= 25:
            # A veces el SNC trae el predial completo en la columna NoPredial
            # Verificamos si empieza con el Dept y Mun
            if p.startswith(d + m):
                return p # Ya está completo
            elif len(p) == 25:
                # Es el formato estándar de 25, le pegamos el encabezado
                return d + m + p
            else:
                # Caso raro, forzamos concatenación estándar
                return d + m + p.zfill(25)
        else:
            # Caso corto (ej. sector urbano), rellenamos a 25 y pegamos
            return d + m + p.zfill(25)

    df['Predial_Nacional'] = df.apply(construir_llave, axis=1)
    
    # Eliminar duplicados de llave (Solo registros únicos de Predial)
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar Precierre (Nuestra Base Maestra)
    df_pre = cargar_snc(file_pre)
    
    # 2. Cargar Postcierre (Solo para consultar valores)
    df_post = cargar_snc(file_post)
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Post'})
    
    # 3. EMPAREJAMIENTO (Left Join)
    # "Busca cada predio de Precierre dentro de Postcierre. Si no está, pon vacío."
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    
    # Si no encontró el predio en postcierre, ponemos -1 para control interno (luego mostramos $0)
    df_final['Avaluo_Post'] = df_final['Avaluo_Post'].fillna(-1)

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def aplicar_logica(row):
        avaluo_base = row['Avaluo']
        predial_30 = str(row['Predial_Nacional'])
        
        # Lógica de Zona usando el Predial Nacional de 30 dígitos
        # Estructura: Dept(2) Mun(3) Zona(2) ...
        # Posición 0-1: Dept
        # Posición 2-4: Mun
        # Posición 5-6: ZONA (Indices 5:7)
        
        zona_code = predial_30[5:7] 
        
        if zona_code == '00':
            factor = 1 + pct_rur_decimal
            pct_teorico = pct_rur_decimal
            zona = 'RURAL'
        else:
            factor = 1 + pct_urb_decimal
            pct_teorico = pct_urb_decimal
            zona = 'URBANO'
            
        calculado = redondear_excel(avaluo_base * factor)
        avaluo_post = row['Avaluo_Post']

        # Lógica de Estados
        if avaluo_post == -1:
            estado = 'NO_EN_POST' # Existe en precierre, no en post
            avaluo_post_real = 0
            diferencia = 0
            pct_sistema = 0
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

    # Aplicamos lógica
    res = df_final.apply(aplicar_logica, axis=1, result_type='expand')
    df_final[['Avaluo_Calc', 'Zona', 'Estado', 'Diferencia', 'Pct_Teorico', 'Pct_Sistema', 'Avaluo_Post_Final']] = res
    
    # Estadísticas para el Dashboard
    stats = {
        'total_predios_base': int(len(df_pre)), # Total precierre
        'total_predios_post': int(len(df_post)), # Total postcierre (referencia)
        'encontrados_post': int((df_final['Avaluo_Post'] != -1).sum()), # Cuántos cruzaron
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum())
    }

    # Datos Web
    cols = ['Predial_Nacional', 'Nombre', 'DestinoEconomico', 'Zona', 
            'Avaluo', 'Avaluo_Calc', 'Avaluo_Post_Final', 
            'Estado', 'Pct_Teorico', 'Pct_Sistema', 'Diferencia']
            
    df_export = df_final[cols].rename(columns={'Avaluo_Post_Final': 'Avaluo_Post'})
    records = df_export.to_dict(orient='records')

    return {'stats': stats, 'data': records}