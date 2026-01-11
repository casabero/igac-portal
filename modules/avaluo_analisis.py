import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

# Configuración de cortes Registro 1 (SNC)
CORTES_R1 = [0, 2, 5, 30, 31, 34, 37, 137, 138, 139, 151, 251, 252, 253, 268, 274, 289, 297, 312]
COLS_R1 = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno", "AreaConstruida", "Avaluo", "Vigencia", "NoPredialAnterior", "Espacio_Final"]

def generar_colspecs(cortes):
    colspecs = []
    for i in range(len(cortes) - 1):
        colspecs.append((cortes[i], cortes[i+1]))
    colspecs.append((cortes[-1], None))
    return colspecs

def redondear_excel(valor):
    """Redondeo aritmético estricto (como Excel)"""
    if pd.isna(valor) or valor == 0: return 0
    d = Decimal(str(valor))
    return int(d.quantize(Decimal("1E3"), rounding=ROUND_HALF_UP))

def cargar_snc(stream):
    """Carga y limpieza inicial del archivo plano"""
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
    
    # CONSTRUCCIÓN DE LLAVE 30 DÍGITOS (Rigurosa)
    df['Predial_Nacional'] = (
        df['Departamento'].astype(str).str.zfill(2) + 
        df['Municipio'].astype(str).str.zfill(3) + 
        df['NoPredial'].astype(str).str.zfill(25)
    )
    
    # Eliminar duplicados para tener relación 1 a 1 por predio
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    
    # Retornamos solo lo vital para el análisis
    return df[['Predial_Nacional', 'Avaluo', 'Nombre', 'DestinoEconomico']]

def procesar_incremento_web(file_pre, file_post, pct_urbano, pct_rural):
    # 1. Cargar DataFrames
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 2. UNIÓN TOTAL (OUTER JOIN con INDICATOR) - TU LÓGICA
    # Esto crea la columna '_merge' que nos dice: left_only, right_only, both
    df_final = pd.merge(
        df_pre, 
        df_post, 
        on='Predial_Nacional', 
        how='outer', 
        suffixes=('_pre', '_post'), 
        indicator=True
    )
    
    # 3. Limpieza post-cruce (Rellenar Nulos)
    df_final['Avaluo_pre'] = df_final['Avaluo_pre'].fillna(0)
    df_final['Avaluo_post'] = df_final['Avaluo_post'].fillna(0)
    
    # Fusionar Nombres y Destinos (Si es nuevo, toma el del post; si es viejo, el del pre)
    df_final['Nombre'] = df_final['Nombre_pre'].combine_first(df_final['Nombre_post']).fillna('SIN NOMBRE')
    df_final['Destino'] = df_final['DestinoEconomico_pre'].combine_first(df_final['DestinoEconomico_post']).fillna('-')

    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def auditar_registro(row):
        estado_cruce = row['_merge']
        avaluo_base = row['Avaluo_pre']
        avaluo_sistema = row['Avaluo_post']
        predial = str(row['Predial_Nacional'])
        
        # Detectar Zona (Posición 5 y 6 del string de 30 digitos)
        zona_code = predial[5:7] if len(predial) >= 7 else '00'
        
        if zona_code == '00':
            factor = 1 + pct_rur_decimal
            zona = 'RURAL'
            pct_teorico = pct_rur_decimal
        else:
            factor = 1 + pct_urb_decimal
            zona = 'URBANO'
            pct_teorico = pct_urb_decimal

        # --- LÓGICA DE ESTADOS ---
        calculado = 0
        diferencia = 0
        pct_real = 0
        
        if estado_cruce == 'left_only':
            # Estaba en Precierre, no en Postcierre
            estado = 'DESAPARECIDO'
            # Calculamos cuánto DEBERÍA haber sido
            calculado = redondear_excel(avaluo_base * factor)
            diferencia = 0 - calculado # Perdimos todo el valor
            
        elif estado_cruce == 'right_only':
            # No estaba, apareció nuevo
            estado = 'NUEVO'
            calculado = 0
            diferencia = avaluo_sistema # Ganancia neta
            
        else: # both (Existe en ambos)
            # Aplicamos incremento
            calculado = redondear_excel(avaluo_base * factor)
            diferencia = calculado - avaluo_sistema
            
            # % Real aplicado en el sistema
            if avaluo_base > 0:
                pct_real = (avaluo_sistema / avaluo_base) - 1
            
            if diferencia == 0:
                estado = 'OK'
            elif avaluo_base == calculado:
                estado = 'SIN_AUMENTO' # Redondeo lo anuló
            else:
                estado = 'INCONSISTENCIA'

        return zona, calculado, diferencia, estado, pct_teorico, pct_real

    # Aplicar lógica fila por fila
    res = df_final.apply(auditar_registro, axis=1, result_type='expand')
    df_final[['Zona', 'Avaluo_Calc', 'Diferencia', 'Estado', 'Pct_Teorico', 'Pct_Real']] = res
    
    # 4. Estadísticas
    stats = {
        'total_registros': int(len(df_final)),
        'ok': int((df_final['Estado'] == 'OK').sum()),
        'nuevos': int((df_final['Estado'] == 'NUEVO').sum()),
        'desaparecidos': int((df_final['Estado'] == 'DESAPARECIDO').sum()),
        'inconsistencias': int((df_final['Estado'] == 'INCONSISTENCIA').sum())
    }

    # 5. Preparar JSON para Web
    cols = ['Predial_Nacional', 'Nombre', 'Destino', 'Zona', 
            'Avaluo_pre', 'Avaluo_Calc', 'Avaluo_post', 
            'Estado', 'Pct_Teorico', 'Pct_Real', 'Diferencia']
            
    # Renombramos para que el JS lo entienda fácil
    df_export = df_final[cols].rename(columns={
        'Avaluo_pre': 'Base',
        'Avaluo_post': 'Sistema',
        'Avaluo_Calc': 'Calculado'
    })
    
    records = df_export.to_dict(orient='records')

    return {'stats': stats, 'data': records}