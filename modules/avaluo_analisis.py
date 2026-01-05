import pandas as pd
import io
import os
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
    """Replica =REDONDEAR(valor, -3)"""
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
    df['Avaluo'] = pd.to_numeric(df['Avaluo'], errors='coerce').fillna(0)
    
    # Concatenar para crear el Predial Nacional Completo (30 dígitos)
    # Dept(2) + Mun(3) + Resto(25)
    df['Predial_Nacional'] = df['Departamento'] + df['Municipio'] + df['NoPredial']
    
    # Eliminar duplicados usando el Predial Completo
    df = df.drop_duplicates(subset=['Predial_Nacional'], keep='first')
    return df

def procesar_incremento(file_pre, file_post, pct_urbano, pct_rural, filename_base):
    # 1. Cargar Datos
    df_pre = cargar_snc(file_pre)
    df_post = cargar_snc(file_post)
    
    # 2. Preparar Postcierre (Usamos el Predial Nacional como llave)
    df_post_subset = df_post[['Predial_Nacional', 'Avaluo']].rename(columns={'Avaluo': 'Avaluo_Sistema_Post'})
    
    # 3. Cruce
    df_final = pd.merge(df_pre, df_post_subset, on='Predial_Nacional', how='left')
    
    # 4. Cálculo
    pct_urb_decimal = float(pct_urbano) / 100
    pct_rur_decimal = float(pct_rural) / 100
    
    def aplicar_regla(row):
        avaluo_base = row['Avaluo']
        # En el archivo plano, la columna 'NoPredial' (extracted) comienza justo después del municipio.
        # Por tanto, sus primeros 2 dígitos son la ZONA.
        fragmento_predial = str(row['NoPredial']) 
        
        # Detector de Zona
        if fragmento_predial.startswith('00'):
            factor = 1 + pct_rur_decimal
            tipo = 'RURAL'
            pct_aplicado = pct_rur_decimal
        else:
            factor = 1 + pct_urb_decimal
            tipo = 'URBANO'
            pct_aplicado = pct_urb_decimal
            
        calculado_bruto = avaluo_base * factor
        redondeado = redondear_excel(calculado_bruto)
        
        # Calcular % Efectivo (La prueba real de cuánto subió)
        if avaluo_base > 0:
            pct_efectivo = (redondeado / avaluo_base) - 1
        else:
            pct_efectivo = 0

        return redondeado, tipo, pct_aplicado, pct_efectivo

    resultados = df_final.apply(aplicar_regla, axis=1, result_type='expand')
    df_final['Avaluo_Calculado_Py'] = resultados[0]
    df_final['Tipo_Zona'] = resultados[1]
    df_final['Pct_Teorico'] = resultados[2]
    df_final['Pct_Efectivo_Real'] = resultados[3]
    
    # 5. Estados
    df_final['Avaluo_Sistema_Post'] = df_final['Avaluo_Sistema_Post'].fillna(0)
    df_final['Diferencia'] = df_final['Avaluo_Calculado_Py'] - df_final['Avaluo_Sistema_Post']
    
    def determinar_estado(row):
        if row['Avaluo_Sistema_Post'] == 0: return 'NO_CRUZA_POST'
        if row['Avaluo'] == row['Avaluo_Calculado_Py'] and row['Pct_Teorico'] > 0: return 'ALERTA_SIN_INCREMENTO'
        if row['Diferencia'] == 0: return 'OK'
        return 'INCONSISTENCIA'

    df_final['Estado'] = df_final.apply(determinar_estado, axis=1)

    # 6. Estadísticas
    stats = {
        'Predios Precierre': len(df_pre),
        'Predios Postcierre': len(df_post),
        'Predios Cruzados': len(df_final) - (df_final['Estado'] == 'NO_CRUZA_POST').sum(),
        'Avaluo Precierre': df_pre['Avaluo'].sum(),
        'Avaluo Calculado': df_final['Avaluo_Calculado_Py'].sum(),
        'Avaluo Postcierre': df_post['Avaluo'].sum(),
        'Inconsistencias': (df_final['Estado'] == 'INCONSISTENCIA').sum()
    }
    df_stats = pd.DataFrame(list(stats.items()), columns=['Métrica', 'Valor'])

    # 7. Exportación
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_stats.to_excel(writer, sheet_name='Resumen', index=False)
        
        # Usamos Predial_Nacional (30 dígitos) como primera columna
        cols_export = ['Predial_Nacional', 'Tipo_Zona', 'Direccion', 'Avaluo', 'Pct_Teorico', 
                       'Avaluo_Calculado_Py', 'Pct_Efectivo_Real', 'Avaluo_Sistema_Post', 'Estado']
        
        df_final[cols_export].to_excel(writer, sheet_name='Detalle', index=False)
        
        # Formatos
        wb = writer.book
        ws = writer.sheets['Detalle']
        fmt_money = wb.add_format({'num_format': '$ #,##0'})
        fmt_pct = wb.add_format({'num_format': '0.00%'})
        fmt_red = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        fmt_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        
        ws.set_column('A:A', 32) # Predial 30 digitos ancho
        ws.set_column('D:D', 15, fmt_money)
        ws.set_column('F:F', 15, fmt_money)
        ws.set_column('H:H', 15, fmt_money)
        ws.set_column('E:E', 10, fmt_pct)
        ws.set_column('G:G', 10, fmt_pct)
        
        ws.conditional_format('I2:I1048576', {'type': 'text', 'criteria': 'containing', 'value': 'INCONSISTENCIA', 'format': fmt_red})
        ws.conditional_format('I2:I1048576', {'type': 'text', 'criteria': 'containing', 'value': 'ALERTA', 'format': fmt_yellow})

    output.seek(0)
    return output, f"Analisis_Avaluos_{filename_base}.xlsx"