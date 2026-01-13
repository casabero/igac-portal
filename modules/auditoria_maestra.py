import pandas as pd
import numpy as np
import io
from decimal import Decimal, ROUND_HALF_UP
from fpdf import FPDF

# ==========================================
# 1. LÓGICA DE NEGOCIO
# ==========================================

def calcular_avaluo_excel(valor_base, pct_incremento):
    """Redondeo idéntico a Excel: =REDONDEAR(numero * (1+pct); -3)"""
    if pd.isna(valor_base) or valor_base == 0:
        return 0
    # Usamos Decimal para precisión financiera exacta
    val = Decimal(str(valor_base))
    factor = Decimal("1") + (Decimal(str(pct_incremento)) / Decimal("100"))
    incrementado = val * factor
    # Redondeado a miles (1E3) hacia arriba desde .5 (ROUND_HALF_UP)
    final = incrementado.quantize(Decimal("1E3"), rounding=ROUND_HALF_UP)
    return int(final)

def obtener_zona(id_obj):
    """Obtiene la zona del código catastral (posiciones 5 y 6)"""
    try:
        s = str(id_obj).strip().replace('.0', '')
        # Si es corto, rellenar ceros a la izquierda hasta 30 si parece ser el predial
        if len(s) < 15: # Probablemente no es predial completo
            return 'Desconocida'
        
        # Posición 5 y 6 (índices 5:7)
        cod = s[5:7] 
        if cod == '00': return 'Rural'
        if cod == '01': return 'Urbana'
        if cod.isdigit(): return f'Corregimiento {cod}'
        return 'Desc.'
    except: return 'Error'

def procesar_auditoria(files_dict, pct_incremento):
    """Procesa los archivos subidos y genera la auditoría"""
    df_prop = None
    df_calc = None
    
    for filename, stream in files_dict.items():
        # Leer cabecera para detección
        temp = pd.read_excel(stream, nrows=5)
        stream.seek(0)
        cols = [str(c).lower().strip() for c in temp.columns]
        
        # Detección Propietarios (R1)
        if 'departamento' in cols and 'municipio' in cols:
            df_prop = pd.read_excel(stream, dtype=str)
            df_prop['ID_Unico'] = df_prop['Departamento'].str.strip().str.zfill(2) + \
                                  df_prop['Municipio'].str.strip().str.zfill(3) + \
                                  df_prop['NoPredial'].str.strip().str.zfill(25)
            
            if 'Avaluo ($)' in df_prop.columns:
                df_prop['Valor_Base_R1'] = pd.to_numeric(df_prop['Avaluo ($)'], errors='coerce').fillna(0)
            else:
                # Buscar columna parecida
                av_col = [c for c in df_prop.columns if 'avaluo' in c.lower()]
                if av_col:
                    df_prop['Valor_Base_R1'] = pd.to_numeric(df_prop[av_col[0]], errors='coerce').fillna(0)
            
            df_prop = df_prop.drop_duplicates(subset=['ID_Unico'], keep='first')
            df_prop['Zona'] = df_prop['ID_Unico'].apply(obtener_zona)

        # Detección Listado Avalúos
        elif any(k in ' '.join(cols) for k in ['valor avaluo', 'valor_calculado']):
            df_calc = pd.read_excel(stream, dtype=str)
            df_calc.columns = [c.strip() for c in df_calc.columns]
            
            mapa = {
                'Número predial': 'ID_Unico',
                'Valor avaluo precierre': 'Valor_Base_Listado',
                'Valor avaluo cierre': 'Valor_Cierre_Listado'
            }
            # Caso especial si las columnas vienen exactas del script
            df_calc.rename(columns=mapa, inplace=True)
            
            # Si no encontró ID_Unico, buscar por No Predial
            if 'ID_Unico' not in df_calc.columns:
                pred_col = [c for c in df_calc.columns if 'predial' in c.lower()]
                if pred_col: df_calc.rename(columns={pred_col[0]: 'ID_Unico'}, inplace=True)
            
            df_calc['ID_Unico'] = df_calc['ID_Unico'].str.strip().str.replace('.0', '', regex=False).str.zfill(30)
            df_calc['Valor_Base_Listado'] = pd.to_numeric(df_calc.get('Valor_Base_Listado', 0), errors='coerce').fillna(0)
            df_calc['Valor_Cierre_Listado'] = pd.to_numeric(df_calc.get('Valor_Cierre_Listado', 0), errors='coerce').fillna(0)
            df_calc['Zona'] = df_calc['ID_Unico'].apply(obtener_zona)

    if df_prop is None or df_calc is None:
        raise ValueError("Se requieren ambos archivos (Propietarios y Listado de Avalúos) para la auditoría.")

    # 1. Estadísticas de Zona
    stats_r1 = df_prop['Zona'].value_counts().rename('R1')
    stats_calc = df_calc['Zona'].value_counts().rename('Listado')
    tabla_zonas = pd.concat([stats_r1, stats_calc], axis=1).fillna(0).astype(int)
    tabla_zonas['Dif'] = tabla_zonas['R1'] - tabla_zonas['Listado']
    
    # 2. Cruce y Auditoría
    full = pd.merge(
        df_prop[['ID_Unico', 'Valor_Base_R1', 'Zona']],
        df_calc[['ID_Unico', 'Valor_Base_Listado', 'Valor_Cierre_Listado']],
        on='ID_Unico',
        how='outer',
        indicator=True
    )
    
    full[['Valor_Base_R1', 'Valor_Base_Listado', 'Valor_Cierre_Listado']] = \
        full[['Valor_Base_R1', 'Valor_Base_Listado', 'Valor_Cierre_Listado']].fillna(0)

    full['Base_Usada'] = np.where(full['Valor_Base_R1'] > 0, full['Valor_Base_R1'], full['Valor_Base_Listado'])
    full['Diff_Base'] = full['Valor_Base_R1'] - full['Valor_Base_Listado']
    full['Cierre_Calculado'] = full['Base_Usada'].apply(lambda x: calcular_avaluo_excel(x, pct_incremento))
    full['Diff_Calculo'] = full['Valor_Cierre_Listado'] - full['Cierre_Calculado']
    
    # Clasificación de errores
    full['Estado'] = 'OK'
    full.loc[full['_merge'] == 'left_only', 'Estado'] = 'Faltante en Listado'
    full.loc[full['_merge'] == 'right_only', 'Estado'] = 'Sobran en Listado'
    full.loc[(full['_merge'] == 'both') & (full['Diff_Base'] != 0), 'Estado'] = 'Base Diferente'
    full.loc[(full['_merge'] == 'both') & (full['Diff_Calculo'] != 0), 'Estado'] = 'Error de Cálculo'

    # Preparar resultados para la UI
    resumen_estados = full['Estado'].value_counts().to_dict()
    
    # Top de inconsistencias para mostrar
    inconsistencias = full[full['Estado'] != 'OK'].head(100).to_dict(orient='records')
    
    return {
        'stats_zonas': tabla_zonas.reset_index().to_dict(orient='records'),
        'resumen_estados': resumen_estados,
        'inconsistencias': inconsistencias,
        'total_predios': len(full),
        'full_df': full, # Para el PDF
        'pct_incremento': pct_incremento
    }

class AuditoriaPDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'REPORTE DE AUDITORÍA CATASTRAL - CASABERO', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

def generar_pdf_auditoria(resultados):
    pdf = AuditoriaPDF()
    pdf.add_page()
    pdf.set_font('Arial', '', 10)
    
    # Resumen General
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Resumen de Auditoría', 0, 1)
    pdf.set_font('Arial', '', 10)
    pdf.cell(0, 7, f"Total Predios Auditados: {resultados['total_predios']}", 0, 1)
    pdf.cell(0, 7, f"Incremento Configurado: {resultados['pct_incremento']}%", 0, 1)
    pdf.ln(5)
    
    # Tabla de Estados
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(60, 7, 'Estado', 1)
    pdf.cell(30, 7, 'Cantidad', 1)
    pdf.ln()
    pdf.set_font('Arial', '', 10)
    for estado, cant in resultados['resumen_estados'].items():
        pdf.cell(60, 7, str(estado), 1)
        pdf.cell(30, 7, str(cant), 1)
        pdf.ln()
    
    pdf.ln(10)
    
    # Tabla de Zonas
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, 'Distribución por Zona', 0, 1)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(60, 7, 'Zona', 1)
    pdf.cell(30, 7, 'Cant. R1', 1)
    pdf.cell(30, 7, 'Cant. Listado', 1)
    pdf.cell(30, 7, 'Diferencia', 1)
    pdf.ln()
    pdf.set_font('Arial', '', 10)
    for row in resultados['stats_zonas']:
        pdf.cell(60, 7, str(row['Zona']), 1)
        pdf.cell(30, 7, str(row['R1']), 1)
        pdf.cell(30, 7, str(row['Listado']), 1)
        pdf.cell(30, 7, str(row['Dif']), 1)
        pdf.ln()

    # Si hay muchas inconsistencias, listar las primeras
    if resultados['inconsistencias']:
        pdf.add_page()
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Detalle de Inconsistencias (Primeras 50)', 0, 1)
        pdf.set_font('Arial', 'B', 7)
        pdf.cell(50, 7, 'ID Único', 1)
        pdf.cell(30, 7, 'Base R1', 1)
        pdf.cell(30, 7, 'Cierre Listado', 1)
        pdf.cell(30, 7, 'Calculado Py', 1)
        pdf.cell(50, 7, 'Estado', 1)
        pdf.ln()
        pdf.set_font('Arial', '', 6)
        for i, item in enumerate(resultados['inconsistencias'][:50]):
            pdf.cell(50, 6, str(item['ID_Unico']), 1)
            pdf.cell(30, 6, f"{item['Valor_Base_R1']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Cierre_Calculado']:,.0f}", 1)
            pdf.cell(50, 6, str(item['Estado']), 1)
            pdf.ln()

    return pdf.output()
