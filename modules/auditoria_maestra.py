import pandas as pd
import numpy as np
import io
from decimal import Decimal, ROUND_HALF_UP
from fpdf import FPDF
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg') # Modo no interactivo para el servidor

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
                av_col = [c for c in df_prop.columns if 'avaluo' in c.lower()]
                if av_col:
                    df_prop['Valor_Base_R1'] = pd.to_numeric(df_prop[av_col[0]], errors='coerce').fillna(0)
            
            # Detectar nombre/código del municipio y departamento
            cols_lower = [c.lower() for c in df_prop.columns]
            
            # Buscar códigos específicos (estándar IGAC)
            cod_depto = ""
            cod_muni = ""
            
            for c in df_prop.columns:
                cl = c.lower()
                if 'depto' in cl or 'departamento' in cl or 'cod_dep' in cl:
                    cod_depto = str(df_prop[c].iloc[0]).strip().zfill(2)
                if 'muni' in cl or 'municipio' in cl or 'cod_mun' in cl:
                    # Evitar nombres, buscar códigos numéricos de 3 dígitos
                    val = str(df_prop[c].iloc[0]).strip()
                    if val.isdigit():
                        cod_muni = val.zfill(3)

            nombre_municipio = "Desconocido"
            if cod_depto and cod_muni:
                nombre_municipio = f"{cod_depto}{cod_muni}"
            else:
                muni_col = [c for c in df_prop.columns if 'nombre' in c.lower() and ('muni' in c.lower() or 'mpio' in c.lower())]
                if muni_col:
                    nombre_municipio = str(df_prop[muni_col[0]].iloc[0]).strip()

            df_prop = df_prop.drop_duplicates(subset=['ID_Unico'], keep='first')
            df_prop['Zona'] = df_prop['ID_Unico'].apply(obtener_zona)
            df_prop['Muni_Name'] = nombre_municipio

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
    
    # % de Variación Real (Cierre Listado vs Precierre)
    full['Pct_Variacion'] = np.where(full['Base_Usada'] > 0, 
                                     ((full['Valor_Cierre_Listado'] - full['Base_Usada']) / full['Base_Usada'] * 100), 
                                     0)
    
    # Clasificación de errores
    full['Estado'] = 'OK'
    full.loc[full['_merge'] == 'left_only', 'Estado'] = 'Faltante en Listado'
    full.loc[full['_merge'] == 'right_only', 'Estado'] = 'Sobran en Listado'
    full.loc[(full['_merge'] == 'both') & (full['Diff_Base'] != 0), 'Estado'] = 'Base Diferente'
    full.loc[(full['_merge'] == 'both') & (full['Diff_Calculo'] != 0), 'Estado'] = 'Error de Cálculo'

    # Renombrar para mayor claridad en el reporte y UI
    full.rename(columns={'ID_Unico': 'Numero_Predial'}, inplace=True)

    # Outliers (Top 5 y Bottom 5 de variaciones significativas)
    top_5_var = full[full['_merge'] == 'both'].sort_values(by='Pct_Variacion', ascending=False).head(5).to_dict(orient='records')
    bottom_5_var = full[full['_merge'] == 'both'].sort_values(by='Pct_Variacion', ascending=True).head(5).to_dict(orient='records')

    # Totales Globales
    totales = {
        'conteo': int(len(full)),
        'conteo_r1': int(len(df_prop)),
        'conteo_listado': int(len(df_calc)),
        'avaluo_precierre': float(full['Base_Usada'].sum()),
        'avaluo_cierre_listado': float(full['Valor_Cierre_Listado'].sum()),
        'avaluo_cierre_calculado': float(full['Cierre_Calculado'].sum())
    }
    
    inconsistencias = full[full['Estado'] != 'OK'].head(200).to_dict(orient='records')
    municipio_detectado = df_prop['Muni_Name'].iloc[0] if df_prop is not None and not df_prop.empty else "Desconocido"

    return {
        'municipio': municipio_detectado,
        'stats_zonas': tabla_zonas.reset_index().to_dict(orient='records'),
        'resumen_estados': full['Estado'].value_counts().to_dict(),
        'inconsistencias': inconsistencias,
        'total_predios': len(full),
        'totales': totales,
        'outliers': {'top': top_5_var, 'bottom': bottom_5_var},
        'variaciones_all': full[full['_merge'] == 'both']['Pct_Variacion'].tolist(), # Para el BoxPlot
        'full_data': full.to_dict(orient='records'),
        'pct_incremento': pct_incremento
    }

class AuditoriaPDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 12)
        self.cell(0, 10, 'REPORTE DE AUDITORÍA CATASTRAL - CASABERO', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'C')

def generar_pdf_auditoria(resultados):
    pdf = AuditoriaPDF()
    pdf.add_page()
    pdf.set_font('Helvetica', '', 10)
    
    # Resumen General
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 10, f"Reporte de Auditoría: {resultados.get('municipio', 'Municipio Desconocido')}", 0, 1)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 7, f"Total Predios Auditados: {resultados['total_predios']}", 0, 1)
    pdf.cell(0, 7, f"Incremento Configurado: {resultados['pct_incremento']}%", 0, 1)
    
    # Totales monetarios en el PDF
    if 'totales' in resultados:
        pdf.ln(2)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, "Totales Financieros:", 0, 1)
        pdf.set_font('Helvetica', '', 9)
        t = resultados['totales']
        pdf.cell(80, 6, f"Precierre (Avalúo Anterior) Total: $ {t['avaluo_precierre']:,.0f}", 0, 1)
        pdf.cell(80, 6, f"Cierre en Lista de Avalúo Total: $ {t['avaluo_cierre_listado']:,.0f}", 0, 1)
        pdf.cell(80, 6, f"Cierre Calculado (Python) Total: $ {t['avaluo_cierre_calculado']:,.0f}", 0, 1)
        pdf.set_font('Helvetica', 'B', 9)
        dif_global = t['avaluo_cierre_listado'] - t['avaluo_precierre']
        pdf.cell(80, 6, f"Diferencia Real (Cierre - Precierre): $ {dif_global:,.0f}", 0, 1)
    
    # Gráfico de Variación (Box Plot)
    if 'variaciones_all' in resultados and resultados['variaciones_all']:
        try:
            plt.figure(figsize=(6, 3))
            plt.boxplot(resultados['variaciones_all'], vert=False, patch_artist=True,
                        boxprops=dict(facecolor='#EEF2FF', color='#4F46E5'),
                        medianprops=dict(color='#EF4444'))
            plt.title('Distribución de % Variación (Cierre vs Precierre)', fontsize=10)
            plt.xlabel('% Variación', fontsize=8)
            plt.grid(axis='x', linestyle='--', alpha=0.7)
            plt.tight_layout()
            
            img_buf = io.BytesIO()
            plt.savefig(img_buf, format='png', dpi=150)
            plt.close()
            img_buf.seek(0)
            
            pdf.ln(5)
            # Centrar imagen
            pdf.image(img_buf, x=35, w=140)
            pdf.ln(5)
        except Exception as e:
            print(f"Error generando gráfico: {e}")

    pdf.ln(5)
    
    # Outliers: Top/Bottom 5 variaciones
    if 'outliers' in resultados:
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 10, 'Análisis de Outliers (% de Variación)', 0, 1)
        
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, 'Top 5 Mayores Incrementos %:', 0, 1)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.cell(50, 7, 'Número Predial', 1)
        pdf.cell(30, 7, 'Precierre', 1)
        pdf.cell(30, 7, 'Cierre Lista', 1)
        pdf.cell(30, 7, '% Var.', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 7)
        for item in resultados['outliers']['top']:
            pdf.cell(50, 6, str(item['Numero_Predial']), 1)
            pdf.cell(30, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.ln()

        pdf.ln(5)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, 'Top 5 Menores Incrementos %:', 0, 1)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.cell(50, 7, 'Número Predial', 1)
        pdf.cell(30, 7, 'Precierre', 1)
        pdf.cell(30, 7, 'Cierre Lista', 1)
        pdf.cell(30, 7, '% Var.', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 7)
        for item in resultados['outliers']['bottom']:
            pdf.cell(50, 6, str(item['Numero_Predial']), 1)
            pdf.cell(30, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.ln()

    # Tabla de Zonas (en nueva página si es necesario)
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 10, 'Distribución por Zona', 0, 1)
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(60, 7, 'Zona', 1)
    pdf.cell(30, 7, 'Cant. R1', 1)
    pdf.cell(30, 7, 'Cant. Listado', 1)
    pdf.cell(30, 7, 'Diferencia', 1)
    pdf.ln()
    pdf.set_font('Helvetica', '', 10)
    for row in resultados['stats_zonas']:
        pdf.cell(60, 7, str(row['Zona']), 1)
        pdf.cell(30, 7, str(row['R1']), 1)
        pdf.cell(30, 7, str(row['Listado']), 1)
        pdf.cell(30, 7, str(row['Dif']), 1)
        pdf.ln()

    # Si hay muchas inconsistencias, listar las primeras
    if resultados['inconsistencias']:
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 10, 'Detalle de Inconsistencias (Primeras 50)', 0, 1)
        pdf.set_font('Helvetica', 'B', 7)
        pdf.cell(40, 7, 'Número Predial', 1)
        pdf.cell(25, 7, 'Precierre', 1)
        pdf.cell(25, 7, 'Cierre Lista', 1)
        pdf.cell(25, 7, 'Cierre Calc.', 1)
        pdf.cell(25, 7, '% Var.', 1)
        pdf.cell(50, 7, 'Estado', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 6)
        for i, item in enumerate(resultados['inconsistencias'][:50]):
            pdf.cell(40, 6, str(item['Numero_Predial']), 1)
            pdf.cell(25, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(25, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(25, 6, f"{item['Cierre_Calculado']:,.0f}", 1)
            pdf.cell(25, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.cell(50, 6, str(item['Estado']), 1)
            pdf.ln()

    return bytes(pdf.output())
