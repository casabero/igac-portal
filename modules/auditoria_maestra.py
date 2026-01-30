import pandas as pd
import numpy as np
import io
from decimal import Decimal, ROUND_HALF_UP
from fpdf import FPDF
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg') # Modo no interactivo para el servidor

# ==========================================
# 0. MAPEO DE MUNICIPIOS (SUCRE)
# ==========================================
MUNICIPIOS_SUCRE = {
    "70001": "Sincelejo (Capital)",
    "70110": "Buenavista",
    "70124": "Caimito",
    "70204": "Colosó",
    "70215": "Corozal",
    "70221": "Coveñas",
    "70230": "Chalán",
    "70233": "El Roble",
    "70235": "Galeras",
    "70265": "Guaranda",
    "70400": "La Unión",
    "70418": "Los Palmitos",
    "70429": "Majagual",
    "70473": "Morroa",
    "70508": "Ovejas",
    "70523": "Palmito (S. Antonio)",
    "70670": "Sampués",
    "70678": "San Benito Abad",
    "70702": "San Juan de Betulia",
    "70708": "San Marcos",
    "70713": "San Onofre",
    "70717": "San Pedro",
    "70742": "Sincé (San Luis de)",
    "70771": "Sucre",
    "70820": "Santiago de Tolú",
    "70823": "Tolúviejo"
}

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
        cod_str = s[5:7] 
        if not cod_str.isdigit():
            return 'Desc.'
            
        cod = int(cod_str)
        if cod == 0: return 'Rural'            # 00
        if cod == 1: return 'Urbana'           # 01
        if 2 <= cod <= 99: return f'Corregimiento {cod_str}'
        return 'Desc.'
    except: return 'Error'

def procesar_auditoria(files_dict, pct_incremento, zona_filtro='General'):
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
            
            # Fallback a buscar columna con nombre
            muni_col = [c for c in df_prop.columns if 'nombre' in c.lower() and ('muni' in c.lower() or 'mpio' in c.lower())]
            if muni_col:
                nombre_municipio = str(df_prop[muni_col[0]].iloc[0]).strip()
            
            # Si sigue siendo Desconocido, intentar extraer de ID_Unico (primeros 5)
            if nombre_municipio == "Desconocido" and not df_prop.empty:
                nombre_municipio = str(df_prop['ID_Unico'].iloc[0])[:5]
            
            # Buscar el nombre en el diccionario de Sucre si es un código DANE
            if nombre_municipio in MUNICIPIOS_SUCRE:
                nombre_municipio = f"{nombre_municipio} - {MUNICIPIOS_SUCRE[nombre_municipio]}"

            df_prop = df_prop.drop_duplicates(subset=['ID_Unico'], keep='first')
            df_prop['Zona'] = df_prop['ID_Unico'].apply(obtener_zona)
            df_prop['Muni_Name'] = nombre_municipio

        # Detección Listado Avalúos
        elif any(k in ' '.join(cols) for k in ['valor avaluo', 'valor_calculado']):
            df_calc = pd.read_excel(stream, dtype=str)
            df_calc.columns = [c.strip() for c in df_calc.columns]
            
            # Helper local para normalizar nombres de columnas (quitar acentos)
            def normalize_col(c):
                c = str(c).lower().strip()
                import unicodedata
                return "".join(c for c in unicodedata.normalize('NFD', c) if unicodedata.category(c) != 'Mn')

            # 1. Identificar columnas clave (Listado)
            cols_map = {
                'ID_Unico': next((c for c in df_calc.columns if 'identificador' in normalize_col(c) or 'numero predial' in normalize_col(c)), None),
                'Valor_Base_Listado': next((c for c in df_calc.columns if 'valor avaluo precierre' in normalize_col(c) or 'valor_base' in normalize_col(c)), None),
                'Valor_Cierre_Listado': next((c for c in df_calc.columns if 'valor avaluo cierre' in normalize_col(c) or 'valor_calculado' in normalize_col(c)), None),
                'Condicion_Propiedad': next((c for c in df_calc.columns if 'condicion propiedad' in normalize_col(c) or 'condicion_propiedad' in normalize_col(c)), None)
            }

            # 2. Renombrar y/o Inicializar Columnas
            for target, source in cols_map.items():
                if source and source in df_calc.columns:
                    df_calc.rename(columns={source: target}, inplace=True)
                elif target not in df_calc.columns:
                    # Si no existe, crear con valor por defecto
                    df_calc[target] = 0 if 'Valor' in target else (-1 if 'Condicion' in target else None)

            # 3. Fallback adicional para ID_Unico si no se detectó arriba (buscando solo "predial")
            if df_calc['ID_Unico'].isnull().all():
                pred_col = [c for c in df_calc.columns if 'predial' in normalize_col(c) and c != 'ID_Unico']
                if pred_col:
                    df_calc['ID_Unico'] = df_calc[pred_col[0]]

            # 4. Limpieza y Normalización
            df_calc['ID_Unico'] = df_calc['ID_Unico'].astype(str).str.strip().str.replace('.0', '', regex=False).str.zfill(30)
            df_calc['Valor_Base_Listado'] = pd.to_numeric(df_calc['Valor_Base_Listado'], errors='coerce').fillna(0)
            df_calc['Valor_Cierre_Listado'] = pd.to_numeric(df_calc['Valor_Cierre_Listado'], errors='coerce').fillna(0)
            df_calc['Condicion_Propiedad'] = pd.to_numeric(df_calc['Condicion_Propiedad'], errors='coerce').fillna(-1)
            
            df_calc['Zona'] = df_calc['ID_Unico'].apply(obtener_zona)

    if df_prop is None or df_calc is None:
        raise ValueError("Se requieren ambos archivos (Propietarios y Listado de Avalúos) para la auditoría.")

    # APLICAR FILTRO DE ZONA SI NO ES GENERAL
    if zona_filtro != 'General':
        if zona_filtro == 'Corregimientos':
            df_prop = df_prop[df_prop['Zona'].str.startswith('Corregimiento', na=False)].copy()
            df_calc = df_calc[df_calc['Zona'].str.startswith('Corregimiento', na=False)].copy()
        else:
            df_prop = df_prop[df_prop['Zona'] == zona_filtro].copy()
            df_calc = df_calc[df_calc['Zona'] == zona_filtro].copy()

    # 1. Estadísticas de Zona
    stats_r1 = df_prop['Zona'].value_counts().rename('R1')
    stats_calc = df_calc['Zona'].value_counts().rename('Listado')
    tabla_zonas = pd.concat([stats_r1, stats_calc], axis=1).fillna(0).astype(int)
    tabla_zonas['Dif'] = tabla_zonas['R1'] - tabla_zonas['Listado']
    
    # 2. Cruce y Auditoría
    full = pd.merge(
        df_prop[['ID_Unico', 'Valor_Base_R1', 'Zona']],
        df_calc[['ID_Unico', 'Valor_Base_Listado', 'Valor_Cierre_Listado', 'Condicion_Propiedad']],
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
    full.loc[(full['_merge'] == 'both') & (full['Diff_Base'] != 0), 'Estado'] = 'Avaluo Precierre Diferente'
    full.loc[(full['_merge'] == 'both') & (full['Diff_Calculo'] != 0), 'Estado'] = 'Error de Cálculo'

    # Identificar predios con avalúo $0 (no es normal)
    full['Avaluo_Zero'] = (full['Base_Usada'] == 0) | (full['Valor_Cierre_Listado'] == 0)
    
    # Categorización de Avalúos en $0
    full['Zero_Category'] = 'Ninguna'
    # Crítico: Avaluo 0 y Condicion 0
    full.loc[full['Avaluo_Zero'] & (full['Condicion_Propiedad'] == 0), 'Zero_Category'] = 'Crítico'
    # Informal: Avaluo 0 y Condicion 2
    full.loc[full['Avaluo_Zero'] & (full['Condicion_Propiedad'] == 2), 'Zero_Category'] = 'Informal'
    # PH: Avaluo 0 y Condicion 9
    full.loc[full['Avaluo_Zero'] & (full['Condicion_Propiedad'] == 9), 'Zero_Category'] = 'PH'
    # Otros ceros (ej: Condicion 1 o no encontrada)
    full.loc[full['Avaluo_Zero'] & (full['Zero_Category'] == 'Ninguna'), 'Zero_Category'] = 'Otros $0'

    predios_zero = full[full['Avaluo_Zero']].copy()
    
    # Renombrar para mayor claridad en el reporte y UI
    full.rename(columns={'ID_Unico': 'Numero_Predial'}, inplace=True)
    predios_zero.rename(columns={'ID_Unico': 'Numero_Predial'}, inplace=True)

    # Limpieza final de NaNs para evitar errores de serialización JSON (NaN -> null/0)
    for col in full.columns:
        # Si es categórico (como _merge), convertir a objeto primero
        if str(full[col].dtype) == 'category':
            full[col] = full[col].astype(object)
            
        if full[col].dtype == object:
            full[col] = full[col].fillna('')
        else:
            full[col] = full[col].fillna(0)
    
    # Asegurar que Numero_Predial sea siempre string para evitar fallos en JS (.includes)
    full['Numero_Predial'] = full['Numero_Predial'].astype(str)
    predios_zero['Numero_Predial'] = predios_zero['Numero_Predial'].astype(str).fillna('N/A')

    # Outliers (Top 5 y Bottom 5 de variaciones significativas)
    # Mostramos Numero_Predial, Pct_Variacion y Valor_Cierre_Listado
    top_5_var = full[full['_merge'] == 'both'].sort_values(by='Pct_Variacion', ascending=False).head(5)[['Numero_Predial', 'Pct_Variacion', 'Valor_Cierre_Listado', 'Base_Usada']].to_dict(orient='records')
    bottom_5_var = full[full['_merge'] == 'both'].sort_values(by='Pct_Variacion', ascending=True).head(5)[['Numero_Predial', 'Pct_Variacion', 'Valor_Cierre_Listado', 'Base_Usada']].to_dict(orient='records')

    # Totales Globales
    totales = {
        'conteo': int(len(full)),
        'conteo_r1': int(len(df_prop)),
        'conteo_listado': int(len(df_calc)),
        'avaluo_precierre': float(full['Base_Usada'].sum()),
        'avaluo_cierre_listado': float(full['Valor_Cierre_Listado'].sum()),
        'avaluo_cierre_calculado': float(full['Cierre_Calculado'].sum()),
        'conteo_zero_critico': int(len(full[full['Zero_Category'] == 'Crítico'])),
        'conteo_zero_informal': int(len(full[full['Zero_Category'] == 'Informal'])),
        'conteo_zero_ph': int(len(full[full['Zero_Category'] == 'PH'])),
        'conteo_zero_total': int(len(predios_zero))
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
        'predios_zero': predios_zero.head(100).to_dict(orient='records'),
        'variaciones_all': full[full['_merge'] == 'both']['Pct_Variacion'].tolist(), # Para el BoxPlot
        'full_data': full.to_dict(orient='records'),
        'pct_incremento': pct_incremento,
        'zona_filtro': zona_filtro
    }

class AuditoriaPDF(FPDF):
    def header(self):
        # Fondo oscuro para el encabezado (Civil-Hacker)
        self.set_fill_color(17, 24, 39) # #111827
        self.rect(0, 0, 216, 35, 'F') # Ajuste para Letter (216mm ancho)
        
        self.set_y(12)
        self.set_font('Helvetica', 'B', 16)
        self.set_text_color(255, 255, 255)
        self.cell(0, 10, 'REPORTE DE AUDITORÍA DE CIERRE - IGAC', 0, 1, 'C')
        
        self.set_font('Helvetica', '', 8)
        self.set_text_color(156, 163, 175) # Gris claro
        self.cell(0, 5, 'PLATAFORMA DE GESTIÓN CATASTRAL AVANZADA', 0, 1, 'C')
        self.ln(15)

    def footer(self):
        self.set_y(-20)
        self.set_draw_color(229, 231, 235)
        self.line(20, self.get_y(), 196, self.get_y())
        self.ln(2)
        
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(107, 114, 128)
        # Firma solicitada
        self.cell(0, 10, 'by casabero quien se hace llamar joseph.gari', 0, 0, 'L')
        self.cell(0, 10, f'Página {self.page_no()}', 0, 0, 'R')

def generar_pdf_auditoria(resultados):
    pdf = AuditoriaPDF()
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 7, 'Municipio:', 0)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 7, resultados.get('municipio', 'Desconocido'), 0, 1)
    
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 7, 'Zona Analizada:', 0)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 7, resultados.get('zona_filtro', 'General'), 0, 1)
    
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 7, 'Incremento:', 0)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 7, f"{resultados['pct_incremento']}%", 0, 1)

    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 7, 'Total Predios Auditados:', 0)
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(0, 7, f"{resultados['total_predios']}", 0, 1)
    
    # Totales monetarios en el PDF
    if 'totales' in resultados:
        pdf.ln(2)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, "Totales Financieros:", 0, 1)
        pdf.set_font('Helvetica', '', 9)
        t = resultados['totales']
        pdf.cell(100, 6, f"Avaluo Base Total: $ {t['avaluo_precierre']:,.0f}", 0, 1)
        pdf.cell(100, 6, f"Avaluo Final Total: $ {t['avaluo_cierre_listado']:,.0f}", 0, 1)
        pdf.cell(100, 6, f"Avaluo Calculado Total: $ {t['avaluo_cierre_calculado']:,.0f}", 0, 1)
        pdf.set_font('Helvetica', 'B', 9)
        dif_global = t['avaluo_cierre_listado'] - t['avaluo_precierre']
        pdf.cell(80, 6, f"Diferencia (Final - Base): $ {dif_global:,.0f}", 0, 1)
    
    # Gráfico de Variación (Box Plot)
    if 'variaciones_all' in resultados and resultados['variaciones_all']:
        try:
            plt.figure(figsize=(6, 3))
            plt.boxplot(resultados['variaciones_all'], vert=False, patch_artist=True,
                        boxprops=dict(facecolor='#EEF2FF', color='#4F46E5'),
                        medianprops=dict(color='#EF4444'))
            plt.title('Distribución de % Variación (Final vs Base)', fontsize=10)
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
        pdf.cell(35, 7, 'Avaluo Precierre', 1)
        pdf.cell(35, 7, 'Avaluo Cierre', 1)
        pdf.cell(30, 7, '% Var.', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 7)
        for item in resultados['outliers']['top']:
            pdf.cell(50, 6, str(item['Numero_Predial']), 1)
            pdf.cell(35, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(35, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.ln()

        pdf.ln(5)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.cell(0, 7, 'Top 5 Menores Incrementos %:', 0, 1)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.cell(50, 7, 'Número Predial', 1)
        pdf.cell(35, 7, 'Avaluo Precierre', 1)
        pdf.cell(35, 7, 'Avaluo Cierre', 1)
        pdf.cell(30, 7, '% Var.', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 7)
        for item in resultados['outliers']['bottom']:
            pdf.cell(50, 6, str(item['Numero_Predial']), 1)
            pdf.cell(35, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(35, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.ln()

    # Sección de Avalúos en $0 (Alerta)
    # ---------------------------------------------------------
    # SECCIÓN DE ALERTAS: PREDIO EN $0 (Siempre presente)
    # ---------------------------------------------------------
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 11)
    pdf.set_text_color(220, 38, 38) # Rojo
    pdf.cell(0, 10, 'ALERTA: PREDIOS CON AVALÚO EN $0 (Base o Cierre)', 0, 1)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font('Helvetica', '', 9)
    pdf.multi_cell(0, 5, 'Esta sección identifica los predios que presentan un valor de $0 pesos en el precierre (R1) o en el cierre del listado de avalúos. Es una inconsistencia que debe ser revisada.')
    pdf.ln(2)

    if not resultados['predios_zero']:
        pdf.set_font('Helvetica', 'I', 10)
        pdf.set_text_color(107, 114, 128)
        pdf.cell(0, 10, 'Sin hallazgos de avalúos en $0 para este municipio.', 0, 1, 'C')
        pdf.set_text_color(0, 0, 0)
    else:
        # Tabla de predios en cero
        pdf.set_font('Helvetica', 'B', 8)
        pdf.cell(50, 7, 'Número Predial', 1)
        pdf.cell(30, 7, 'Avaluo Precierre', 1)
        pdf.cell(30, 7, 'Avaluo Cierre', 1)
        pdf.cell(30, 7, 'Categoría', 1)
        pdf.cell(40, 7, 'Estado', 1)
        pdf.ln()
        pdf.set_font('Helvetica', '', 7)
        for item in resultados['predios_zero']:
            # Salto de página si es necesario
            if pdf.get_y() > 260:
                pdf.add_page()
                pdf.set_font('Helvetica', 'B', 8)
                pdf.cell(50, 7, 'Número Predial', 1)
                pdf.cell(30, 7, 'Avaluo Precierre', 1)
                pdf.cell(30, 7, 'Avaluo Cierre', 1)
                pdf.cell(30, 7, 'Categoría', 1)
                pdf.cell(40, 7, 'Estado', 1)
                pdf.ln()
                pdf.set_font('Helvetica', '', 7)

            pdf.cell(50, 6, str(item['Numero_Predial']), 1)
            pdf.cell(30, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(30, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            
            # Color por categoría
            cat = item.get('Zero_Category', 'Otros $0')
            if cat == 'Crítico':
                pdf.set_text_color(239, 68, 68) 
            elif cat == 'Informal':
                pdf.set_text_color(59, 130, 246)
            elif cat == 'PH':
                pdf.set_text_color(147, 51, 234) # Púrpura para PH
            
            pdf.cell(30, 6, str(cat), 1)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(40, 6, str(item['Estado']), 1)
            pdf.ln()
    pdf.set_text_color(0, 0, 0)

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

    # DETALLE DE INCONSISTENCIAS (TODAS)
    if resultados['inconsistencias']:
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 10, 'Detalle de Inconsistencias (Total)', 0, 1)
        
        # Función para imprimir cabecera de tabla de inconsistencias
        def print_table_header():
            pdf.set_font('Helvetica', 'B', 7)
            pdf.cell(45, 7, 'Número Predial', 1)
            pdf.cell(27, 7, 'Avaluo Precierre', 1)
            pdf.cell(27, 7, 'Avaluo Cierre', 1)
            pdf.cell(27, 7, 'Avaluo Cierre Calc.', 1)
            pdf.cell(18, 7, '% Var.', 1)
            pdf.cell(45, 7, 'Estado', 1)
            pdf.ln()

        print_table_header()
        pdf.set_font('Helvetica', '', 6)
        
        for item in resultados['inconsistencias']:
            # Control de salto de página manual para tablas largas
            if pdf.get_y() > 250:
                pdf.add_page()
                print_table_header()
                pdf.set_font('Helvetica', '', 6)

            pdf.cell(45, 6, str(item['Numero_Predial']), 1)
            pdf.cell(27, 6, f"{item['Base_Usada']:,.0f}", 1)
            pdf.cell(27, 6, f"{item['Valor_Cierre_Listado']:,.0f}", 1)
            pdf.cell(27, 6, f"{item['Cierre_Calculado']:,.0f}", 1)
            pdf.cell(18, 6, f"{item['Pct_Variacion']:.2f}%", 1)
            pdf.cell(45, 6, str(item['Estado']), 1)
            pdf.ln()

    return bytes(pdf.output())
