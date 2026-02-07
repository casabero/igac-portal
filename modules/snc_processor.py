import pandas as pd
import io
import os

def generar_colspecs(cortes):
    colspecs = []
    for i in range(len(cortes) - 1):
        colspecs.append((cortes[i], cortes[i+1]))
    colspecs.append((cortes[-1], None))
    return colspecs

def procesar_dataframe(file_stream, opcion, filename_original):
    config = {}

    # --- 1. CONFIGURACIÓN DE COLUMNAS Y CORTES ---
    if opcion == '1':
        config['columnas'] = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno (m2)", "AreaConstruida (m2)", "Avaluo ($)", "Vigencia", "NoPredialAnterior", "Espacio_Final"]
        config['cortes'] = [0, 2, 5, 30, 31, 34, 37, 137, 138, 139, 151, 251, 252, 253, 268, 274, 289, 297, 312]
        config['filtro_tipo'] = None
    elif opcion == '2':
        config['columnas'] = ["Departamento", "Municipio", "NoPredial", "TipoRegistro", "NoOrden", "TotalRegistro", "MatriculaInmobiliaria", "Espacio1", "ZonaFisica_1", "ZonaEconomica_1", "AreaTerreno_1 (m2)", "Espacio2", "ZonaFisica_2", "ZonaEconomica_2", "AreaTerreno_2 (m2)", "Espacio3", "Habitaciones_1", "Baños_1", "Locales_1", "Pisos_1", "Estrato_1", "Uso_1", "Puntaje_1", "AreaConstruida_1 (m2)", "Espacio4", "Habitaciones_2", "Baños_2", "Locales_2", "Pisos_2", "Estrato_2", "Uso_2", "Puntaje_2", "AreaConstruida_2 (m2)", "Espacio5", "Habitaciones_3", "Baños_3", "Locales_3", "Pisos_3", "Estrato_3", "Uso_3", "Puntaje_3", "AreaConstruida_3 (m2)", "Espacio6", "Vigencia", "NoPredialAnterior", "FinLinea"]
        config['cortes'] = [0, 2, 5, 30, 31, 34, 37, 55, 77, 80, 83, 98, 120, 123, 126, 141, 163, 165, 167, 169, 171, 172, 175, 177, 183, 205, 207, 209, 211, 213, 214, 217, 219, 225, 247, 249, 251, 253, 255, 256, 259, 261, 267, 289, 297, 312]
        config['filtro_tipo'] = None
    elif opcion == '3': # Reso Tipo 1
        config['columnas'] = ["Departamento", "Municipio", "NoResolucion", "NoRadicacion", "TipoTramite", "ClaseMutacion", "NoPredio", "Cancela/Inscribe", "TipoRegistro", "NoDocumento_A", "NoOrden", "TotalRegistros", "Nombre", "EstadoCivil", "TipoDocumento", "NoDocumento_B", "Direccion", "Comuna", "DestinoEconomico", "AreaTerreno (m2)", "AreaConstruida (m2)", "Avaluo ($ miles)", "Vigencia", "NoPredialAnterior", "Espacio"]
        config['cortes'] = [0, 2, 5, 18, 33, 35, 36, 61, 62, 63, 66, 69, 169, 170, 171, 183, 283, 284, 285, 300, 306, 321, 329, 344, 410]
        config['filtro_tipo'] = 1
    elif opcion == '4': # Reso Tipo 2
        config['columnas'] = ["Departamento", "Municipio", "NoResolucion", "NoRadicacion", "TipoTramite", "ClaseMutacion", "NoPredio", "Cancela/Inscribe", "TipoRegistro", "NoDocumento", "NoOrden", "TotalRegistros", "MatriculaInmobiliaria", "Espacio1", "ZonaFisica_1", "ZonaEconomica_1", "AreaTerreno_1", "Espacio2", "ZonaFisica_2", "ZonaEconomica_2", "AreaTerreno_2", "Espacio3", "Habitaciones_1", "Baños_1", "Locales_1", "Pisos_1", "Tipificacion_1", "Uso_1", "Puntaje_1", "AreaConstruida_1", "Espacio4", "Habitaciones_2", "Baños_2", "Locales_2", "Pisos_2", "Tipificacion_2", "Uso_2", "Puntaje_2", "AreaConstruida_2", "Espacio5", "Habitaciones_3", "Baños_3", "Locales_3", "Pisos_3", "Tipificacion_3", "Uso_3", "Puntaje_3", "AreaConstruida_3", "Espacio6", "NoPredialAnterior"]
        config['cortes'] = [0, 2, 5, 18, 33, 35, 36, 61, 62, 63, 66, 69, 87, 120, 123, 126, 141, 174, 177, 180, 195, 228, 230, 232, 234, 236, 238, 241, 243, 249, 282, 284, 286, 288, 290, 292, 295, 297, 303, 336, 338, 340, 342, 344, 346, 349, 351, 357, 395, 410]
        config['filtro_tipo'] = 2
    elif opcion == '5': # Reso Tipo 3
        config['columnas'] = ["Departamento", "Municipio", "NoResolucion", "NoRadicacion", "TipoTramite", "ClaseMutacion", "NoPredio", "Cancela/Inscribe", "TipoRegistro", "NoDocumento", "NoOrden", "TotalRegistros", "Decretos", "Motivacion", "NoPredialAnterior"]
        config['cortes'] = [0, 2, 5, 18, 33, 35, 36, 61, 62, 63, 66, 69, 139, 395, 410]
        config['filtro_tipo'] = 3
    else:
        raise ValueError("Opción no válida")

    # --- 2. LECTURA Y PROCESAMIENTO ---
    colspecs = generar_colspecs(config['cortes'])
    
    # Leer como texto (str) y con encoding latin-1 para tildes/ñ
    df = pd.read_fwf(file_stream, colspecs=colspecs, header=None, encoding='latin-1', dtype=str)
    
    # Asignar columnas de forma segura
    if len(df.columns) == len(config['columnas']):
        df.columns = config['columnas']
    else:
        df.columns = config['columnas'][:len(df.columns)]

    # Filtros por Tipo de Registro (para resoluciones)
    if config['filtro_tipo'] is not None and "TipoRegistro" in df.columns:
        df["TipoRegistro_Num"] = pd.to_numeric(df["TipoRegistro"], errors='coerce')
        df = df[df["TipoRegistro_Num"] == config['filtro_tipo']]
        df = df.drop(columns=["TipoRegistro_Num"])

    # Limpieza de espacios en blanco
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # --- 3. ORDENAMIENTO ---
    if 'NoOrden' in df.columns:
        df['NoOrden_Num'] = pd.to_numeric(df['NoOrden'], errors='coerce').fillna(0)
    
    sort_keys = ['Departamento', 'Municipio']
    if 'NoPredial' in df.columns: sort_keys.append('NoPredial')
    elif 'NoResolucion' in df.columns: sort_keys.append('NoResolucion')
    if 'NoOrden_Num' in df.columns: sort_keys.append('NoOrden_Num')
    
    final_keys = [k for k in sort_keys if k in df.columns]
    if final_keys:
        df = df.sort_values(by=final_keys, ascending=True)
    if 'NoOrden_Num' in df.columns: df = df.drop(columns=['NoOrden_Num'])

    # --- 4. CONVERSIÓN NUMÉRICA ---
    cols_valores = [c for c in df.columns if any(x in c for x in ['Area', 'Avaluo', 'Valor', 'Puntaje', 'Habitaciones', 'Baños', 'Locales', 'NoOrden', 'TotalRegistro'])]
    for col in cols_valores:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- 5. EXPORTACIÓN A EXCEL (SIN ESTILOS) ---
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos', startrow=1, header=False)
        workbook = writer.book
        worksheet = writer.sheets['Datos']
        plain_format = workbook.add_format({'bold': False, 'border': 0, 'align': 'left'})
        
        # Escribir encabezados manualmente
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, plain_format)
            
        # Ajustar ancho columnas
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
            if max_len > 50: max_len = 50
            worksheet.set_column(i, i, max_len)
            
    output.seek(0)
    
    # Nombre archivo de salida
    base_name = os.path.splitext(filename_original)[0]
    new_filename = f"{base_name}.xlsx"
    
    return output, new_filename