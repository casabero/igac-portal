
import pandas as pd
import io
import sys
import os

# Add path to module
sys.path.append(r'c:\Users\josep\OneDrive\Documentos\apps\igac-portal')
from modules.renumeracion_auditor import procesar_renumeracion

def create_mock_excel_logic():
    data = [
        # 1. PERMANENCIA (No cambia)
        # Mz 0001, Terr 0001 -> Se queda igual
        ('520010101000000010001000000000', '520010101000000010001000000000', 'ACTIVO', '1', ''), 
        
        # 2. NO REPETIDOS (Unicidad)
        # Dos predios diferentes intentan ir al mismo SNC (Error Critico)
        ('520010000000000090001000000001', '520010101000000010002000000999', 'ACTIVO', '2', 'Duplicado A'), 
        ('520010000000000090001000000002', '520010101000000010002000000999', 'ACTIVO', '2', 'Duplicado B'),

        # 3. NUEVO EN MZ EXISTENTE (Append)
        # Sector 01, Mz 0002. Ultimo conocido es 0010 (simulado por orden). LLegan nuevos.
        # Definimos historia: Mz 0002 tiene Terr 0010 (Permanencia)
        ('520010101000000020010000000000', '520010101000000020010000000000', 'ACTIVO', '1', 'Base Mz2'),
        # Llega uno nuevo 9xxx para Mz 0002. Deberia ser 0011.
        ('520010000000000099999000000001', '520010101000000020011000000000', 'ACTIVO', '1', 'Nuevo Mz2 -> 11'),

        # 4. NUEVO EN MZ NUEVA (9xxx -> Last+1)
        # Sector 01. Ultima Mz conocida es 0002 (del caso anterior).
        # Llega Mz 9001. Deberia convertirse en Mz 0003. Terr 0001.
        ('520010000000000088888000000001', '520010101000000009001000100000', 'ACTIVO', '1', 'Mz Nueva 9001 -> 0003'),
        # Otro predio en la misma Mz 9001. Deberia ser Mz 0003. Terr 0002.
        ('520010000000000088888000000002', '520010101000000009001000200000', 'ACTIVO', '1', 'Mz Nueva 9001 -> 0003-T2'),
    ]
    
    # Columnas: CICA, SNC, ESTADO, COND_PROP (pos 22 check), LABEL
    # Ojo: COND_PROP esta embebida en SNC pos 22 (index 21). Asegurarnos de usar chars correctos.
    # Los strings SNC de arriba tienen '0' en pos 22?
    # 52001... (30 chars)
    # Índices:
    # 0-5 Mpio
    # 5-7 Zona
    # 7-9 Sect
    # 9-13 Gap
    # 13-17 Mz
    # 17-21 Terr
    # 21 CondProp
    
    # Corregir data para asegurar COND_PROP = 1 (Formal) en los ejemplos
    # Pos 21 es el char 22. '520010101000000010001 `0` 0000000' -> El 0 es pos 21?
    # 5chars(0-4) + 2(5-6) + 2(7-8) + 4(9-12) + 4(13-16) + 4(17-20) + 1(21)
    # '52001' '01' '01' '0000' '0001' '0010' '0' ...
    # Sí, index 21 es CondProp.
    
    df = pd.DataFrame(data, columns=['NÚMERO_PREDIAL_CICA', 'NÚMERO_PREDIAL_SNC', 'ESTADO', 'COND_PROP_DUMMY', 'LABEL'])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

def test_logic_review():
    print("=== VERIFICACION DE LOGICA RENUMERACION V3.3 ===")
    excel = create_mock_excel_logic()
    res = procesar_renumeracion(excel, '1')
    
    # Obtener el DF Procesado con Sugerencias (no accesible directo desde res dict, 
    # pero podemos inferir de los 'errores' si tuviéramos acceso al engine, 
    # o mejor, modificamos procesar_renumeracion para devolver SUGGESTED en top_problematicos? 
    # No, mejor importamos la clase y corremos manual para inspeccionar el DF).
    
    from modules.renumeracion_auditor import AuditoriaSNC
    engine = AuditoriaSNC()
    excel.seek(0)
    engine.cargar_datos(excel, '1')
    engine.parsear_y_limpiar()
    engine.validar_unicidad_absoluta()
    engine.inicializar_memoria()
    engine.validar_lotes()
    engine.generar_sugerencias()
    
    df = engine.df_clean
    
    print("\n--- RESULTADOS DE SUGERENCIA ---")
    cols = ['LABEL', 'NÚMERO_PREDIAL_SNC', 'SUGGESTED_SNC', 'MATCH_SUGGESTION']
    # Add Mz info for debug
    if 'LABEL' not in df.columns:
        # Merge back label? No easy way. Just print by index matching
        pass
        
    for i, row in df.iterrows():
        snc = row['NÚMERO_PREDIAL_SNC']
        sugg = row['SUGGESTED_SNC']
        # Extraer Mz del sugerido
        # 52001 01 01 0000 [MZ:4] ...
        mz_sugg = sugg[13:17]
        terr_sugg = sugg[17:21]
        
        print(f"SNC: {snc} -> SUGG: {sugg} | Mz: {mz_sugg} Terr: {terr_sugg}")
        
        # Validaciones
        # Caso 4: Mz Nueva 9001 -> 0003
        if '9001' in snc and '0003' not in sugg:
             print(f"❌ FALLO CASO 4: Se esperaba Manzana 0003 para input 9001. Recibido Mz {mz_sugg}")
        elif '9001' in snc:
             print(f"✅ PASO CASO 4: Manzana 9001 renumerada a {mz_sugg}")

if __name__ == "__main__":
    test_logic_review()
