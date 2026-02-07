
import pandas as pd
import io
import os
import sys

# Agregar path al modulo
sys.path.append(r'c:\Users\joseph.gari\OneDrive\Documentos\apps\igac-portal')

from modules.renumeracion_auditor import procesar_renumeracion

def create_mock_excel():
    # Creamos un DF con casos de prueba
    data = [
        # Caso 1: Permanencia correcta
        ('520010101000000010001000000001', '520010101000000010001000000001', 'ACTIVO'),
        
        # Caso 2: Duplicidad Absoluta (Critico)
        ('520010000000000090001000000001', '520010101000000010002000000999', 'ACTIVO'), # Origen A -> Destino X
        ('520010000000000090001000000002', '520010101000000010002000000999', 'ACTIVO'), # Origen B -> Destino X (DUPLICADO)

        # Caso 3: Huecos Numeracion (1, 3)
        ('520010000000000090005000000001', '520010101000000010005000000001', 'ACTIVO'),
        ('520010000000000090005000000003', '520010101000000010005000000003', 'ACTIVO'),
    ]
    
    df = pd.DataFrame(data, columns=['NÚMERO_PREDIAL_CICA', 'NÚMERO_PREDIAL_SNC', 'ESTADO'])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

def test_logic():
    print("Iniciando prueba de logica v3.1...")
    excel_file = create_mock_excel()
    
    try:
        resultado = procesar_renumeracion(excel_file, '1')
        print(f"Total Auditado: {resultado['total_auditado']}")
        print(f"Errores encontrados de reporte: {len(resultado['errores'])}")
        
        # Acceso directo a estadisticas del engine para mas detalle
        engine = resultado.get('engine_instance')
        if engine:
             print(f"Stats Engine: {engine.stats}")

        reglas = [e['REGLA'] for e in resultado['errores']]
        
        # 1. Unicidad
        assert any('UNICIDAD_SNC' in r for r in reglas), "Fallo deteccion Unicidad"
        print("✅ Unicidad Detectada")
        
        # 2. Huecos
        assert any('HUECOS_NUMERACION' in r for r in reglas), "Fallo deteccion Huecos"
        print("✅ Huecos Detectados")
        
        print("✅ PRUEBA EXITOSA V3.1: Se detectaron los errores esperados.")
        
    except Exception as e:
        print(f"❌ ERROR EN PRUEBA: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_logic()
