
import pandas as pd
import io
import os
import sys

# Agregar path al modulo
sys.path.append(r'c:\Users\joseph.gari\OneDrive\Documentos\apps\igac-portal')

from modules.renumeracion_auditor import procesar_renumeracion

def create_mock_excel():
    # Creamos un DF con casos de prueba
    # 1. Permanencia Correcta
    # 2. Hueco numeracion (1, 2, 4)
    # 3. Salto Manzana injustificado
    # 4. Estructura Invalida
    
    data = [
        # OLD (30 chars logic), NEW (30 chars logic), ESTADO
        # Caso 1: Permanencia
        ('520010101000000010001000000001', '520010101000000010001000000001', 'ACTIVO'),
        
        # Caso 2: Nuevo Lote con Hueco (Manzana nueva 0102)
        # Lote viene de 9000
        ('520010000000000090001000000001', '520010101000000010002000000001', 'ACTIVO'),
        ('520010000000000090001000000002', '520010101000000010002000000002', 'ACTIVO'),
        # Falta el 3
        ('520010000000000090001000000004', '520010101000000010002000000004', 'ACTIVO'),
        
        # Caso 3: Estructura Mala
        ('520010000000000090001000000005', '520010101000000010002000000BAD', 'ACTIVO'),
    ]
    
    df = pd.DataFrame(data, columns=['NÚMERO_PREDIAL_CICA', 'NÚMERO_PREDIAL_SNC', 'ESTADO'])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

def test_logic():
    print("Iniciando prueba de logica...")
    excel_file = create_mock_excel()
    
    try:
        resultado = procesar_renumeracion(excel_file, '1')
        print(f"Total Auditado: {resultado['total_auditado']}")
        print(f"Errores encontrados: {len(resultado['errores'])}")
        
        for e in resultado['errores']:
            print(f" - {e['REGLA']}: {e['DETALLE']} ({e['NUEVO']})")
            
        # Validaciones basicas
        reglas = [e['REGLA'] for e in resultado['errores']]
        
        # Chequear huecos
        assert any('HUECOS_NUMERACION' in r for r in reglas), "Fallo deteccion Huecos"
        # Chequear estructura
        assert any('ESTRUCTURA_NPN' in r for r in reglas), "Fallo deteccion Estructura"
        
        print("✅ PRUEBA EXITOSA: Se detectaron los errores esperados.")
        
    except Exception as e:
        print(f"❌ ERROR EN PRUEBA: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_logic()
