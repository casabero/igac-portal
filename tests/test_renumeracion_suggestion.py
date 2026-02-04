
import unittest
import pandas as pd
import sys
import os

# Add modules to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.renumeracion_auditor import AuditoriaSNC

class TestRenumeracionSuggestion(unittest.TestCase):
    def setUp(self):
        self.engine = AuditoriaSNC()
        
        # Create a mock DataFrame with sample data
        # Structure:
        # Col 0: ANT (Sort order)
        # Col 1: SNC (Current Code)
        # Col 2: ESTADO (ACTIVO)
        
        # We need to construct valid SNCs (30 chars)
        # Schema: M(5) Z(2) S(2) C(2) B(2) Mz(4) T(4) Cond(1) Edif(2) Piso(2) Unid(4)
        # Total 30 chars.
        
        # Common prefix: 05001 (M) 01 (Z) 01 (S) 00 (C) 00 (B) -> "0500101010000" (13 chars)
        self.prefix = "0500101010000"
        
        data = [
            # --- FORMAL GROUP: Manzana 0001 ---
            # Should restart Terreno sequence 0001, 0002, 0003
            {"ANT": "OLD_01", "SNC": f"{self.prefix}00010005000000000", "COND": "0"}, # Gap in T (5)
            {"ANT": "OLD_02", "SNC": f"{self.prefix}00010006000000000", "COND": "0"}, 
            {"ANT": "OLD_03", "SNC": f"{self.prefix}00010007000000000", "COND": "0"},
            
            # --- INFORMAL GROUP: Manzana 0002, Terreno 0001 (Parent) ---
            # Condition '2'. Should restart Seq 001, 002.
            # Parent: Prefix + 0002 + 0001 = 21 chars.
            # Suffix: 200000 + xxx
            {"ANT": "OLD_04", "SNC": f"{self.prefix}00020001200000999", "COND": "2"}, # Random old seq
            {"ANT": "OLD_05", "SNC": f"{self.prefix}00020001299999000", "COND": "2"}, # Random old seq
            
            # --- INFORMAL GROUP: Manzana 0002, Terreno 0002 (Different Parent) ---
             {"ANT": "OLD_06", "SNC": f"{self.prefix}00020002200000005", "COND": "2"}
        ]
        
        self.df = pd.DataFrame(data)
        self.engine.df = self.df
        self.engine.col_new = 'SNC'
        self.engine.col_ant = 'ANT'
        self.engine.col_estado = 'ESTADO'
        
        # Prepare valid SNC column for parsing
        self.engine.df['ESTADO'] = 'ACTIVO'
        
    def test_suggestions(self):
        print("\nRunning Suggested Renumeration Test...")
        
        # Run pipeline steps
        self.engine.parsear_y_limpiar()
        # Mock cleaning step passing
        self.engine.df_clean = self.engine.df.copy()
        
        # Need to run parsing again on df_clean to get SNC_PARTS columns?
        # The logic expects df_clean to have 'M_N', etc.
        # Let's manually trigger parse on the clean df as the code does.
        # Actually parsear_y_limpiar sets self.df_clean containing the columns.
        
        self.engine.generar_sugerencias()
        
        res = self.engine.df_clean[['ANT', 'SNC', 'SUGGESTED_SNC', 'COND_PROP']]
        print(res.to_string())
        
        # Verify Formal
        # Manzana 0001
        row1 = res.iloc[0] # OLD_01
        expected_1 = f"{self.prefix}00010001000000000" # T=0001
        self.assertEqual(row1['SUGGESTED_SNC'], expected_1, "Formal 1 should be Terreno 0001")
        
        row2 = res.iloc[1] # OLD_02
        expected_2 = f"{self.prefix}00010002000000000" # T=0002
        self.assertEqual(row2['SUGGESTED_SNC'], expected_2, "Formal 2 should be Terreno 0002")

        # Verify Informal
        # Parent 0002-0001. Cond 2.
        row4 = res.iloc[3] # OLD_04
        # Parent: ...00020001 (21 chars) + 200000 + 001 (3 chars)
        expected_inf_1 = f"{self.prefix}00020001200000001"
        self.assertEqual(row4['SUGGESTED_SNC'], expected_inf_1, "Informal 1 should be 200000001 suffix")
        
        row5 = res.iloc[4] # OLD_05
        expected_inf_2 = f"{self.prefix}00020001200000002"
        self.assertEqual(row5['SUGGESTED_SNC'], expected_inf_2, "Informal 2 should be 200000002 suffix")

        # Verify Informal 3 (New Parent)
        row6 = res.iloc[5]
        expected_inf_3 = f"{self.prefix}00020002200000001" # Reset counter for new parent
        self.assertEqual(row6['SUGGESTED_SNC'], expected_inf_3, "Informal 3 (New Parent) should reset to 001")


if __name__ == '__main__':
    unittest.main()
