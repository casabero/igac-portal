import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
import os
import shutil
import zipfile
import uuid
from datetime import datetime, timezone, timedelta
import traceback

def unzip_file(zip_path, extract_to):
    """Extrae un archivo ZIP en la carpeta especificada."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def find_shp_in_folder(folder):
    """Busca el primer archivo .shp en una carpeta (recursivo)."""
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.lower().endswith(".shp"):
                return os.path.join(root, file)
    return None

def validar_geometrias(gdf):
    """
    Valida y corrige geometrías usando shapely.make_valid.
    Elimina geometrías vacías o nulas.
    """
    if gdf is None or gdf.empty:
        return gdf
    
    # 1. Detectar inválidos
    mask_invalid = ~gdf.geometry.is_valid
    if mask_invalid.any():
        print(f"  -> Corrigiendo {mask_invalid.sum()} geometrías inválidas...")
        # make_valid puede devolver GeometryCollection, nos aseguramos de explotarlas o quedarnos con polígonos
        gdf.loc[mask_invalid, "geometry"] = gdf.loc[mask_invalid, "geometry"].apply(make_valid)
    
    # 2. Filtrar solo Polígonos/MultiPolígonos (por si make_valid devolvió puntos/lineas)
    gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]

    # 3. Eliminar vacíos
    gdf = gdf[~gdf.geometry.is_empty]
    gdf = gdf[gdf.geometry.notna()]
    
    return gdf

def find_gdb_in_folder(folder):
    """Busca una carpeta con extensión .gdb"""
    for root, dirs, files in os.walk(folder):
        for d in dirs:
            if d.lower().endswith(".gdb"):
                return os.path.join(root, d)
    return None

def cargar_capas_gdb(gdb_path, prefix_filter=None, exact_layers=None):
    """
    Carga capas de un GDB.
    exact_layers: lista de nombres exactos a buscar.
    """
    if not gdb_path or not os.path.exists(gdb_path):
        return []
    
    import fiona
    all_layers = fiona.listlayers(gdb_path)
    loaded_gdfs = []
    
    to_load = []
    if exact_layers:
        to_load = [lyr for lyr in all_layers if lyr in exact_layers]
    
    if not to_load and prefix_filter:
        to_load = [lyr for lyr in all_layers if lyr.startswith(prefix_filter)]

    for layer_name in to_load:
        print(f"    -> Cargando capa: {layer_name}")
        try:
            # Intentar usar pyogrio si está disponible para velocidad
            gdf = gpd.read_file(gdb_path, layer=layer_name, engine='pyogrio' if 'pyogrio' in globals() else None)
            if not gdf.empty:
                loaded_gdfs.append(gdf)
        except:
            # Fallback a fiona
            gdf = gpd.read_file(gdb_path, layer=layer_name)
            if not gdf.empty:
                loaded_gdfs.append(gdf)
                
    return loaded_gdfs

def procesar_informales(rutas_zips, output_folder, prefijo='200000'):
    """
    rutas_zips: dict con keys 'zip_inf', 'zip_formal'
    output_folder: carpeta donde guardar resultados.
    prefijo: string para la renumeración.
    """
    temp_dir = os.path.join(output_folder, "temp_process_" + str(uuid.uuid4()))
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # 1. Procesar INFORMAL
        path_inf_zip = rutas_zips.get('zip_inf')
        gdfs_inf = []
        if path_inf_zip:
            folder_inf = os.path.join(temp_dir, "inf_extracted")
            unzip_file(path_inf_zip, folder_inf)
            gdb_inf = find_gdb_in_folder(folder_inf)
            if gdb_inf:
                print(f"GDB Informal encontrado: {gdb_inf}")
                gdfs_inf = cargar_capas_gdb(gdb_inf, exact_layers=['R_TERRENO_INFORMAL', 'U_TERRENO_INFORMAL', 'TERRENO_INFORMAL'])
        
        if not gdfs_inf:
            raise ValueError("No se encontraron capas válidas en el GDB Informal (R_TERRENO_INFORMAL, U_TERRENO_INFORMAL).")
        
        gdf_inf = pd.concat(gdfs_inf, ignore_index=True)
        gdf_inf = validar_geometrias(gdf_inf)

        # 2. Procesar FORMAL (CTM12)
        path_formal_zip = rutas_zips.get('zip_formal')
        gdfs_formal = []
        if path_formal_zip:
            folder_formal = os.path.join(temp_dir, "formal_extracted")
            unzip_file(path_formal_zip, folder_formal)
            gdb_formal = find_gdb_in_folder(folder_formal)
            if gdb_formal:
                print(f"GDB Formal encontrado: {gdb_formal}")
                gdfs_formal = cargar_capas_gdb(gdb_formal, exact_layers=['R_TERRENO', 'U_TERRENO', 'TERRENO'])
        
        if not gdfs_formal:
            raise ValueError("No se encontraron capas válidas en el GDB Formal (R_TERRENO, U_TERRENO).")
            
        gdf_ctm = pd.concat(gdfs_formal, ignore_index=True)
        gdf_ctm = validar_geometrias(gdf_ctm)

        # 3. Asegurar mismo CRS
        if gdf_inf.crs != gdf_ctm.crs:
            print(f"Reproyectando {len(gdf_inf)} predios informales al CRS de Formal...")
            gdf_inf = gdf_inf.to_crs(gdf_ctm.crs)

        # 4. Optimización Espacial: Filtrar Formal (CTM) por el Bounding Box de Informal
        print("Optimizando capas espaciales...")
        bbox = gdf_inf.total_bounds
        gdf_ctm_filt = gdf_ctm.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
        print(f"  -> CTM reducido de {len(gdf_ctm)} a {len(gdf_ctm_filt)} predios potenciales.")

        if gdf_ctm_filt.empty:
            raise ValueError("No hay intersección espacial entre los predios informales y la base Formal (CTM12) cargada.")

        # 5. Intersección
        print(f"Ejecutando intersección de mayor área ({len(gdf_inf)} vs {len(gdf_ctm_filt)})...")
        inter = gpd.overlay(gdf_inf, gdf_ctm_filt, how='intersection', keep_geom_type=True)
        inter["area_calc"] = inter.geometry.area
        
        # Identificar columna de ID Informal (CODIGO)
        col_id_inf = "CODIGO"
        if col_id_inf not in inter.columns:
            candidates = [c for c in inter.columns if "codigo" in c.lower()]
            col_id_inf = candidates[0] if candidates else inter.columns[0]

        # Quedarse con el de mayor área para cada informal
        inter = (
            inter.sort_values([col_id_inf, "area_calc"], ascending=[True, False])
                 .drop_duplicates(subset=col_id_inf)
        )
        
        # Identificar columna de ID Formal
        # Si overlay tiene dos columnas CODIGO, las renombra como _1 y _2 (o _1 y _2 si ya existian)
        # Normalmente: left_df (inf), right_df (formal).
        # Si ambos son 'CODIGO', left -> CODIGO_1, right -> CODIGO_2
        col_ctm_final = None
        for c in ["CODIGO_2", "CODIGO_CTM_2", "CODIGO_CTM", "CODIGO_1"]:
            if c in inter.columns:
                col_ctm_final = c
                break
        
        if not col_ctm_final:
             col_ctm_final = inter.columns[0] 

        print(f"Usando columna identificadora CTM: {col_ctm_final}")

        # Numeración
        inter = inter.sort_values(by=[col_ctm_final])
        inter["NumRen_Val"] = inter.groupby(col_ctm_final).cumcount() + 1
        inter["NumRen"] = inter["NumRen_Val"].astype(str).str.zfill(3)
        
        inter["RENUMERADO"] = (
            inter[col_ctm_final].astype(str).str.slice(0, 21) + 
            str(prefijo) + 
            inter["NumRen"]
        )
        
        # 5. Generar Log
        log_df = (
            inter.groupby(col_ctm_final)
            .agg(
                cantidad=("RENUMERADO", "count"),
                area_total=("area_calc", "sum"),
                codigos_asignados=("RENUMERADO", lambda x: ", ".join(x.astype(str)))
            )
            .reset_index()
        )
        log_df["fecha_proceso"] = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d %H:%M:%S")
        log_data = log_df.to_dict(orient="records")
        
        # 6. Exportar SHP resultado
        out_shp_name = "RENOMERACION_INFORMALES.shp"
        out_shp_path = os.path.join(temp_dir, out_shp_name)
        inter.to_file(out_shp_path, encoding='utf-8')
        
        # 7. Zippear todo
        zip_filename = f"Resultado_Renumeracion_{uuid.uuid4().hex[:8]}.zip"
        zip_output_path = os.path.join(output_folder, zip_filename)
        
        with zipfile.ZipFile(zip_output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            base_name = os.path.splitext(out_shp_name)[0]
            for file in os.listdir(temp_dir):
                if file.startswith(base_name) or file.endswith(".json"):
                     zipf.write(os.path.join(temp_dir, file), arcname=file)
        
        try: shutil.rmtree(temp_dir)
        except: pass
            
        return {
            "status": "success",
            "zip_path": zip_output_path,
            "zip_filename": zip_filename,
            "log": log_data,
            "total_procesados": len(inter)
        }

    except Exception as e:
        traceback.print_exc()
        try: shutil.rmtree(temp_dir)
        except: pass
        return {"status": "error", "message": str(e)}

    except Exception as e:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e)
        }
