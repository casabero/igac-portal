import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
import os
import shutil
import zipfile
import uuid
from datetime import datetime
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

def procesar_informales(rutas_zips, output_folder, prefijo='200000'):
    """
    rutas_zips: dict con keys 'r_inf', 'u_inf', 'r_ctm', 'u_ctm' apuntando a rutas de archivos ZIP o SHP.
    output_folder: carpeta donde guardar resultados.
    prefijo: string para la renumeración.
    """
    temp_dir = os.path.join(output_folder, "temp_process_" + str(uuid.uuid4()))
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        dataframes = {}
        
        # 1. Descomprimir y leer cada input
        for key, filepath in rutas_zips.items():
            if not filepath:
                continue
                
            subfolder = os.path.join(temp_dir, key)
            os.makedirs(subfolder, exist_ok=True)
            
            if filepath.lower().endswith('.zip'):
                unzip_file(filepath, subfolder)
                shp_path = find_shp_in_folder(subfolder)
            elif filepath.lower().endswith('.shp'):
                shp_path = filepath
            else:
                 # Asumimos que es un shapefile subido que se guardó temporalmente sin extensión o algo así, 
                 # pero idealmente debe ser zip o shp. Si no lo encontramos, error.
                 shp_path = None

            if shp_path:
                print(f"Cargando {key}: {shp_path}")
                gdf = gpd.read_file(shp_path)
                dataframes[key] = gdf
            else:
                print(f"Advertencia: No se encontró shapefile para {key}")
                dataframes[key] = None

        # 2. Merge Informales (R + U)
        list_inf = [df for k, df in dataframes.items() if 'inf' in k and df is not None]
        if not list_inf:
            raise ValueError("No se cargaron capas de predios informales.")
        
        gdf_inf = pd.concat(list_inf, ignore_index=True)
        gdf_inf = validar_geometrias(gdf_inf)
        
        # 3. Merge CTM (R + U)
        list_ctm = [df for k, df in dataframes.items() if 'ctm' in k and df is not None]
        if not list_ctm:
            raise ValueError("No se cargaron capas CTM12.")
            
        gdf_ctm = pd.concat(list_ctm, ignore_index=True)
        gdf_ctm = validar_geometrias(gdf_ctm)

        # Asegurar mismo CRS
        if gdf_inf.crs != gdf_ctm.crs:
            print("Reproyectando Informales al CRS de CTM...")
            gdf_inf = gdf_inf.to_crs(gdf_ctm.crs)

        # 4. Intersección
        print("Ejecutando intersección...")
        # overlay 'intersection'
        # keep_geom_type=True para asegurar que el resultado sea del mismo tipo (Polígonos)
        inter = gpd.overlay(gdf_inf, gdf_ctm, how='intersection', keep_geom_type=True)
        
        # Calcular área
        inter["area_calc"] = inter.geometry.area
        
        # 5. Ordenar y Quedarse con mayor área
        # Asumimos que "CODIGO" viene de los Informales (identificador único del informal original)
        # Si el campo se llama distinto, ajustar. En el script original era "CODIGO"
        if "CODIGO" not in inter.columns:
            # Intentar buscar columnas parecidas si no existe exacto
            candidates = [c for c in inter.columns if "codigo" in c.lower()]
            col_id_inf = candidates[0] if candidates else None
            if not col_id_inf:
                # Fallback: usar índice
                inter["CODIGO_TEMP"] = inter.index
                col_id_inf = "CODIGO_TEMP"
        else:
            col_id_inf = "CODIGO"

        print("Filtrando duplicados por mayor área...")
        inter = (
            inter.sort_values([col_id_inf, "area_calc"], ascending=[True, False])
                 .drop_duplicates(subset=col_id_inf)
        )
        
        # 6. Consecutivo por Código CTM (CODIGO_1)
        # Asumimos que "CODIGO_1" viene del CTM. Ajustar si es necesario.
        # En el overlay, si ambos tienen columna "CODIGO", geopandas pone _1 y _2.
        # Necesitamos saber cual es el del CTM.
        # Estrategia robusta: ver columnas de gdf_ctm.
        ctm_cols = gdf_ctm.columns
        target_col_ctm = "CODIGO" 
        
        # Si en el resultado del overlay la columna 'CODIGO' del CTM cambió de nombre:
        # Geopandas overlay: left_df (inf), right_df (ctm).
        # Si ambos tienen 'CODIGO', left -> CODIGO_1, right -> CODIGO_2
        
        col_ctm_final = None
        if "CODIGO_2" in inter.columns:
             col_ctm_final = "CODIGO_2"
        elif "CODIGO_1" in inter.columns and "CODIGO" in gdf_inf.columns:
             # Esto pasa si INF tiene CODE y CTM tiene CODE
             col_ctm_final = "CODIGO_2" 
        else:
             # Buscamos en las columnas del CTM original una que sea codigo
             for c in ["CODIGO", "CODIGO_CTM", "PK_PREDIO"]:
                 if c in gdf_ctm.columns:
                     # Ahora buscamos como se llama en 'inter'
                     if c in inter.columns:
                         col_ctm_final = c
                     elif f"{c}_2" in inter.columns:
                         col_ctm_final = f"{c}_2"
                     break
        
        if not col_ctm_final:
             # Fallback terrible
             col_ctm_final = inter.columns[0] 

        print(f"Usando columna CTM: {col_ctm_final}")

        # Sorting final para numeracion ordenada
        inter = inter.sort_values(by=[col_ctm_final])

        # Generar consecutivos
        print("Generando consecutivos...")
        inter["NumRen"] = (
            inter.groupby(col_ctm_final)
                 .cumcount() + 1
        )
        
        # Formatear
        inter["NumRen_Str"] = inter["NumRen"].astype(str).str.zfill(3)
        
        # RENUMERADO: CODIGO_CTM (primeros 21? o completo) + PREFIJO + CONSECUTIVO
        # El usuario dijo: codigo[:21]
        inter["RENUMERADO"] = (
            inter[col_ctm_final].astype(str).str.slice(0, 21) + 
            str(prefijo) + 
            inter["NumRen_Str"]
        )
        
        # Limpieza de columnas auxiliares
        # inter = inter.drop(columns=["area_calc", "NumRen"])
        # Renombrar NumRen_Str a NumRen
        inter["NumRen"] = inter["NumRen_Str"]
        
        # 7. Generar Log
        print("Generando Logs...")
        log_df = (
            inter.groupby(col_ctm_final)
            .agg(
                cantidad=("RENUMERADO", "count"),
                area_total=("area_calc", "sum"),
                codigos_asignados=("RENUMERADO", lambda x: ", ".join(x.astype(str)))
            )
            .reset_index()
        )
        log_df["fecha_proceso"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Guardar Log JSON
        log_json_path = os.path.join(temp_dir, "log_renumeracion.json")
        log_df.to_json(log_json_path, orient="records", force_ascii=False)
        
        log_data = log_df.to_dict(orient="records") # Para retornarlo a la vista
        
        # 8. Exportar SHP resultado
        print("Exportando Shapefile...")
        out_shp_name = "RENOMERACION_INFORMALES.shp"
        out_shp_path = os.path.join(temp_dir, out_shp_name)
        
        # Geopandas trunca nombres de columnas a 10 chars en SHP. Ojo.
        # RENUMERADO -> RENUMERAD
        inter.to_file(out_shp_path, encoding='utf-8')
        
        # 9. Zippear todo para descarga
        print("Creando ZIP de salida...")
        zip_filename = f"Resultado_Renumeracion_{uuid.uuid4().hex[:8]}.zip"
        zip_output_path = os.path.join(output_folder, zip_filename)
        
        with zipfile.ZipFile(zip_output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Agregar el shp y sus componentes (.dbf, .shx, .prj, cpg)
            base_name = os.path.splitext(out_shp_name)[0]
            for file in os.listdir(temp_dir):
                if file.startswith(base_name) or file.endswith(".json") or file.endswith(".csv"):
                     zipf.write(os.path.join(temp_dir, file), arcname=file)
        
        # Limpieza
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
            
        return {
            "status": "success",
            "zip_path": zip_output_path,
            "zip_filename": zip_filename,
            "log": log_data,
            "total_procesados": len(inter)
        }

    except Exception as e:
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e)
        }
