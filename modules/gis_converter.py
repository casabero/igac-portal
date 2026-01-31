
import os
import shutil
import zipfile
import uuid
import geopandas as gpd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_gdb_conversion(zip_path, output_folder):
    """
    Procesa un archivo ZIP que contiene una GDB, la convierte a GeoPackage
    y devuelve la ruta del archivo generado.
    """
    task_id = str(uuid.uuid4())
    extract_path = os.path.join(output_folder, f"temp_{task_id}")
    os.makedirs(extract_path, exist_ok=True)
    
    try:
        # 1. Descomprimir el ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        # 2. Buscar la carpeta .gdb
        # Buscamos recursivamente por si la GDB está dentro de una subcarpeta
        gdb_path = None
        for root, dirs, files in os.walk(extract_path):
            for d in dirs:
                if d.endswith('.gdb'):
                    gdb_path = os.path.join(root, d)
                    break
            if gdb_path:
                break
        
        if not gdb_path:
            raise ValueError("No se encontró ninguna carpeta .gdb dentro del archivo ZIP.")
            
        # 3. Leer capas y convertir a GPKG
        # Nombre del output basado en el nombre de la GDB
        gdb_name = os.path.splitext(os.path.basename(gdb_path))[0]
        output_gpkg = os.path.join(output_folder, f"{gdb_name}_{task_id}.gpkg")
        
        # Listar capas disponibles
        layers = gpd.list_layers(gdb_path)
        if layers.empty:
             raise ValueError("La GDB no contiene capas reconocibles.")
             
        # Iterar sobre las capas y guardarlas en el GeoPackage
        # Usamos pyogrio engine si está disponible por rendimiento, sino fiona (default)
        engine = 'pyogrio' 
        
        for layer_name in layers['layer_name']:
            try:
                logger.info(f"Procesando capa: {layer_name}")
                gdf = gpd.read_file(gdb_path, layer=layer_name, engine=engine)
                
                # Guardar en GPKG
                # Si es la primera capa, 'w' (write), sino 'a' (append)
                # Ojo: geopandas to_file con driver GPKG maneja capas
                gdf.to_file(output_gpkg, layer=layer_name, driver="GPKG", engine=engine)
                
            except Exception as e:
                logger.error(f"Error procesando capa {layer_name}: {str(e)}")
                # Podríamos decidir si fallar todo o continuar con otras capas.
                # Por ahora, continuamos e informamos en el log.
                continue
                
        if not os.path.exists(output_gpkg):
             raise ValueError("No se pudo generar el archivo GeoPackage.")
             
        return output_gpkg
        
    except Exception as e:
        logger.error(f"Error en conversión GDB -> GPKG: {str(e)}")
        raise e
        
    finally:
        # 4. Limpieza de temporales (carpeta descomprimida)
        if os.path.exists(extract_path):
            try:
                shutil.rmtree(extract_path)
            except Exception as e:
                logger.warning(f"No se pudo eliminar temporales {extract_path}: {e}")
