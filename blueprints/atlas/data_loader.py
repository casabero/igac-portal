"""Carga y gestión de datos geoespaciales para el Atlas.
Maneja la extracción de GDB/SHP desde ZIP, detección de capas y conversión a GPKG."""

import os
import json
import shutil
import tempfile
import zipfile
import geopandas as gpd
import fiona
from shapely.validation import make_valid

from .models import DATA_DIR, actualizar_municipio_gpkg, obtener_municipio, obtener_departamento

# Mapeo de nombres de capas esperados (normalizado)
LAYER_ALIASES = {
    'u_terreno': 'terrenos_urbano',
    'r_terreno': 'terrenos_rural',
    'u_manzana': 'manzanas',
    'r_vereda': 'veredas',
    'u_vial': 'vias_urbano',
    'r_vial': 'vias_rural',
    'u_nomenclatura_vial': 'nomenclatura_vial',
    'u_nomenclatura_domiciliaria': 'nomenclatura_dom',
    'u_construccion': 'construcciones',
    'r_construccion': 'construcciones_rural',
}


def normalizar_nombre_capa(name):
    """Normaliza nombre de capa para matching flexible."""
    return name.strip().lower().replace(' ', '_')


def detectar_fuente_gdb(extract_dir):
    """Busca un .gdb, .shp o .gpkg dentro del directorio extraido."""
    for root, dirs, files in os.walk(extract_dir):
        for d in dirs:
            if d.lower().endswith('.gdb'):
                return os.path.join(root, d), 'gdb'
        for f in files:
            if f.lower().endswith('.gpkg'):
                return os.path.join(root, f), 'gpkg'
            if f.lower().endswith('.shp'):
                return os.path.join(root, f), 'shp'
    return None, None


def listar_capas(source_path, source_type='gdb'):
    """Lista las capas disponibles en la fuente de datos."""
    try:
        layers = fiona.listlayers(source_path)
        return layers
    except Exception as e:
        print(f"Error listando capas: {e}")
        return []


def detectar_srs(source_path, layers):
    """Detecta el sistema de referencia de coordenadas de la primera capa con datos."""
    for layer_name in layers:
        try:
            gdf = gpd.read_file(source_path, layer=layer_name, rows=1)
            if gdf.crs:
                return str(gdf.crs)
        except Exception:
            continue
    return 'DESCONOCIDO'


def get_municipio_dir(departamento_nombre, municipio_nombre):
    """Retorna el path del directorio del municipio en el filesystem."""
    dep_slug = departamento_nombre.lower().replace(' ', '_')
    muni_slug = municipio_nombre.lower().replace(' ', '_')
    return os.path.join(DATA_DIR, dep_slug, muni_slug)


def procesar_upload_gdb(zip_file_storage, municipio_id, fecha_version=None):
    """
    Procesa un archivo ZIP subido que contiene GDB/SHP/GPKG.
    Lo convierte a GPKG y lo almacena permanentemente.

    Args:
        zip_file_storage: FileStorage de Flask (archivo subido)
        municipio_id: ID del municipio en la BD
        fecha_version: Optional date string for GDB version

    Returns:
        dict con status, capas detectadas, SRS, etc.
    """
    muni = obtener_municipio(municipio_id)
    if not muni:
        return {'status': 'error', 'message': 'Municipio no encontrado'}

    dep = obtener_departamento(muni['departamento_id'])
    if not dep:
        return {'status': 'error', 'message': 'Departamento no encontrado'}

    muni_dir = get_municipio_dir(dep['nombre'], muni['nombre'])
    os.makedirs(muni_dir, exist_ok=True)

    # Limpiar datos anteriores
    old_gpkg = os.path.join(muni_dir, 'atlas_data.gpkg')
    if os.path.exists(old_gpkg):
        os.remove(old_gpkg)

    tmp_dir = tempfile.mkdtemp()
    try:
        # Extraer ZIP
        zip_path = os.path.join(tmp_dir, 'upload.zip')
        zip_file_storage.save(zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(os.path.join(tmp_dir, 'extracted'))

        # Detectar fuente
        source_path, source_type = detectar_fuente_gdb(os.path.join(tmp_dir, 'extracted'))
        if not source_path:
            return {'status': 'error', 'message': 'No se encontró GDB, SHP ni GPKG dentro del ZIP'}

        # Listar capas
        raw_layers = listar_capas(source_path, source_type)
        if not raw_layers:
            return {'status': 'error', 'message': 'La fuente no contiene capas legibles'}

        # Detectar SRS
        srs = detectar_srs(source_path, raw_layers)

        # Convertir a GPKG (capa por capa)
        output_gpkg = os.path.join(muni_dir, 'atlas_data.gpkg')
        capas_cargadas = []
        errores = []

        for layer_name in raw_layers:
            try:
                gdf = gpd.read_file(source_path, layer=layer_name)
                if gdf.empty:
                    continue

                # Validar geometrias
                if gdf.geometry is not None and not gdf.geometry.is_empty.all():
                    gdf['geometry'] = gdf.geometry.apply(
                        lambda g: make_valid(g) if g and not g.is_valid else g
                    )

                gdf.to_file(output_gpkg, layer=layer_name, driver='GPKG',
                            mode='a' if os.path.exists(output_gpkg) else 'w')

                norm = normalizar_nombre_capa(layer_name)
                alias = LAYER_ALIASES.get(norm, norm)
                capas_cargadas.append({
                    'nombre_original': layer_name,
                    'nombre_normalizado': alias,
                    'tipo_geometria': gdf.geometry.geom_type.iloc[0] if len(gdf) > 0 else 'Unknown',
                    'registros': len(gdf),
                    'columnas': list(gdf.columns)
                })
            except Exception as e:
                errores.append(f"{layer_name}: {str(e)}")

        if not capas_cargadas:
            return {'status': 'error', 'message': 'No se pudo cargar ninguna capa', 'errores': errores}

        # Actualizar BD
        nombres_capas = [c['nombre_original'] for c in capas_cargadas]
        actualizar_municipio_gpkg(municipio_id, output_gpkg, srs, nombres_capas, fecha_version)

        # Guardar metadata
        metadata = {
            'srs': srs,
            'capas': capas_cargadas,
            'errores': errores,
            'source_type': source_type,
        }
        with open(os.path.join(muni_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        return {
            'status': 'ok',
            'srs': srs,
            'capas': capas_cargadas,
            'errores': errores,
            'gpkg_path': output_gpkg,
            'total_capas': len(capas_cargadas),
        }

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def cargar_capa(municipio_id, layer_name, bbox=None):
    """Carga una capa específica del GPKG de un municipio.

    Args:
        municipio_id: ID del municipio
        layer_name: Nombre de la capa en el GPKG
        bbox: Optional tuple (minx, miny, maxx, maxy) para filtrar espacialmente

    Returns:
        GeoDataFrame o None
    """
    muni = obtener_municipio(municipio_id)
    if not muni or not muni.get('gpkg_path'):
        return None

    gpkg_path = muni['gpkg_path']
    if not os.path.exists(gpkg_path):
        return None

    try:
        if bbox:
            gdf = gpd.read_file(gpkg_path, layer=layer_name, bbox=bbox)
        else:
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
        return gdf
    except Exception as e:
        print(f"Error cargando capa {layer_name}: {e}")
        return None


def buscar_predio(municipio_id, codigo, campo='CODIGO'):
    """Busca un predio en U_TERRENO + R_TERRENO (combinadas) por CODIGO o CODIGO_ANTERIOR.

    Args:
        municipio_id: ID del municipio
        codigo: Valor a buscar
        campo: 'CODIGO' o 'CODIGO_ANTERIOR' — el usuario elige

    Returns:
        dict con 'geometry', 'bounds', 'attributes', 'layer' o None
    """
    muni = obtener_municipio(municipio_id)
    if not muni or not muni.get('gpkg_path'):
        return None

    gpkg_path = muni['gpkg_path']

    # Solo buscar en capas de terrenos (U_TERRENO y R_TERRENO)
    try:
        all_layers = fiona.listlayers(gpkg_path)
    except Exception:
        return None

    capas_terreno = []
    for ln in all_layers:
        norm = normalizar_nombre_capa(ln)
        if norm in ('u_terreno', 'r_terreno'):
            capas_terreno.append(ln)

    if not capas_terreno:
        return None

    # Cargar y combinar U_TERRENO + R_TERRENO como una sola fuente
    frames = []
    for layer_name in capas_terreno:
        try:
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            if not gdf.empty:
                gdf['_source_layer'] = layer_name
                frames.append(gdf)
        except Exception as e:
            print(f"Error cargando {layer_name}: {e}")

    if not frames:
        return None

    import pandas as pd
    terrenos = pd.concat(frames, ignore_index=True)

    # Buscar la columna del campo solicitado (case-insensitive)
    campo_upper = campo.upper()
    col_match = None
    for col in terrenos.columns:
        if col.upper() == campo_upper:
            col_match = col
            break

    if not col_match:
        return None

    # Match exacto primero
    codigo_str = str(codigo).strip()
    match = terrenos[terrenos[col_match].astype(str).str.strip() == codigo_str]

    # Si no hay match exacto, buscar parcial (contiene)
    if match.empty:
        match = terrenos[terrenos[col_match].astype(str).str.contains(codigo_str, na=False)]

    if match.empty:
        return None

    row = match.iloc[0]
    bounds = row.geometry.bounds
    attrs = {k: str(v) for k, v in row.drop('geometry').items() if k != '_source_layer'}
    return {
        'geometry': row.geometry,
        'bounds': bounds,
        'codigo': str(row[col_match]),
        'attributes': attrs,
        'layer': row.get('_source_layer', 'TERRENO'),
        'total_matches': len(match),
    }


def buscar_predio_por_coordenada(municipio_id, x, y):
    """Busca un predio en capas de terreno que contenga el punto (x, y)."""
    muni = obtener_municipio(municipio_id)
    if not muni or not muni.get('gpkg_path'):
        return None

    gpkg_path = muni['gpkg_path']
    from shapely.geometry import Point

    # Intentar crear punto (validar inputs antes de llamar)
    try:
        p = Point(float(x), float(y))
    except (ValueError, TypeError):
        return None

    # Identificar capas de terreno
    try:
        all_layers = fiona.listlayers(gpkg_path)
    except Exception:
        return None

    capas_terreno = []
    for ln in all_layers:
        norm = normalizar_nombre_capa(ln)
        if norm in ('u_terreno', 'r_terreno'):
            capas_terreno.append(ln)

    if not capas_terreno:
        return None

    # Buscar en cada capa (spatial filter es mas eficiente pero geopandas read_file con bbox/mask es complejo con puntos)
    # Una optimizacion: cargar solo geometrias que intersecten un bbox pequeño alrededor del punto
    # bbox = (x-1, y-1, x+1, y+1)
    
    # Dado que son archivos locales GPKG, leer todo y filtrar es lento si son grandes.
    # Usaremos bbox en read_file para optimizar.
    
    delta = 1000 # 1km bbox search buffer for safety/accuracy with arbitrary coords? No, exact point.
    # A point has no dimension, bbox needs extension. Let's try small delta.
    delta = 50 
    bbox = (x - delta, y - delta, x + delta, y + delta)

    for layer_name in capas_terreno:
        try:
            # Usar bbox filter para cargar solo geometrias cercanas
            gdf = gpd.read_file(gpkg_path, layer=layer_name, bbox=bbox)
            if gdf.empty:
                continue
            
            # Filtrar exacto (contains)
            matches = gdf[gdf.geometry.contains(p)]
            if not matches.empty:
                row = matches.iloc[0]
                attrs = {k: str(v) for k, v in row.drop('geometry').items()}
                
                # Intentar buscar codigo en columnas comunes
                codigo = "SIN_CODIGO"
                for col in gdf.columns:
                    if 'CODIGO' in col.upper():
                        codigo = str(row[col])
                        break
                        
                return {
                    'geometry': row.geometry,
                    'bounds': row.geometry.bounds,
                    'codigo': codigo,
                    'attributes': attrs,
                    'layer': layer_name,
                    'total_matches': 1
                }
        except Exception as e:
            print(f"Error espacial en {layer_name}: {e}")
            continue

    return None
