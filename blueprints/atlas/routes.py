"""Blueprint: GIS Atlas Generator — Rutas para gestión de datos y generación de mapas."""

from flask import Blueprint, render_template, request, jsonify, Response, session
from modules.db_logger import registrar_visita

from .models import (
    init_atlas_db, crear_departamento, listar_departamentos, obtener_departamento,
    eliminar_departamento, crear_municipio, listar_municipios, obtener_municipio,
    eliminar_municipio, obtener_municipio_completo
)
from .data_loader import procesar_upload_gdb, buscar_predio, cargar_capa
from .map_renderer import render_preview, render_pdf

import json
import fiona

atlas_bp = Blueprint('atlas', __name__, url_prefix='/karta',
                      template_folder='../../templates/atlas')


# --- Pagina principal ---
@atlas_bp.route('/')
def index():
    registrar_visita('/karta')
    departamentos = listar_departamentos()
    return render_template('atlas/index.html', departamentos=departamentos, is_admin=session.get('admin_logged_in', False))


# --- API: Departamentos ---
@atlas_bp.route('/api/departamentos', methods=['GET'])
def api_listar_departamentos():
    return jsonify(listar_departamentos())


@atlas_bp.route('/api/departamentos', methods=['POST'])
def api_crear_departamento():
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Acceso solo para administrador'}), 403

    data = request.get_json()
    if not data or not data.get('nombre'):
        return jsonify({'error': 'Nombre requerido'}), 400
    try:
        dep = crear_departamento(data['nombre'], data.get('codigo'))
        return jsonify(dict(dep)), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@atlas_bp.route('/api/departamentos/<int:dep_id>', methods=['DELETE'])
def api_eliminar_departamento(dep_id):
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Acceso solo para administrador'}), 403

    try:
        eliminar_departamento(dep_id)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


# --- API: Municipios ---
@atlas_bp.route('/api/departamentos/<int:dep_id>/municipios', methods=['GET'])
def api_listar_municipios(dep_id):
    return jsonify(listar_municipios(dep_id))


@atlas_bp.route('/api/departamentos/<int:dep_id>/municipios', methods=['POST'])
def api_crear_municipio(dep_id):
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Acceso solo para administrador'}), 403

    data = request.get_json()
    if not data or not data.get('nombre'):
        return jsonify({'error': 'Nombre requerido'}), 400
    try:
        muni = crear_municipio(dep_id, data['nombre'], data.get('codigo'))
        return jsonify(dict(muni)), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@atlas_bp.route('/api/municipios/<int:muni_id>', methods=['DELETE'])
def api_eliminar_municipio(muni_id):
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Acceso solo para administrador'}), 403

    try:
        eliminar_municipio(muni_id)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@atlas_bp.route('/api/municipios/<int:muni_id>', methods=['GET'])
def api_obtener_municipio(muni_id):
    muni = obtener_municipio_completo(muni_id)
    if not muni:
        return jsonify({'error': 'No encontrado'}), 404
    # Parsear capas JSON
    if muni.get('capas_disponibles'):
        try:
            muni['capas_disponibles'] = json.loads(muni['capas_disponibles'])
        except Exception:
            pass
    return jsonify(muni)


# --- API: Upload GDB ---
@atlas_bp.route('/api/municipios/<int:muni_id>/upload', methods=['POST'])
def api_upload_gdb(muni_id):
    if not session.get('admin_logged_in'):
        return jsonify({'error': 'Acceso solo para administrador'}), 403

    if 'archivo_zip' not in request.files:
        return jsonify({'error': 'No se recibió archivo'}), 400
    file = request.files['archivo_zip']
    if file.filename == '':
        return jsonify({'error': 'Archivo vacío'}), 400
    if not file.filename.lower().endswith('.zip'):
        return jsonify({'error': 'Solo se aceptan archivos .zip'}), 400

    result = procesar_upload_gdb(file, muni_id)
    if result['status'] == 'error':
        return jsonify(result), 400
    return jsonify(result)


# --- API: Capas disponibles ---
@atlas_bp.route('/api/municipios/<int:muni_id>/capas', methods=['GET'])
def api_listar_capas(muni_id):
    muni = obtener_municipio(muni_id)
    if not muni or not muni.get('gpkg_path'):
        return jsonify({'error': 'Sin datos cargados'}), 404
    try:
        import os
        if not os.path.exists(muni['gpkg_path']):
            return jsonify({'error': 'Archivo GPKG no encontrado'}), 404
        layers = fiona.listlayers(muni['gpkg_path'])
        return jsonify({'capas': layers, 'srs': muni.get('srs', 'N/A')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- API: Busqueda de predio ---
@atlas_bp.route('/api/municipios/<int:muni_id>/buscar', methods=['GET'])
def api_buscar_predio(muni_id):
    codigo = request.args.get('codigo', '').strip()
    if not codigo:
        return jsonify({'error': 'Código requerido'}), 400

    resultado = buscar_predio(muni_id, codigo)
    if not resultado:
        return jsonify({'error': 'Predio no encontrado', 'codigo': codigo}), 404

    # Serializar bounds (no la geometria completa)
    return jsonify({
        'codigo': resultado['codigo'],
        'bounds': resultado['bounds'],
        'layer': resultado['layer'],
        'attributes': resultado['attributes'],
        'total_matches': resultado['total_matches'],
    })


# --- API: Preview (PNG) ---
@atlas_bp.route('/api/municipios/<int:muni_id>/preview', methods=['GET'])
def api_render_preview(muni_id):
    # Parametros opcionales de extent
    minx = request.args.get('minx', type=float)
    miny = request.args.get('miny', type=float)
    maxx = request.args.get('maxx', type=float)
    maxy = request.args.get('maxy', type=float)
    codigo = request.args.get('codigo', '').strip()
    show_labels = request.args.get('labels', 'true').lower() == 'true'

    bounds = None
    if all(v is not None for v in [minx, miny, maxx, maxy]):
        bounds = (minx, miny, maxx, maxy)

    selected_geom = None
    selected_code = None
    if codigo:
        resultado = buscar_predio(muni_id, codigo)
        if resultado:
            selected_geom = resultado['geometry']
            selected_code = resultado['codigo']
            if bounds is None:
                bounds = resultado['bounds']

    # Layers habilitados
    layers_param = request.args.get('layers', '')
    enabled_layers = layers_param.split(',') if layers_param else None

    output = render_preview(
        municipio_id=muni_id,
        bounds=bounds,
        selected_geom=selected_geom,
        selected_code=selected_code,
        enabled_layers=enabled_layers,
        show_labels=show_labels,
    )

    if output is None:
        return jsonify({'error': 'No se pudo renderizar'}), 500

    return Response(output.getvalue(), mimetype='image/png')


# --- API: PDF ---
@atlas_bp.route('/api/municipios/<int:muni_id>/pdf', methods=['GET'])
def api_generate_pdf(muni_id):
    minx = request.args.get('minx', type=float)
    miny = request.args.get('miny', type=float)
    maxx = request.args.get('maxx', type=float)
    maxy = request.args.get('maxy', type=float)
    codigo = request.args.get('codigo', '').strip()
    page_size = request.args.get('size', 'carta').lower()
    show_labels = request.args.get('labels', 'true').lower() == 'true'

    bounds = None
    if all(v is not None for v in [minx, miny, maxx, maxy]):
        bounds = (minx, miny, maxx, maxy)

    selected_geom = None
    selected_code = None
    if codigo:
        resultado = buscar_predio(muni_id, codigo)
        if resultado:
            selected_geom = resultado['geometry']
            selected_code = resultado['codigo']
            if bounds is None:
                bounds = resultado['bounds']

    layers_param = request.args.get('layers', '')
    enabled_layers = layers_param.split(',') if layers_param else None

    if page_size not in ('carta', 'oficio', 'plotter'):
        page_size = 'carta'

    output = render_pdf(
        municipio_id=muni_id,
        bounds=bounds,
        selected_geom=selected_geom,
        selected_code=selected_code,
        page_size=page_size,
        enabled_layers=enabled_layers,
        show_labels=show_labels,
    )

    if output is None:
        return jsonify({'error': 'No se pudo generar PDF'}), 500

    filename = f"{(codigo or 'MAPA').strip()}.pdf"
    return Response(
        output.getvalue(),
        mimetype='application/pdf',
        headers={'Content-disposition': f'attachment; filename={filename}'}
    )


# --- API: Ir a coordenada ---
@atlas_bp.route('/api/municipios/<int:muni_id>/coordenada', methods=['GET'])
def api_ir_coordenada(muni_id):
    x = request.args.get('x', type=float)
    y = request.args.get('y', type=float)
    if x is None or y is None:
        return jsonify({'error': 'Se requieren coordenadas X e Y'}), 400

    # Generar preview centrada en la coordenada con un buffer fijo
    buffer = request.args.get('buffer', 100, type=float)
    bounds = (x - buffer, y - buffer, x + buffer, y + buffer)

    output = render_preview(
        municipio_id=muni_id,
        bounds=bounds,
        show_labels=True,
    )

    if output is None:
        return jsonify({'error': 'No se pudo renderizar en esa coordenada'}), 500

    return Response(output.getvalue(), mimetype='image/png')
