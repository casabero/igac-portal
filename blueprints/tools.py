"""Blueprint: Herramientas existentes del portal (SNC, Avaluos, Auditoria, Renumeracion, GIS)."""

from flask import Blueprint, render_template, request, send_file, flash, redirect, url_for, session, Response, jsonify
from modules.snc_processor import procesar_dataframe
from modules.db_logger import registrar_visita
from modules.avaluo_analisis import procesar_incremento_web
from modules.auditoria_maestra import procesar_auditoria, generar_pdf_auditoria
from modules.renumeracion_auditor import procesar_renumeracion, generar_excel_renumeracion, procesar_geografica, generar_pdf_renumeracion
from modules.renumeracion_informales import procesar_informales
from modules.gis_converter import process_gdb_conversion

import pandas as pd
import os
import uuid
import json
import traceback

tools_bp = Blueprint('tools', __name__)

UPLOAD_FOLDER = 'temp_uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


# --- SNC ---
@tools_bp.route('/snc', methods=['GET', 'POST'])
def snc_tool():
    registrar_visita('/snc')
    if request.method == 'POST':
        if 'archivo' not in request.files:
            flash('ERROR_SISTEMA :: ARCHIVO_REQUERIDO_PARA_PROCESO')
            return redirect(request.url)
        file = request.files['archivo']
        opcion = request.form.get('opcion')
        if file.filename == '':
            flash('ERROR_FLUJO :: FORMATO_NO_ADMITIDO (REQUERIDO: TXT/PRN/EXCEL/CSV)')
            return redirect(request.url)
        if file and opcion:
            try:
                output_stream, new_filename = procesar_dataframe(file, opcion, file.filename)
                return send_file(
                    output_stream,
                    as_attachment=True,
                    download_name=new_filename,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            except Exception as e:
                flash(f"Error al procesar: {str(e)}")
                return redirect(request.url)
    return render_template('snc_tool.html')


# --- AVALUOS ---
@tools_bp.route('/avaluos', methods=['GET', 'POST'])
def avaluos_tool():
    registrar_visita('/avaluos')
    if request.method == 'POST':
        f_pre = request.files.get('file_pre')
        f_post = request.files.get('file_post')
        if f_pre and f_pre.filename:
            path_pre = os.path.join(UPLOAD_FOLDER, f"pre_{session.sid if hasattr(session, 'sid') else 'user'}_{f_pre.filename}")
            f_pre.save(path_pre)
            session['path_pre'] = path_pre
            session['name_pre'] = f_pre.filename
        if f_post and f_post.filename:
            path_post = os.path.join(UPLOAD_FOLDER, f"post_{session.sid if hasattr(session, 'sid') else 'user'}_{f_post.filename}")
            f_post.save(path_post)
            session['path_post'] = path_post
            session['name_post'] = f_post.filename
        f_pre_final = session.get('path_pre')
        f_post_final = session.get('path_post')
        if not f_pre_final or not f_post_final:
            flash('Debe cargar ambos archivos (Base y Sistema) para realizar la comparación.')
            return redirect(request.url)
        def get_float_param(key):
             val = request.form.get(key, '')
             try: return float(val) if val else 0.0
             except: return 0.0
        pct_u = get_float_param('pct_urbano')
        pct_r = get_float_param('pct_rural')
        try:
            sample_pct = request.form.get('sample_pct', 100)
            zona_filter = request.form.get('zona_filter', 'TODOS')
            resultados = procesar_incremento_web(f_pre_final, f_post_final, pct_u, pct_r, sample_pct=sample_pct, zona_filter=zona_filter)
            return render_template('avaluo_tool.html', resultados=resultados, session_data=session)
        except Exception as e:
            flash(f"Error en análisis: {str(e)}")
            return redirect(request.url)
    return render_template('avaluo_tool.html', resultados=None, session_data=session)

@tools_bp.route('/clear_analysis')
def clear_analysis():
    for key in ['path_pre', 'path_post']:
        path = session.get(key)
        if path and os.path.exists(path):
            try: os.remove(path)
            except: pass
    session.pop('path_pre', None)
    session.pop('name_pre', None)
    session.pop('path_post', None)
    session.pop('name_post', None)
    flash('RES_BUFFER_DEPURADO // DATOS_SESIÓN_BORRADOS')
    return redirect(url_for('tools.avaluos_tool'))


# --- AUDITORIA ---
@tools_bp.route('/auditoria', methods=['GET', 'POST'])
def auditoria_tool():
    registrar_visita('/auditoria')
    if request.method == 'POST':
        f_prop = request.files.get('file_prop')
        f_calc = request.files.get('file_calc')
        incremento = request.form.get('incremento', 3)
        if not f_prop or not f_calc:
            flash('Se requieren ambos archivos (Propietarios y Listado) para la auditoría.')
            return redirect(request.url)
        try:
            files_dict = {f_prop.filename: f_prop, f_calc.filename: f_calc}
            zona = request.form.get('zona', 'General')
            res = procesar_auditoria(files_dict, incremento, zona_filtro=zona)
            audit_id = str(uuid.uuid4())
            audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
            with open(audit_path, 'w', encoding='utf-8') as f:
                json.dump(res, f, ensure_ascii=False)
            session['audit_id'] = audit_id
            return render_template('auditoria_tool.html', resultados=res)
        except Exception as e:
            traceback.print_exc()
            flash(f"Error procesando auditoría: {str(e)}")
            return redirect(request.url)
    audit_id = session.get('audit_id')
    resultados = None
    if audit_id:
        audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
        if os.path.exists(audit_path):
            try:
                with open(audit_path, 'r', encoding='utf-8') as f:
                    resultados = json.load(f)
                if resultados and 'totales' in resultados and 'avaluo_precierre' not in resultados['totales']:
                    resultados = None
                    session.pop('audit_id', None)
            except:
                resultados = None
                session.pop('audit_id', None)
    return render_template('auditoria_tool.html', resultados=resultados)

@tools_bp.route('/auditoria/pdf')
def auditoria_pdf():
    audit_id = session.get('audit_id')
    if not audit_id:
        flash('No hay resultados para generar PDF. Ejecute la auditoría primero.')
        return redirect(url_for('tools.auditoria_tool'))
    audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
    if not os.path.exists(audit_path):
        flash('La sesión de auditoría ha expirado o el archivo fue borrado.')
        return redirect(url_for('tools.auditoria_tool'))
    try:
        with open(audit_path, 'r', encoding='utf-8') as f:
            resultados = json.load(f)
        if 'full_data' in resultados: del resultados['full_data']
        pdf_bytes = generar_pdf_auditoria(resultados)
        return Response(pdf_bytes, mimetype="application/pdf", headers={"Content-disposition": "attachment; filename=Reporte_Auditoria.pdf"})
    except Exception as e:
        flash(f"Error generando PDF.")
        return redirect(url_for('tools.auditoria_tool'))

@tools_bp.route('/clear_auditoria')
def clear_auditoria():
    audit_id = session.get('audit_id')
    if audit_id:
        audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
        if os.path.exists(audit_path):
            try: os.remove(audit_path)
            except: pass
    session.pop('audit_id', None)
    session.pop('last_auditoria', None)
    for key in ['path_pre', 'path_post']:
        path = session.get(key)
        if path and os.path.exists(path):
            try: os.remove(path)
            except: pass
        session.pop(key, None)
    flash('SIS_AUDITORIA_CACHE_DEPURADO // BUFFER_REINICIADO')
    return redirect(url_for('tools.auditoria_tool'))


# --- RENUMERACION ---
@tools_bp.route('/renumeracion/detectar-columnas', methods=['POST'])
def detectar_columnas_renumeracion():
    if 'file' not in request.files: return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({'error': 'No selected file'}), 400
    if file:
        try:
            df = pd.read_excel(file, nrows=5)
            columnas = df.columns.tolist()
            return jsonify({'columnas': columnas})
        except Exception as e:
            return jsonify({'error': f'Error al leer el archivo: {str(e)}'}), 500
    return jsonify({'error': 'Unexpected error'}), 500

@tools_bp.route('/renumeracion', methods=['GET', 'POST'])
def renumeracion_tool():
    registrar_visita('/renumeracion')
    audit_id = session.get('renum_audit_id')
    resultados = None
    if audit_id:
        path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    resultados = json.load(f)
            except:
                session.pop('renum_audit_id', None)
    if request.method == 'POST':
        file = request.files.get('archivo_excel')
        tipo = request.form.get('tipo', '1')
        fase = request.form.get('fase', '1')
        col_snc = request.form.get('col_snc')
        col_ant = request.form.get('col_ant')
        col_estado = request.form.get('col_estado')
        if not file or file.filename == '':
            flash('Seleccione el archivo de reporte (Excel) para continuar.')
            return redirect(request.url)
        try:
            res = procesar_renumeracion(file, tipo, col_snc_manual=col_snc, col_ant_manual=col_ant, col_estado_manual=col_estado)
            res['fase_ejecutada'] = int(fase)
            if fase == '2':
                gdb_f = request.files.get('archivo_gdb_formal')
                gdb_i = request.files.get('archivo_gdb_informal')
                if not gdb_f and not gdb_i:
                    flash('Para la Fase 2 (Geográfica) debe subir al menos una GDB (.zip).')
                    return redirect(request.url)
                set_activos = {k for k, v in res['diccionario_estados'].items() if v == 'ACTIVO'}
                errores_geo, logs_geo = procesar_geografica(gdb_f, gdb_i, set_activos, res['diccionario_estados'], res['df_referencia'])
                res['errores_geo'], res['logs_geo'] = errores_geo, logs_geo
            else:
                res['errores_geo'], res['logs_geo'] = [], {}
            new_id = str(uuid.uuid4())
            path = os.path.join(UPLOAD_FOLDER, f"renum_{new_id}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(res, f, ensure_ascii=False, default=str)
            session['renum_audit_id'] = new_id
            return render_template('renumeracion_tool.html', resultados=res, tipo_config=tipo)
        except Exception as e:
            traceback.print_exc()
            flash(f"Error: {str(e)}")
            return redirect(request.url)
    return render_template('renumeracion_tool.html', resultados=resultados)

@tools_bp.route('/renumeracion/excel')
def renumeracion_excel():
    audit_id = session.get('renum_audit_id')
    if not audit_id: return redirect(url_for('tools.renumeracion_tool'))
    path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
    if not os.path.exists(path): return redirect(url_for('tools.renumeracion_tool'))
    try:
        with open(path, 'r', encoding='utf-8') as f: res = json.load(f)
        output = generar_excel_renumeracion(res['errores'], res.get('errores_geo'), fase=res.get('fase_ejecutada', 1))
        return send_file(output, as_attachment=True, download_name="REPORTE_RENUMERACION.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        flash(f"Error al generar Excel.")
        return redirect(url_for('tools.renumeracion_tool'))

@tools_bp.route('/renumeracion/pdf')
def renumeracion_pdf():
    audit_id = session.get('renum_audit_id')
    if not audit_id: return redirect(url_for('tools.renumeracion_tool'))
    path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
    if not os.path.exists(path): return redirect(url_for('tools.renumeracion_tool'))
    try:
        with open(path, 'r', encoding='utf-8') as f: res = json.load(f)
        pdf_bytes = generar_pdf_renumeracion(res)
        return Response(pdf_bytes, mimetype="application/pdf", headers={"Content-disposition": "attachment; filename=Reporte_Renumeracion.pdf"})
    except Exception as e:
        flash(f"Error al generar PDF.")
        return redirect(url_for('tools.renumeracion_tool'))

@tools_bp.route('/clear_renumeracion')
def clear_renumeracion():
    audit_id = session.get('renum_audit_id')
    if audit_id:
        path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
        if os.path.exists(path):
            try: os.remove(path)
            except: pass
    session.pop('renum_audit_id', None)
    flash('Sesión de renumeración limpiada.')
    return redirect(url_for('tools.renumeracion_tool'))


# --- INFORMALES ---
@tools_bp.route('/renumeracion-informales', methods=['GET', 'POST'])
def informales_tool():
    registrar_visita('/renumeracion-informales')
    res = session.get('res_informales')
    if request.method == 'POST':
        try:
            files_map = {}
            for key in ['file_informal', 'file_formal']:
                f = request.files.get(key)
                if f and f.filename:
                    path = os.path.join(UPLOAD_FOLDER, f"{uuid.uuid4().hex}_{f.filename}")
                    f.save(path)
                    files_map[key] = path
                else: files_map[key] = None
            prefijo = request.form.get('prefijo', '200000')
            if not any(files_map.values()):
                flash('Debe subir al menos un archivo ZIP.')
                return redirect(request.url)
            resultado = procesar_informales(files_map, UPLOAD_FOLDER, prefijo)
            if resultado['status'] != 'error':
                session['res_informales'] = resultado
                return render_template('informales_tool.html', resultados=resultado)
            flash(f"Error: {resultado.get('message')}")
        except Exception as e:
            flash(f"Error crítico: {str(e)}")
            return redirect(request.url)
    return render_template('informales_tool.html', resultados=res)

@tools_bp.route('/download-informales/<filename>')
def download_informales_zip(filename):
    path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(path): return send_file(path, as_attachment=True)
    flash('Archivo no encontrado.')
    return redirect(url_for('tools.informales_tool'))

@tools_bp.route('/clear_informales')
def clear_informales():
    res = session.get('res_informales')
    if res and 'zip_path' in res:
        try: os.remove(res['zip_path'])
        except: pass
    session.pop('res_informales', None)
    flash('Resultados borrados.')
    return redirect(url_for('tools.informales_tool'))


# --- GIS CONVERTER ---
@tools_bp.route('/gis/gdb-gpkg', methods=['GET', 'POST'])
def gis_converter_tool():
    registrar_visita('/gis/gdb-gpkg')
    if request.method == 'POST':
        if 'archivo_zip' not in request.files:
            flash('No se seleccionó ningún archivo.')
            return redirect(request.url)
        file = request.files['archivo_zip']
        if file.filename == '':
            flash('Nombre de archivo vacío.')
            return redirect(request.url)
        if file and (file.filename.endswith('.zip') or file.filename.endswith('.rar')):
            try:
                temp_filename = f"upload_{uuid.uuid4().hex}.zip"
                temp_path = os.path.join(UPLOAD_FOLDER, temp_filename)
                file.save(temp_path)
                try:
                    output_gpkg = process_gdb_conversion(temp_path, UPLOAD_FOLDER)
                    return send_file(
                        output_gpkg,
                        as_attachment=True,
                        download_name=os.path.basename(output_gpkg)
                    )
                finally:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
            except Exception as e:
                flash(f"Error en la conversión: {str(e)}")
                return redirect(request.url)
        else:
            flash('Por favor suba un archivo .zip')
            return redirect(request.url)
    return render_template('gis_converter_tool.html')
