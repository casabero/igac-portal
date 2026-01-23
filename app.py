from flask import Flask, render_template, request, send_file, flash, redirect, url_for, session, Response
from modules.snc_processor import procesar_dataframe
from modules.db_logger import init_db, registrar_visita  # <--- IMPORTANTE
from modules.avaluo_analisis import procesar_incremento_web 
from modules.auditoria_maestra import procesar_auditoria, generar_pdf_auditoria
from modules.renumeracion_auditor import procesar_renumeracion, generar_excel_renumeracion, procesar_geografica, generar_pdf_renumeracion

import os
import uuid
import json
import traceback

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'casabero_igac_secure_key')

# Inicializar DB al arrancar la app
try:
    init_db()
    print("Base de datos de logs inicializada correctamente.")
except Exception as e:
    print(f"Advertencia: No se pudo iniciar la DB de logs: {e}")

@app.template_filter('format_number')
def format_number(value):
    try:
        if value is None: return "0"
        return f"{value:,.0f}".replace(",", ".")
    except:
        return value

# --- RUTA 1: HOME (PORTAL) ---
@app.route('/')
def index():
    # Registramos la visita
    registrar_visita('/')
    return render_template('index.html')

# --- RUTA 2: HERRAMIENTA SNC ---
@app.route('/snc', methods=['GET', 'POST'])
def snc_tool():
    # Registramos la visita también aquí para saber quién usa la herramienta
    registrar_visita('/snc')
    
    if request.method == 'POST':
        if 'archivo' not in request.files:
            flash('No se subió ningún archivo')
            return redirect(request.url)
            
        file = request.files['archivo']
        opcion = request.form.get('opcion')
        
        if file.filename == '':
            flash('Selecciona un archivo válido')
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

# --- RUTA OCULTA: VER LOGS (Opcional, para que pruebes rápido) ---
# Solo funcionará si visitas /ver-logs-secreto
@app.route('/ver-logs-secreto')
def ver_logs():
    import sqlite3
    conn = sqlite3.connect('/app/data/igac_logs.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM visitas ORDER BY id DESC LIMIT 50')
    logs = cursor.fetchall()
    conn.close()
    
    # HTML simple para ver los logs rápido
    html = "<h1>Últimas 50 Visitas</h1><table border='1'><tr><th>ID</th><th>Fecha</th><th>IP</th><th>País</th><th>Ruta</th><th>User Agent</th></tr>"
    for log in logs:
        html += f"<tr><td>{log[0]}</td><td>{log[1]}</td><td>{log[2]}</td><td>{log[3]}</td><td>{log[6]}</td><td>{log[5][:30]}...</td></tr>"
    html += "</table>"
    return html

# --- RUTA 4: ANALISIS AVALUOS ---
UPLOAD_FOLDER = 'temp_uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route('/avaluos', methods=['GET', 'POST'])
def avaluos_tool():
    registrar_visita('/avaluos')
    
    if request.method == 'POST':
        # Intentar obtener archivos de la sesión si no vienen en el request
        f_pre = request.files.get('file_pre')
        f_post = request.files.get('file_post')

        # Persistencia: Si se suben archivos nuevos, guardarlos en temp
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

        # Validar que tengamos archivos (ya sea por subida o por sesión)
        f_pre_final = session.get('path_pre')
        f_post_final = session.get('path_post')

        if not f_pre_final or not f_post_final:
            flash('Faltan archivos (Base o Sistema)')
            return redirect(request.url)
        
        # Get params
        def get_float_param(key):
             val = request.form.get(key, '')
             if not val: return 0.0
             try: return float(val)
             except: return 0.0

        pct_u = get_float_param('pct_urbano')
        pct_r = get_float_param('pct_rural')
        
        try:
            sample_pct = request.form.get('sample_pct', 100)
            zona_filter = request.form.get('zona_filter', 'TODOS')
            
            # procesar_incremento_web ya acepta paths o file objects
            resultados = procesar_incremento_web(f_pre_final, f_post_final, pct_u, pct_r, sample_pct=sample_pct, zona_filter=zona_filter)
            
            return render_template('avaluo_tool.html', resultados=resultados, session_data=session)
            
        except Exception as e:
            flash(f"Error en análisis: {str(e)}")
            return redirect(request.url)
            
    return render_template('avaluo_tool.html', resultados=None, session_data=session)

@app.route('/clear_analysis')
def clear_analysis():
    # Borrar archivos físicos
    for key in ['path_pre', 'path_post']:
        path = session.get(key)
        if path and os.path.exists(path):
            try: os.remove(path)
            except: pass
    
    # Limpiar sesión
    session.pop('path_pre', None)
    session.pop('name_pre', None)
    session.pop('path_post', None)
    session.pop('name_post', None)
    
    flash('Sesión y archivos temporales eliminados.')
    return redirect(url_for('avaluos_tool'))


# --- RUTA 5: AUDITORÍA DE CIERRE Y MASIVOS ---
@app.route('/auditoria', methods=['GET', 'POST'])
def auditoria_tool():
    registrar_visita('/auditoria')
    
    # Intentar recuperar resultados de la sesión si existen
    resultados = session.get('last_auditoria')
    
    if request.method == 'POST':
        f_prop = request.files.get('file_prop')
        f_calc = request.files.get('file_calc')
        incremento = request.form.get('incremento', 3)
        
        if not f_prop or not f_calc:
            flash('Se requieren ambos archivos para la auditoría.')
            return redirect(request.url)
            
        try:
            files_dict = {
                f_prop.filename: f_prop,
                f_calc.filename: f_calc
            }
            
            zona = request.form.get('zona', 'General')
            res = procesar_auditoria(files_dict, incremento, zona_filtro=zona)
            
            # Guardamos un identificador único para esta auditoría
            import uuid
            audit_id = str(uuid.uuid4())
            import json
            
            # Guardamos los resultados pesados en un archivo temporal
            audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
            with open(audit_path, 'w', encoding='utf-8') as f:
                json.dump(res, f, ensure_ascii=False)
            
            session['audit_id'] = audit_id
            
            return render_template('auditoria_tool.html', resultados=res)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            flash(f"Error procesando auditoría: {str(e)}")
            return redirect(request.url)
            
    # Intentar recuperar resultados del archivo temporal si existe
    audit_id = session.get('audit_id')
    resultados = None
    if audit_id:
        audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
        if os.path.exists(audit_path):
            try:
                with open(audit_path, 'r', encoding='utf-8') as f:
                    resultados = json.load(f)
                
                # Validación mínima: si es una versión vieja, la ignoramos
                if resultados and 'totales' in resultados and 'avaluo_precierre' not in resultados['totales']:
                    resultados = None
                    session.pop('audit_id', None)
            except:
                resultados = None
                session.pop('audit_id', None)
            
    return render_template('auditoria_tool.html', resultados=resultados)

@app.route('/auditoria/pdf')
def auditoria_pdf():
    audit_id = session.get('audit_id')
    if not audit_id:
        flash('No hay resultados para generar PDF. Ejecute la auditoría primero.')
        return redirect(url_for('auditoria_tool'))
    
    audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
    if not os.path.exists(audit_path):
        flash('La sesión de auditoría ha expirado o el archivo fue borrado.')
        return redirect(url_for('auditoria_tool'))
        
    try:
        with open(audit_path, 'r', encoding='utf-8') as f:
            resultados = json.load(f)
            
        # Optimización: No enviar full_data al generador de PDF para ahorrar memoria y tiempo
        if 'full_data' in resultados:
            del resultados['full_data']
            
        pdf_bytes = generar_pdf_auditoria(resultados)
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-disposition": "attachment; filename=Reporte_Auditoria.pdf"}
        )
    except Exception as e:
        print(f"Error PDF: {e}")
        flash(f"Error generando PDF. Es posible que los datos sean muy pesados.")
        return redirect(url_for('auditoria_tool'))

@app.route('/clear_auditoria')
def clear_auditoria():
    # Limpiar sesión y archivos de auditoría
    audit_id = session.get('audit_id')
    if audit_id:
        audit_path = os.path.join(UPLOAD_FOLDER, f"audit_{audit_id}.json")
        if os.path.exists(audit_path):
            try: os.remove(audit_path)
            except: pass
    
    session.pop('audit_id', None)
    session.pop('last_auditoria', None) # Limpiar el viejo por si acaso
    
    # Limpiar archivos temporales de ESTE usuario si los hubiera (en el futuro se pueden rastrear por session_id)
    # Por ahora limpiamos la carpeta general si es seguro, o solo las de la sesión actual.
    # Como la auditoría no guarda archivos permanentemente (usa streams), solo limpiamos sesión.
    
    # Adicionalmente limpiamos los de /avaluos por si acaso el usuario vino de allá
    for key in ['path_pre', 'path_post']:
        path = session.get(key)
        if path and os.path.exists(path):
            try: os.remove(path)
            except: pass
        session.pop(key, None)
    
    flash('Sesión y temporales limpiados correctamente.')
    return redirect(url_for('index'))

# --- RUTA 6: AUDITORÍA DE RENUMERACIÓN ---
@app.route('/renumeracion', methods=['GET', 'POST'])
def renumeracion_tool():
    registrar_visita('/renumeracion')
    
    # Intentar recuperar resultados de la sesión (id del archivo json)
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
        tipo = request.form.get('tipo', '1') # 1: CICA, 2: LC
        
        if not file or file.filename == '':
            flash('Seleccione un archivo Excel válido.')
            return redirect(request.url)
            
        try:
            res = procesar_renumeracion(file, tipo)
            
            # --- FASE 2: GEOGRÁFICA (Opcional) ---
            gdb_f = request.files.get('archivo_gdb_formal')
            gdb_i = request.files.get('archivo_gdb_informal')
            
            if gdb_f or gdb_i:
                # set_alfa_activos es un set de CODIGO_SNC del diccionario de estados que son 'ACTIVO'
                set_activos = {k for k, v in res['diccionario_estados'].items() if v == 'ACTIVO'}
                errores_geo, logs_geo = procesar_geografica(gdb_f, gdb_i, set_activos, res['diccionario_estados'], res['df_referencia'])
                res['errores_geo'] = errores_geo
                res['logs_geo'] = logs_geo
            else:
                res['errores_geo'] = []
                res['logs_geo'] = []

            # Limpiar dataframe de referencia para no saturar JSON si es muy grande (opcional)
            # res.pop('df_referencia', None)
            # res.pop('diccionario_estados', None)
            
            # Guardar resultados pesados en disco
            new_id = str(uuid.uuid4())
            path = os.path.join(UPLOAD_FOLDER, f"renum_{new_id}.json")
            with open(path, 'w', encoding='utf-8') as f:
                # Convertimos a serie si es necesario o manejamos el error de serialización
                # En este caso errores_geo es una lista de dicts, es serializable.
                json.dump(res, f, ensure_ascii=False, default=str)
            
            session['renum_audit_id'] = new_id
            return render_template('renumeracion_tool.html', resultados=res, tipo_config=tipo)
            
        except Exception as e:
            traceback.print_exc()
            flash(f"Error: {str(e)}")
            return redirect(request.url)
            
    return render_template('renumeracion_tool.html', resultados=resultados)

@app.route('/renumeracion/excel')
def renumeracion_excel():
    audit_id = session.get('renum_audit_id')
    if not audit_id:
        flash('No hay auditoría activa.')
        return redirect(url_for('renumeracion_tool'))
    
    path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
    if not os.path.exists(path):
        flash('La sesión ha expirado.')
        return redirect(url_for('renumeracion_tool'))
        
    try:
        with open(path, 'r', encoding='utf-8') as f:
            res = json.load(f)
            
        output = generar_excel_renumeracion(res['errores'], res.get('errores_geo'))
        return send_file(
            output,
            as_attachment=True,
            download_name="REPORTE_RENUMERACION_CONSOLIDADO.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f"Error al generar Excel: {str(e)}")
        return redirect(url_for('renumeracion_tool'))

@app.route('/renumeracion/pdf')
def renumeracion_pdf():
    audit_id = session.get('renum_audit_id')
    if not audit_id:
        flash('No hay auditoría activa.')
        return redirect(url_for('renumeracion_tool'))
    
    path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
    if not os.path.exists(path):
        flash('La sesión ha expirado.')
        return redirect(url_for('renumeracion_tool'))
        
    try:
        with open(path, 'r', encoding='utf-8') as f:
            res = json.load(f)
            
        pdf_bytes = generar_pdf_renumeracion(res)
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-disposition": "attachment; filename=Reporte_Ejecutivo_Renumeracion.pdf"}
        )
    except Exception as e:
        flash(f"Error al generar PDF: {str(e)}")
        return redirect(url_for('renumeracion_tool'))

@app.route('/clear_renumeracion')
def clear_renumeracion():
    audit_id = session.get('renum_audit_id')
    if audit_id:
        path = os.path.join(UPLOAD_FOLDER, f"renum_{audit_id}.json")
        if os.path.exists(path):
            try: os.remove(path)
            except: pass
    session.pop('renum_audit_id', None)
    flash('Sesión de renumeración limpiada.')
    return redirect(url_for('renumeracion_tool'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)