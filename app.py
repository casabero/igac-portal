from flask import Flask, render_template, request, send_file, flash, redirect, url_for
from modules.snc_processor import procesar_dataframe
from modules.db_logger import init_db, registrar_visita  # <--- IMPORTANTE
from modules.avaluo_analisis import procesar_incremento_web # <-- CAMBIÓ EL NOMBRE
from modules.auditoria_maestra import procesar_auditoria, generar_pdf_auditoria

import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'casabero_igac_secure_key')

# Inicializar DB al arrancar la app
try:
    init_db()
    print("Base de datos de logs inicializada correctamente.")
except Exception as e:
    print(f"Advertencia: No se pudo iniciar la DB de logs: {e}")

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
    from flask import session
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
    from flask import session
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
    from flask import session
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
            # Procesar (Usamos streams directamente)
            # Nota: procesar_auditoria espera un dict de filename: stream
            files_dict = {
                f_prop.filename: f_prop,
                f_calc.filename: f_calc
            }
            
            res = procesar_auditoria(files_dict, incremento)
            
            # Guardamos en sesión (sin el df de pandas ni el pdf, solo lo serializable)
            session['last_auditoria'] = {
                'stats_zonas': res['stats_zonas'],
                'resumen_estados': res['resumen_estados'],
                'inconsistencias': res['inconsistencias'],
                'total_predios': res['total_predios'],
                'pct_incremento': res['pct_incremento']
            }
            # Guardamos el DF original en un objeto temporal para el PDF (opcional, o re-procesamos)
            # Para simplificar y no llenar la RAM/Sesión, el PDF lo generaremos re-procesando si es necesario, 
            # pero por ahora pasamos los datos serializados.
            
            return render_template('auditoria_tool.html', resultados=session['last_auditoria'])
            
        except Exception as e:
            flash(f"Error procesando auditoría: {str(e)}")
            return redirect(request.url)
            
    return render_template('auditoria_tool.html', resultados=resultados)

@app.route('/auditoria/pdf')
def auditoria_pdf():
    from flask import session, Response
    resultados = session.get('last_auditoria')
    if not resultados:
        flash('No hay resultados para generar PDF. Ejecute la auditoría primero.')
        return redirect(url_for('auditoria_tool'))
    
    try:
        pdf_bytes = generar_pdf_auditoria(resultados)
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-disposition": "attachment; filename=Reporte_Auditoria.pdf"}
        )
    except Exception as e:
        flash(f"Error generando PDF: {str(e)}")
        return redirect(url_for('auditoria_tool'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)