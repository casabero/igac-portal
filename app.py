from flask import Flask, render_template, request, send_file, flash, redirect, url_for
from modules.snc_processor import procesar_dataframe
from modules.db_logger import init_db, registrar_visita  # <--- IMPORTANTE
from modules.avaluo_analisis import procesar_incremento_web # <-- CAMBIÓ EL NOMBRE

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)