"""Blueprint: Panel de administración y analytics."""

import sqlite3

from flask import Blueprint, render_template, request, session, flash, redirect, url_for, Response
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash

from modules.db_logger import DB_PATH

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

DEFAULT_ADMIN_USER = "casabero"
DEFAULT_ADMIN_PASS = "casabe123"


def ensure_admin_user():
    """Garantiza la existencia del usuario admin por defecto en SQLite."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute("SELECT id FROM admin_users WHERE username=?", (DEFAULT_ADMIN_USER,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(
                "INSERT INTO admin_users (username, password_hash) VALUES (?, ?)",
                (DEFAULT_ADMIN_USER, generate_password_hash(DEFAULT_ADMIN_PASS)),
            )
        conn.commit()
    finally:
        conn.close()


def validar_admin(username, password):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT password_hash FROM admin_users WHERE username=?", (username,))
        row = cursor.fetchone()
        return bool(row and check_password_hash(row[0], password))
    finally:
        conn.close()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin.admin_login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


@admin_bp.route('/login', methods=['GET', 'POST'])
def admin_login():
    ensure_admin_user()
    login_error = None
    username_value = ''

    if request.method == 'POST':
        user = request.form.get('username', '').strip().lower()
        pw = request.form.get('password')
        username_value = user
        if validar_admin(user, pw):
            session['admin_logged_in'] = True
            session['admin_username'] = user
            flash('SIS_ACCESO_CONCEDIDO // BIENVENIDO_ADMIN_ROOT', 'success')
            return redirect(url_for('admin.admin_dashboard'))

        login_error = 'Credenciales inválidas. Verifique usuario y contraseña e intente de nuevo.'
        flash('SIS_ACCESO_DENEGADO // TOKEN_INVÁLIDO_BLOQUEADO', 'danger')

    return render_template('admin_login.html', login_error=login_error, username_value=username_value)


@admin_bp.route('/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    session.pop('admin_username', None)
    flash('SIS_SESIÓN_TERMINADA // REINICIO_CONEXIÓN', 'info')
    return redirect(url_for('index'))


@admin_bp.route('/dashboard')
@login_required
def admin_dashboard():
    import sqlite3
    from modules.db_logger import DB_PATH
    fecha_inicio = request.args.get('inicio', '')
    fecha_fin = request.args.get('fin', '')
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    query_base = " FROM visitas WHERE 1=1"
    params = []
    if fecha_inicio:
        query_base += " AND timestamp >= ?"
        params.append(f"{fecha_inicio} 00:00:00")
    if fecha_fin:
        query_base += " AND timestamp <= ?"
        params.append(f"{fecha_fin} 23:59:59")
    cursor.execute(f"SELECT COUNT(*) as total {query_base}", params)
    total_visitas = cursor.fetchone()['total']
    cursor.execute(f"SELECT COUNT(DISTINCT session_id) as total {query_base}", params)
    visitantes_unicos = cursor.fetchone()['total']
    cursor.execute(f"SELECT ruta, COUNT(*) as count {query_base} AND ruta != '/admin/dashboard' GROUP BY ruta ORDER BY count DESC LIMIT 5", params)
    top_apps = [dict(row) for row in cursor.fetchall()]
    cursor.execute(f"SELECT pais, COUNT(*) as count {query_base} GROUP BY pais ORDER BY count DESC LIMIT 5", params)
    stats_pais = [dict(row) for row in cursor.fetchall()]
    cursor.execute(f"SELECT * {query_base} ORDER BY timestamp DESC LIMIT 100", params)
    ultimos_logs = [dict(row) for row in cursor.fetchall()]
    conn.close()

    chart_data = {row['ruta']: row['count'] for row in top_apps}
    modulo_top = top_apps[0]['ruta'] if top_apps else 'N/A'
    logs = [
        {
            'modulo': row.get('ruta', 'N/A'),
            'accion': row.get('metodo', 'GET'),
            'timestamp': row.get('timestamp', ''),
            'ip': row.get('ip_publica', 'N/A'),
            'user_agent': row.get('user_agent', ''),
        }
        for row in ultimos_logs
    ]

    return render_template(
        'admin_dashboard.html',
        total_visitas=total_visitas,
        visitantes_unicos=visitantes_unicos,
        top_apps=top_apps,
        stats_pais=stats_pais,
        ultimos_logs=ultimos_logs,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        total_logs=total_visitas,
        usuarios_unicos=visitantes_unicos,
        modulo_top=modulo_top,
        logs=logs,
        chart_data=chart_data,
    )


@admin_bp.route('/export-csv')
@login_required
def export_logs_csv():
    import sqlite3
    import csv
    import io
    from modules.db_logger import DB_PATH
    fecha_inicio = request.args.get('inicio', '')
    fecha_fin = request.args.get('fin', '')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT * FROM visitas WHERE 1=1"
    params = []
    if fecha_inicio:
        query += " AND timestamp >= ?"
        params.append(f"{fecha_inicio} 00:00:00")
    if fecha_fin:
        query += " AND timestamp <= ?"
        params.append(f"{fecha_fin} 23:59:59")
    query += " ORDER BY timestamp DESC"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(column_names)
    writer.writerows(rows)
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=log_visitas_igac.csv"})
