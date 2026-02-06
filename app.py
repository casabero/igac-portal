from flask import Flask, render_template, request, session
from modules.db_logger import init_db, registrar_visita

import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'casabero_igac_secure_key')

# --- Registrar Blueprints ---
from blueprints.tools import tools_bp
from blueprints.admin import admin_bp
from blueprints.atlas.routes import atlas_bp

app.register_blueprint(tools_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(atlas_bp)

# --- Inicializar DBs ---
try:
    init_db()
    print("Base de datos de logs inicializada correctamente.")
except Exception as e:
    print(f"Advertencia: No se pudo iniciar la DB de logs: {e}")

try:
    from blueprints.atlas.models import init_atlas_db
    init_atlas_db()
    print("Base de datos del Atlas inicializada correctamente.")
except Exception as e:
    print(f"Advertencia: No se pudo iniciar la DB del Atlas: {e}")


# --- Filtros de template ---
@app.template_filter('format_number')
def format_number(value):
    try:
        if value is None: return "0"
        return f"{value:,.0f}".replace(",", ".")
    except:
        return value


# --- RUTA HOME ---
@app.route('/')
def index():
    registrar_visita('/')
    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
