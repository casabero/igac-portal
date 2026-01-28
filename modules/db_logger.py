import sqlite3
import os
from datetime import datetime, timezone, timedelta
from flask import request

# Ruta persistente según tus SOPs
DB_FOLDER = '/app/data'
DB_PATH = os.path.join(DB_FOLDER, 'igac_logs.db')

def init_db():
    """Inicializa la base de datos si no existe y asegura las columnas nuevas"""
    # Asegurar que la carpeta exista (aunque Docker se encarga, es doble seguridad)
    os.makedirs(DB_FOLDER, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabla extendida y enriquecida
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            ip_publica TEXT,
            pais TEXT,
            ciudad TEXT,
            user_agent TEXT,
            ruta TEXT,
            metodo TEXT,
            referer TEXT,
            session_id TEXT,
            dispositivo TEXT,
            os TEXT,
            navegador TEXT,
            resolucion TEXT
        )
    ''')
    
    # Migraciones simples
    columnas = {
        "referer": "TEXT",
        "session_id": "TEXT",
        "dispositivo": "TEXT",
        "os": "TEXT",
        "navegador": "TEXT",
        "resolucion": "TEXT"
    }
    
    for col, tipo in columnas.items():
        try:
            cursor.execute(f"ALTER TABLE visitas ADD COLUMN {col} {tipo}")
        except:
            pass

    conn.commit()
    conn.close()

def registrar_visita(ruta_actual):
    """Captura los datos de Cloudflare, sesión y hardware, y los guarda"""
    try:
        from flask import session
        from user_agents import parse
        
        # 1. Obtener IP Real
        ip = request.headers.get('CF-Connecting-IP', request.remote_addr)
        
        # 2. Datos Geográficos
        pais = request.headers.get('CF-IPCountry', 'Desconocido')
        ciudad = request.headers.get('CF-IPCity', 'N/A')
        
        # 3. Datos del Navegador y Hardware
        ua_string = request.headers.get('User-Agent', '')
        user_agent = parse(ua_string)
        
        dispositivo = "PC" if user_agent.is_pc else "Móvil" if user_agent.is_mobile else "Tablet" if user_agent.is_tablet else "Otro"
        os_info = f"{user_agent.os.family} {user_agent.os.version_string}"
        browser_info = f"{user_agent.browser.family} {user_agent.browser.version_string}"
        
        # Resolución (Viene de parámetros GET o Cookies)
        resolucion = request.args.get('res') or request.cookies.get('screen_res', 'N/A')
        
        metodo = request.method
        referer = request.headers.get('Referer', '')
        
        # 4. ID de Sesión
        if 'tracking_id' not in session:
            import uuid
            session['tracking_id'] = str(uuid.uuid4())
        tracking_id = session['tracking_id']

        # 5. Insertar en DB
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO visitas (timestamp, ip_publica, pais, ciudad, user_agent, ruta, metodo, referer, session_id, dispositivo, os, navegador, resolucion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.now(timezone(timedelta(hours=-5))), ip, pais, ciudad, ua_string, ruta_actual, metodo, referer, tracking_id, dispositivo, os_info, browser_info, resolucion))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        # Si falla el log, no queremos tumbar la app, solo imprimimos el error
        print(f"Error guardando log: {e}")