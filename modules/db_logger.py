import sqlite3
import os
from datetime import datetime
from flask import request

# Ruta persistente según tus SOPs
DB_FOLDER = '/app/data'
DB_PATH = os.path.join(DB_FOLDER, 'igac_logs.db')

def init_db():
    """Inicializa la base de datos si no existe"""
    # Asegurar que la carpeta exista (aunque Docker se encarga, es doble seguridad)
    os.makedirs(DB_FOLDER, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Creamos tabla con: Fecha, IP, País, Ciudad, Dispositivo, Ruta visitada
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            ip_publica TEXT,
            pais TEXT,
            ciudad TEXT,
            user_agent TEXT,
            ruta TEXT,
            metodo TEXT
        )
    ''')
    conn.commit()
    conn.close()

def registrar_visita(ruta_actual):
    """Captura los datos de Cloudflare y los guarda"""
    try:
        # 1. Obtener IP Real (Cloudflare Tunnel)
        # Si no hay CF, usa la remota (fallback para desarrollo local)
        ip = request.headers.get('CF-Connecting-IP', request.remote_addr)
        
        # 2. Datos Geográficos (Headers gratuitos de Cloudflare)
        pais = request.headers.get('CF-IPCountry', 'Desconocido')
        ciudad = request.headers.get('CF-IPCity', 'N/A') # A veces requiere config extra en CF
        
        # 3. Datos del Navegador
        user_agent = request.headers.get('User-Agent', '')
        metodo = request.method

        # 4. Insertar en DB
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO visitas (timestamp, ip_publica, pais, ciudad, user_agent, ruta, metodo)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.now(), ip, pais, ciudad, user_agent, ruta_actual, metodo))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        # Si falla el log, no queremos tumbar la app, solo imprimimos el error
        print(f"Error guardando log: {e}")