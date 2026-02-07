"""Modelo de datos para el Atlas GIS. SQLite para metadata de departamentos/municipios."""

import sqlite3
import os
import json
from datetime import datetime, timezone, timedelta

COL_TZ = timezone(timedelta(hours=-5))
DATA_DIR = os.environ.get('ATLAS_DATA_DIR', '/app/data/geodata')
DB_PATH = os.path.join(os.path.dirname(DATA_DIR), 'atlas.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_atlas_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS departamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT UNIQUE NOT NULL,
            codigo TEXT UNIQUE,
            fecha_creacion TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS municipios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            departamento_id INTEGER NOT NULL REFERENCES departamentos(id) ON DELETE CASCADE,
            nombre TEXT NOT NULL,
            codigo TEXT,
            gpkg_path TEXT,
            srs TEXT,
            capas_disponibles TEXT,
            fecha_carga TEXT,
            fecha_actualizacion TEXT,
            fecha_version TEXT,
            UNIQUE(departamento_id, nombre)
        );
    """)
    # Migration: Add fecha_version if not exists
    try:
        conn.execute("ALTER TABLE municipios ADD COLUMN fecha_version TEXT")
        conn.commit()
    except:
        pass  # Column already exists
    conn.commit()
    conn.close()


def now_col():
    return datetime.now(COL_TZ).strftime('%Y-%m-%d %H:%M:%S')


# --- Departamentos ---
def crear_departamento(nombre, codigo=None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO departamentos (nombre, codigo, fecha_creacion) VALUES (?, ?, ?)",
            (nombre.strip().upper(), codigo, now_col())
        )
        conn.commit()
        return conn.execute("SELECT * FROM departamentos WHERE nombre=?", (nombre.strip().upper(),)).fetchone()
    finally:
        conn.close()


def listar_departamentos():
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM departamentos ORDER BY nombre").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def obtener_departamento(dep_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM departamentos WHERE id=?", (dep_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def eliminar_departamento(dep_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM departamentos WHERE id=?", (dep_id,))
        conn.commit()
    finally:
        conn.close()


# --- Municipios ---
def crear_municipio(departamento_id, nombre, codigo=None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO municipios (departamento_id, nombre, codigo) VALUES (?, ?, ?)",
            (departamento_id, nombre.strip().upper(), codigo)
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM municipios WHERE departamento_id=? AND nombre=?",
            (departamento_id, nombre.strip().upper())
        ).fetchone()
    finally:
        conn.close()


def listar_municipios(departamento_id):
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM municipios WHERE departamento_id=? ORDER BY nombre",
            (departamento_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def obtener_municipio(muni_id):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM municipios WHERE id=?", (muni_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def actualizar_municipio_gpkg(muni_id, gpkg_path, srs, capas, fecha_version=None):
    conn = get_db()
    try:
        conn.execute(
            """UPDATE municipios SET gpkg_path=?, srs=?, capas_disponibles=?,
               fecha_carga=?, fecha_actualizacion=?, fecha_version=? WHERE id=?""",
            (gpkg_path, srs, json.dumps(capas), now_col(), now_col(), fecha_version, muni_id)
        )
        conn.commit()
    finally:
        conn.close()


def eliminar_municipio(muni_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM municipios WHERE id=?", (muni_id,))
        conn.commit()
    finally:
        conn.close()


def obtener_municipio_completo(muni_id):
    """Retorna municipio con su departamento."""
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT m.*, d.nombre as dep_nombre, d.codigo as dep_codigo
            FROM municipios m
            JOIN departamentos d ON m.departamento_id = d.id
            WHERE m.id=?
        """, (muni_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
