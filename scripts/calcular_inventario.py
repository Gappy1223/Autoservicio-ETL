"""
calcular_inventario.py
Módulo 3 del pipeline ETL — Gasolineras
Genera snapshots diarios en fact_inventario.
El trigger trg_verificar_inventario en PostgreSQL dispara alertas automáticamente.
"""

import os
import sys
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)


# ──────────────────────────────────────────────
# HELPER: log_etl + heartbeat
# ──────────────────────────────────────────────

def log_etl(proceso, fecha_inicio, fecha_fin, duracion, estatus,
            registros=0, mensaje=""):
    q = text("""
        INSERT INTO log_etl (
            proceso, fecha_inicio, fecha_fin, duracion_segundos,
            estatus, registros_insertados, mensaje
        ) VALUES (
            :proceso, :fecha_inicio, :fecha_fin, :duracion,
            :estatus, :registros, :mensaje
        )
    """)
    with engine.connect() as conn:
        conn.execute(q, dict(proceso=proceso, fecha_inicio=fecha_inicio,
                             fecha_fin=fecha_fin, duracion=duracion,
                             estatus=estatus, registros=registros,
                             mensaje=mensaje))
        conn.commit()


def heartbeat(proceso: str):
    """Actualiza o inserta un heartbeat para monitoreo."""
    q = text("""
        INSERT INTO sistema_heartbeat (proceso, ultima_ejecucion, estatus)
        VALUES (:proceso, CURRENT_TIMESTAMP, 'activo')
        ON CONFLICT (proceso)
        DO UPDATE SET ultima_ejecucion = CURRENT_TIMESTAMP, estatus = 'activo'
    """)
    try:
        with engine.connect() as conn:
            conn.execute(q, {"proceso": proceso})
            conn.commit()
    except Exception:
        pass   # heartbeat no debe bloquear el proceso principal


# ──────────────────────────────────────────────
# HELPERS DE CONSULTA
# ──────────────────────────────────────────────

def obtener_gasolineras_activas() -> list[dict]:
    q = text("SELECT id, nombre, capacidad_total FROM dim_gasolinera WHERE activo = TRUE")
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [{"id": r[0], "nombre": r[1], "capacidad": float(r[2] or 0)} for r in rows]


def obtener_combustibles_activos() -> list[dict]:
    q = text("SELECT id, tipo FROM dim_tipo_combustible WHERE activo = TRUE")
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [{"id": r[0], "tipo": r[1]} for r in rows]


def obtener_inventario_inicial(gasolinera_id: int, combustible_id: int,
                               fecha: date) -> float:
    """
    Devuelve el inventario_final del día anterior como inventario_inicial.
    Si no existe registro previo, retorna 0.
    """
    q = text("""
        SELECT inventario_final
        FROM fact_inventario
        WHERE gasolinera_id      = :gid
          AND tipo_combustible_id = :cid
          AND fecha < :fecha
        ORDER BY fecha DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"gid": gasolinera_id, "cid": combustible_id,
                               "fecha": fecha}).fetchone()
    return float(row[0]) if row else 0.0


def obtener_compras_dia(gasolinera_id: int, combustible_id: int,
                        fecha: date) -> float:
    q = text("""
        SELECT COALESCE(SUM(litros), 0)
        FROM fact_compras
        WHERE gasolinera_id      = :gid
          AND tipo_combustible_id = :cid
          AND fecha_operacion    = :fecha
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"gid": gasolinera_id, "cid": combustible_id,
                               "fecha": fecha}).fetchone()
    return float(row[0]) if row else 0.0


def obtener_ventas_dia(gasolinera_id: int, combustible_id: int,
                       fecha: date) -> float:
    q = text("""
        SELECT COALESCE(SUM(litros), 0)
        FROM fact_ventas
        WHERE gasolinera_id      = :gid
          AND tipo_combustible_id = :cid
          AND fecha_operacion    = :fecha
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"gid": gasolinera_id, "cid": combustible_id,
                               "fecha": fecha}).fetchone()
    return float(row[0]) if row else 0.0


def hay_movimiento_en_dia(gasolinera_id: int, combustible_id: int,
                          fecha: date) -> bool:
    """Retorna True si hubo al menos una venta o compra ese día."""
    ventas  = obtener_ventas_dia(gasolinera_id, combustible_id, fecha)
    compras = obtener_compras_dia(gasolinera_id, combustible_id, fecha)
    return (ventas + compras) > 0


# ──────────────────────────────────────────────
# INSERCIÓN DEL SNAPSHOT
# ──────────────────────────────────────────────

def insertar_snapshot(gasolinera_id: int, combustible_id: int,
                      fecha: date, inv_inicial: float,
                      compras: float, ventas: float,
                      capacidad: float) -> str:
    """
    Inserta o actualiza el snapshot en fact_inventario.
    El trigger de PostgreSQL evaluará alertas automáticamente.
    Retorna 'insertado' | 'actualizado'.
    """
    inv_final = inv_inicial + compras - ventas
    porcentaje = round((inv_final / capacidad * 100), 2) if capacidad > 0 else None

    if inv_final < 0:
        estatus = "Critico"
    elif capacidad > 0 and inv_final < capacidad * 0.10:
        estatus = "Alerta"
    else:
        estatus = "Normal"

    q = text("""
        INSERT INTO fact_inventario (
            gasolinera_id, tipo_combustible_id, fecha,
            inventario_inicial, entradas_compras, salidas_ventas,
            inventario_final, porcentaje_diferencia, estatus
        ) VALUES (
            :gid, :cid, :fecha,
            :inv_inicial, :compras, :ventas,
            :inv_final, :porcentaje, :estatus
        )
        ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha)
        DO UPDATE SET
            inventario_inicial    = EXCLUDED.inventario_inicial,
            entradas_compras      = EXCLUDED.entradas_compras,
            salidas_ventas        = EXCLUDED.salidas_ventas,
            inventario_final      = EXCLUDED.inventario_final,
            porcentaje_diferencia = EXCLUDED.porcentaje_diferencia,
            estatus               = EXCLUDED.estatus,
            fecha_calculo         = CURRENT_TIMESTAMP
        RETURNING xmax  -- 0 = insert, >0 = update
    """)

    with engine.connect() as conn:
        row = conn.execute(q, {
            "gid": gasolinera_id, "cid": combustible_id, "fecha": fecha,
            "inv_inicial": inv_inicial, "compras": compras, "ventas": ventas,
            "inv_final": inv_final, "porcentaje": porcentaje, "estatus": estatus
        }).fetchone()
        conn.commit()

    return "insertado" if (row and row[0] == 0) else "actualizado"


# ──────────────────────────────────────────────
# RANGO DE FECHAS A PROCESAR
# ──────────────────────────────────────────────

def obtener_rango_fechas() -> tuple[date, date]:
    """
    Detecta automáticamente el rango de fechas con transacciones.
    """
    q = text("""
        SELECT
            LEAST(
                (SELECT MIN(fecha_operacion) FROM fact_ventas),
                (SELECT MIN(fecha_operacion) FROM fact_compras)
            ) AS fecha_min,
            GREATEST(
                (SELECT MAX(fecha_operacion) FROM fact_ventas),
                (SELECT MAX(fecha_operacion) FROM fact_compras)
            ) AS fecha_max
    """)
    with engine.connect() as conn:
        row = conn.execute(q).fetchone()

    if not row or not row[0]:
        raise RuntimeError("No hay transacciones en fact_ventas ni fact_compras.")

    return row[0], row[1]


# ──────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ──────────────────────────────────────────────

def calcular_inventario(fecha_desde: date = None,
                        fecha_hasta: date = None,
                        solo_con_movimiento: bool = False) -> int:
    """
    Genera snapshots diarios de inventario para cada combinación
    gasolinera × combustible en el rango de fechas indicado.

    Parámetros:
        fecha_desde          — fecha de inicio (detecta automáticamente si None)
        fecha_hasta          — fecha de fin (detecta automáticamente si None)
        solo_con_movimiento  — si True, omite días sin ventas ni compras
    """
    if not fecha_desde or not fecha_hasta:
        auto_desde, auto_hasta = obtener_rango_fechas()
        fecha_desde = fecha_desde or auto_desde
        fecha_hasta = fecha_hasta or auto_hasta

    gasolineras  = obtener_gasolineras_activas()
    combustibles = obtener_combustibles_activos()

    n_dias   = (fecha_hasta - fecha_desde).days + 1
    combos   = len(gasolineras) * len(combustibles)
    total_op = n_dias * combos

    print(f"  Rango     : {fecha_desde} → {fecha_hasta}  ({n_dias} días)")
    print(f"  Gasolineras: {len(gasolineras)}  |  Combustibles: {len(combustibles)}")
    print(f"  Snapshots potenciales: {total_op}")
    if solo_con_movimiento:
        print("  Modo: solo días con movimiento")

    snapshots = 0
    dia = fecha_desde

    while dia <= fecha_hasta:
        for g in gasolineras:
            for c in combustibles:
                if solo_con_movimiento and not hay_movimiento_en_dia(g["id"], c["id"], dia):
                    continue

                compras   = obtener_compras_dia(g["id"], c["id"], dia)
                ventas    = obtener_ventas_dia(g["id"], c["id"], dia)
                inv_ini   = obtener_inventario_inicial(g["id"], c["id"], dia)

                accion = insertar_snapshot(
                    g["id"], c["id"], dia,
                    inv_ini, compras, ventas, g["capacidad"]
                )
                snapshots += 1

                inv_final = inv_ini + compras - ventas
                estado = "⚠️" if inv_final < 0 else ("🔶" if inv_final < g["capacidad"] * 0.10 else "✓")
                print(f"    {estado} {dia} | {g['nombre'][:20]:20s} | {c['tipo']:8s} "
                      f"| ini={inv_ini:,.0f} +{compras:,.0f} -{ventas:,.0f} = {inv_final:,.0f}L "
                      f"[{accion}]")

        dia += timedelta(days=1)

    return snapshots


# ──────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────

def main(fecha_desde: date = None, fecha_hasta: date = None,
         solo_con_movimiento: bool = False):
    inicio = datetime.now()
    print(f"\n{'='*65}")
    print(f"  CALCULAR INVENTARIO — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*65}")

    try:
        snapshots = calcular_inventario(fecha_desde, fecha_hasta, solo_con_movimiento)
        fin       = datetime.now()
        duracion  = int((fin - inicio).total_seconds())

        log_etl("calcular_inventario", inicio, fin, duracion,
                "exitoso", snapshots, f"{snapshots} snapshots generados")
        heartbeat("calcular_inventario")

        print(f"\n{'='*65}")
        print(f"  COMPLETADO en {duracion}s — {snapshots} snapshots generados")
        print(f"  Trigger PostgreSQL evaluó alertas automáticamente.")
        print(f"{'='*65}\n")

    except Exception as e:
        fin      = datetime.now()
        duracion = int((fin - inicio).total_seconds())
        print(f"\n✗ ERROR: {e}")
        log_etl("calcular_inventario", inicio, fin, duracion, "error", 0, str(e))
        raise


if __name__ == "__main__":
    # Uso: python calcular_inventario.py [fecha_desde] [fecha_hasta] [--solo-movimiento]
    # Ejemplo: python calcular_inventario.py 2025-01-01 2025-03-31
    # Sin argumentos: detecta rango automáticamente.
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = sys.argv[1:]

    fd = date.fromisoformat(args[0]) if len(args) > 0 else None
    fh = date.fromisoformat(args[1]) if len(args) > 1 else None
    solo = "--solo-movimiento" in flags

    main(fd, fh, solo)
