"""
actualizar_agregaciones.py
Módulo 2 del pipeline ETL — Gasolineras
Consolida fact_ventas → agg_ventas_diarias y fact_compras → agg_compras_periodo
Idempotente: usa ON CONFLICT DO UPDATE, seguro de re-ejecutar.
"""

import os
import sys
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)


# ──────────────────────────────────────────────
# HELPER: registrar en log_etl
# ──────────────────────────────────────────────

def log_etl(proceso: str, fecha_inicio, fecha_fin, duracion: int,
            estatus: str, registros: int = 0, mensaje: str = ""):
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
        conn.execute(q, {
            "proceso": proceso,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "duracion": duracion,
            "estatus": estatus,
            "registros": registros,
            "mensaje": mensaje,
        })
        conn.commit()


# ──────────────────────────────────────────────
# MÓDULO: agg_ventas_diarias
# ──────────────────────────────────────────────

def actualizar_ventas_diarias(fecha_desde: date = None, fecha_hasta: date = None) -> int:
    """
    Agrega fact_ventas por (gasolinera, combustible, fecha) con desglose
    por turno e inserta/actualiza agg_ventas_diarias.

    Si no se especifican fechas, procesa todos los días con ventas.
    Retorna número de filas upserted.
    """
    filtro_fecha = ""
    params: dict = {}

    if fecha_desde:
        filtro_fecha += " AND v.fecha_operacion >= :fecha_desde"
        params["fecha_desde"] = fecha_desde
    if fecha_hasta:
        filtro_fecha += " AND v.fecha_operacion <= :fecha_hasta"
        params["fecha_hasta"] = fecha_hasta

    query_agg = text(f"""
        INSERT INTO agg_ventas_diarias (
            gasolinera_id, tipo_combustible_id, fecha,
            total_litros, total_importe, precio_promedio, num_transacciones,
            litros_turno_matutino, litros_turno_vespertino, litros_turno_nocturno,
            fecha_calculo
        )
        SELECT
            v.gasolinera_id,
            v.tipo_combustible_id,
            v.fecha_operacion                                               AS fecha,
            SUM(v.litros)                                                   AS total_litros,
            SUM(v.total)                                                    AS total_importe,
            ROUND(AVG(v.precio_unitario)::numeric, 4)                      AS precio_promedio,
            COUNT(*)                                                        AS num_transacciones,
            COALESCE(SUM(v.litros) FILTER (WHERE v.turno = 'Matutino'),  0) AS litros_matutino,
            COALESCE(SUM(v.litros) FILTER (WHERE v.turno = 'Vespertino'),0) AS litros_vespertino,
            COALESCE(SUM(v.litros) FILTER (WHERE v.turno = 'Nocturno'),  0) AS litros_nocturno,
            CURRENT_TIMESTAMP
        FROM fact_ventas v
        WHERE 1=1 {filtro_fecha}
        GROUP BY v.gasolinera_id, v.tipo_combustible_id, v.fecha_operacion
        ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha)
        DO UPDATE SET
            total_litros              = EXCLUDED.total_litros,
            total_importe             = EXCLUDED.total_importe,
            precio_promedio           = EXCLUDED.precio_promedio,
            num_transacciones         = EXCLUDED.num_transacciones,
            litros_turno_matutino     = EXCLUDED.litros_turno_matutino,
            litros_turno_vespertino   = EXCLUDED.litros_turno_vespertino,
            litros_turno_nocturno     = EXCLUDED.litros_turno_nocturno,
            fecha_calculo             = EXCLUDED.fecha_calculo
    """)

    with engine.connect() as conn:
        result = conn.execute(query_agg, params)
        conn.commit()
        return result.rowcount


# ──────────────────────────────────────────────
# MÓDULO: agg_compras_periodo (semana calendario)
# ──────────────────────────────────────────────

def actualizar_compras_periodo(fecha_desde: date = None, fecha_hasta: date = None) -> int:
    """
    Agrega fact_compras por semana ISO (lunes–domingo) por
    (gasolinera, combustible) e inserta/actualiza agg_compras_periodo.
    Retorna número de filas upserted.
    """
    filtro_fecha = ""
    params: dict = {}

    if fecha_desde:
        filtro_fecha += " AND c.fecha_operacion >= :fecha_desde"
        params["fecha_desde"] = fecha_desde
    if fecha_hasta:
        filtro_fecha += " AND c.fecha_operacion <= :fecha_hasta"
        params["fecha_hasta"] = fecha_hasta

    query_agg = text(f"""
        INSERT INTO agg_compras_periodo (
            gasolinera_id, tipo_combustible_id,
            fecha_inicio, fecha_fin,
            total_litros, total_costo, costo_promedio, num_compras,
            fecha_calculo
        )
        SELECT
            c.gasolinera_id,
            c.tipo_combustible_id,
            date_trunc('week', c.fecha_operacion)::date      AS fecha_inicio,
            (date_trunc('week', c.fecha_operacion) + INTERVAL '6 days')::date AS fecha_fin,
            SUM(c.litros)                                    AS total_litros,
            SUM(c.total)                                     AS total_costo,
            ROUND(AVG(c.costo_unitario)::numeric, 4)         AS costo_promedio,
            COUNT(*)                                         AS num_compras,
            CURRENT_TIMESTAMP
        FROM fact_compras c
        WHERE 1=1 {filtro_fecha}
        GROUP BY
            c.gasolinera_id,
            c.tipo_combustible_id,
            date_trunc('week', c.fecha_operacion)
        ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha_inicio, fecha_fin)
        DO UPDATE SET
            total_litros  = EXCLUDED.total_litros,
            total_costo   = EXCLUDED.total_costo,
            costo_promedio= EXCLUDED.costo_promedio,
            num_compras   = EXCLUDED.num_compras,
            fecha_calculo = EXCLUDED.fecha_calculo
    """)

    with engine.connect() as conn:
        result = conn.execute(query_agg, params)
        conn.commit()
        return result.rowcount


# ──────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────

def main(fecha_desde: date = None, fecha_hasta: date = None):
    from datetime import datetime
    inicio = datetime.now()
    print(f"\n{'='*55}")
    print(f"  ACTUALIZAR AGREGACIONES — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    total = 0

    # — Ventas diarias ——————————————————————————
    print("\n[1/2] Agregando ventas diarias...")
    try:
        n = actualizar_ventas_diarias(fecha_desde, fecha_hasta)
        print(f"      ✓  {n} filas upserted en agg_ventas_diarias")
        total += n
    except Exception as e:
        print(f"      ✗  Error: {e}")
        log_etl("actualizar_ventas_diarias", inicio, datetime.now(),
                int((datetime.now()-inicio).total_seconds()), "error", 0, str(e))
        raise

    # — Compras periodo ——————————————————————————
    print("\n[2/2] Agregando compras por semana...")
    try:
        n = actualizar_compras_periodo(fecha_desde, fecha_hasta)
        print(f"      ✓  {n} filas upserted en agg_compras_periodo")
        total += n
    except Exception as e:
        print(f"      ✗  Error: {e}")
        log_etl("actualizar_compras_periodo", inicio, datetime.now(),
                int((datetime.now()-inicio).total_seconds()), "error", 0, str(e))
        raise

    fin = datetime.now()
    duracion = int((fin - inicio).total_seconds())
    log_etl("actualizar_agregaciones", inicio, fin, duracion,
            "exitoso", total, f"ventas+compras: {total} filas")

    print(f"\n{'='*55}")
    print(f"  COMPLETADO en {duracion}s — {total} filas actualizadas")
    print(f"{'='*55}\n")
    return total


if __name__ == "__main__":
    # Uso: python actualizar_agregaciones.py [fecha_desde] [fecha_hasta]
    # Ejemplo: python actualizar_agregaciones.py 2025-01-01 2025-03-31
    fecha_desde = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    fecha_hasta = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else None
    main(fecha_desde, fecha_hasta)
