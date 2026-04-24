"""
pipeline_completo.py
Orquestador del pipeline ETL — Gasolineras
Ejecuta en secuencia:
  1. procesar_xmls      (opcional, controlado por flag --skip-xml)
  2. actualizar_agregaciones
  3. calcular_inventario
Registra cada ejecución en log_etl y emite heartbeat final.
"""

import os
import sys
import argparse
from datetime import date, datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL)


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"


def banner(texto: str, color: str = CYAN):
    linea = "═" * 60
    print(f"\n{color}{BOLD}{linea}")
    print(f"  {texto}")
    print(f"{linea}{RESET}")


def step(n: int, total: int, texto: str):
    print(f"\n{BOLD}[{n}/{total}] {texto}{RESET}")


def ok(msg: str):
    print(f"  {GREEN}✓{RESET}  {msg}")


def warn(msg: str):
    print(f"  {YELLOW}⚠{RESET}  {msg}")


def error(msg: str):
    print(f"  {RED}✗{RESET}  {msg}")


def log_pipeline(inicio, fin, duracion, estatus, detalle=""):
    q = text("""
        INSERT INTO log_etl (
            proceso, fecha_inicio, fecha_fin, duracion_segundos,
            estatus, mensaje
        ) VALUES (
            'pipeline_completo', :inicio, :fin, :duracion, :estatus, :detalle
        )
    """)
    try:
        with engine.connect() as conn:
            conn.execute(q, dict(inicio=inicio, fin=fin, duracion=duracion,
                                 estatus=estatus, detalle=detalle))
            conn.commit()
    except Exception as e:
        warn(f"No se pudo escribir log_etl: {e}")


def heartbeat(proceso: str):
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
        pass


def verificar_conexion():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    ok("Conexión a Supabase exitosa")


# ──────────────────────────────────────────────
# PASOS DEL PIPELINE
# ──────────────────────────────────────────────

def paso_xml(carpetas: list[dict]):
    """
    carpetas: lista de dicts con keys 'ruta', 'tipo' ('venta'|'compra'),
              'gasolinera_id'
    """
    from procesar_xmls import procesar_carpeta

    stats = {"procesados": 0, "duplicados": 0, "errores": 0}
    for cfg in carpetas:
        print(f"    → {cfg['tipo'].upper():6s} | gasolinera {cfg['gasolinera_id']} | {cfg['ruta']}")
        procesar_carpeta(cfg["ruta"], tipo=cfg["tipo"],
                         gasolinera_id=cfg["gasolinera_id"])
    return stats


def paso_agregaciones(fecha_desde, fecha_hasta):
    from actualizar_agregaciones import main as run_agg
    return run_agg(fecha_desde, fecha_hasta)


def paso_inventario(fecha_desde, fecha_hasta, solo_movimiento):
    from calcular_inventario import main as run_inv
    run_inv(fecha_desde, fecha_hasta, solo_movimiento)


# ──────────────────────────────────────────────
# ORQUESTADOR PRINCIPAL
# ──────────────────────────────────────────────

def run_pipeline(args):
    inicio_total = datetime.now()
    errores_acumulados = []
    pasos_ok = []

    banner(f"PIPELINE ETL — GASOLINERAS   {inicio_total.strftime('%Y-%m-%d %H:%M')}")

    # 1. Verificar conexión
    try:
        verificar_conexion()
    except Exception as e:
        error(f"Sin conexión a la base de datos: {e}")
        sys.exit(1)

    total_pasos = 3 - (1 if args.skip_xml else 0)
    paso_actual = 0

    # ── Paso 1: Procesamiento XML ──────────────────
    if not args.skip_xml:
        paso_actual += 1
        step(paso_actual, total_pasos, "Procesamiento de XMLs")
        carpetas_cfg = [
            {"ruta": f"datos/Atlanta1/ventas",  "tipo": "venta",  "gasolinera_id": 1},
            {"ruta": f"datos/Atlanta1/compras", "tipo": "compra", "gasolinera_id": 1},
            # Agrega más gasolineras aquí cuando expandes a Fase 3:
            # {"ruta": "datos/Atlanta2/ventas",  "tipo": "venta",  "gasolinera_id": 2},
        ]
        try:
            paso_xml(carpetas_cfg)
            ok("XMLs procesados")
            pasos_ok.append("xml")
        except Exception as e:
            warn(f"Error en XMLs (continuando): {e}")
            errores_acumulados.append(f"xml: {e}")

    # ── Paso 2: Agregaciones ───────────────────────
    paso_actual += 1
    step(paso_actual, total_pasos, "Actualizar agregaciones")
    fecha_desde = date.fromisoformat(args.fecha_desde) if args.fecha_desde else None
    fecha_hasta = date.fromisoformat(args.fecha_hasta) if args.fecha_hasta else None

    try:
        n = paso_agregaciones(fecha_desde, fecha_hasta)
        ok(f"{n} filas upserted")
        pasos_ok.append("agregaciones")
    except Exception as e:
        error(f"Error en agregaciones: {e}")
        errores_acumulados.append(f"agregaciones: {e}")
        if not args.continuar_con_errores:
            raise

    # ── Paso 3: Inventario ─────────────────────────
    paso_actual += 1
    step(paso_actual, total_pasos, "Calcular inventario")
    try:
        paso_inventario(fecha_desde, fecha_hasta, args.solo_movimiento)
        ok("Snapshots de inventario generados")
        pasos_ok.append("inventario")
    except Exception as e:
        error(f"Error en inventario: {e}")
        errores_acumulados.append(f"inventario: {e}")
        if not args.continuar_con_errores:
            raise

    # ── Resumen ────────────────────────────────────
    fin_total = datetime.now()
    duracion  = int((fin_total - inicio_total).total_seconds())
    estatus   = "exitoso" if not errores_acumulados else "warning"

    heartbeat("pipeline_completo")
    log_pipeline(inicio_total, fin_total, duracion, estatus,
                 "; ".join(errores_acumulados) if errores_acumulados else "OK")

    banner(f"PIPELINE FINALIZADO en {duracion}s",
           color=(GREEN if not errores_acumulados else YELLOW))

    print(f"  Pasos completados : {', '.join(pasos_ok) or 'ninguno'}")
    if errores_acumulados:
        for e in errores_acumulados:
            warn(e)
    print()


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline ETL completo — Gasolineras",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python pipeline_completo.py                          # ejecución estándar nocturna
  python pipeline_completo.py --skip-xml               # solo agregaciones + inventario
  python pipeline_completo.py --fecha-desde 2025-01-01 --fecha-hasta 2025-03-31
  python pipeline_completo.py --skip-xml --solo-movimiento
        """
    )
    parser.add_argument("--skip-xml",        action="store_true",
                        help="Omitir el paso de procesamiento de XMLs")
    parser.add_argument("--solo-movimiento", action="store_true",
                        help="En inventario, solo procesar días con movimiento")
    parser.add_argument("--fecha-desde",     type=str, default=None,
                        metavar="YYYY-MM-DD",
                        help="Fecha de inicio del rango a procesar")
    parser.add_argument("--fecha-hasta",     type=str, default=None,
                        metavar="YYYY-MM-DD",
                        help="Fecha de fin del rango a procesar")
    parser.add_argument("--continuar-con-errores", action="store_true",
                        help="Continuar aunque un paso falle")

    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
