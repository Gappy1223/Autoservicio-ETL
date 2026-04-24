import os
import sys
import warnings
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL, connect_args={"client_encoding": "utf8"})

# ─────────────────────────────────────────────
# CONFIGURACION
# ─────────────────────────────────────────────

DIAS_PREDICCION    = 30
DIAS_HISTORICO_MIN = 60
INCLUIR_HISTORICO  = True
TAMANO_LOTE        = 100   # filas por commit


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def log_etl(proceso, fecha_inicio, fecha_fin, duracion, estatus, registros=0, mensaje=""):
    q = text("""
        INSERT INTO log_etl (
            proceso, fecha_inicio, fecha_fin, duracion_segundos,
            estatus, registros_insertados, mensaje
        ) VALUES (
            :proceso, :fecha_inicio, :fecha_fin, :duracion,
            :estatus, :registros, :mensaje
        )
    """)
    try:
        with engine.connect() as conn:
            conn.execute(q, dict(proceso=proceso, fecha_inicio=fecha_inicio,
                                 fecha_fin=fecha_fin, duracion=duracion,
                                 estatus=estatus, registros=registros,
                                 mensaje=mensaje))
            conn.commit()
    except Exception as e:
        print(f"  ! No se pudo escribir log_etl: {e}")


def obtener_gasolineras() -> list:
    q = text("SELECT id, nombre FROM dim_gasolinera WHERE activo = TRUE")
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [{"id": r[0], "nombre": r[1]} for r in rows]


def obtener_combustibles() -> list:
    q = text("SELECT id, tipo FROM dim_tipo_combustible WHERE activo = TRUE")
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [{"id": r[0], "tipo": r[1]} for r in rows]


def obtener_serie(gasolinera_id: int, combustible_id: int) -> pd.DataFrame:
    q = text("""
        SELECT fecha, total_litros
        FROM agg_ventas_diarias
        WHERE gasolinera_id       = :gid
          AND tipo_combustible_id = :cid
        ORDER BY fecha ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"gid": gasolinera_id, "cid": combustible_id})

    if df.empty:
        return df

    df = df.rename(columns={"fecha": "ds", "total_litros": "y"})
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = df["y"].astype(float)
    return df


def insertar_lote(filas: list):
    """
    Inserta filas de a TAMANO_LOTE con execute() individual por fila.
    Compatible con SQLAlchemy moderno, evita bug de encoding con executemany.
    """
    q = text("""
        INSERT INTO predicciones_demanda (
            gasolinera_id, tipo_combustible_id, fecha,
            litros_predichos, litros_minimo, litros_maximo,
            tipo, fecha_calculo
        ) VALUES (
            :gasolinera_id, :tipo_combustible_id, :fecha,
            :litros_predichos, :litros_minimo, :litros_maximo,
            :tipo, CURRENT_TIMESTAMP
        )
        ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha)
        DO UPDATE SET
            litros_predichos = EXCLUDED.litros_predichos,
            litros_minimo    = EXCLUDED.litros_minimo,
            litros_maximo    = EXCLUDED.litros_maximo,
            tipo             = EXCLUDED.tipo,
            fecha_calculo    = CURRENT_TIMESTAMP
    """)

    total      = len(filas)
    insertadas = 0

    for i in range(0, total, TAMANO_LOTE):
        lote = filas[i:i + TAMANO_LOTE]
        with engine.connect() as conn:
            for fila in lote:
                conn.execute(q, fila)
            conn.commit()
        insertadas += len(lote)
        print(f"    -> {insertadas}/{total} filas escritas", end="\r")

    print(f"    -> {insertadas}/{total} filas escritas")


# ─────────────────────────────────────────────
# ENTRENAMIENTO Y PREDICCION
# ─────────────────────────────────────────────

def entrenar_y_predecir(gasolinera: dict, combustible: dict) -> int:
    from prophet import Prophet

    label = f"{gasolinera['nombre']} | {combustible['tipo']}"
    df    = obtener_serie(gasolinera["id"], combustible["id"])

    if df.empty or len(df) < DIAS_HISTORICO_MIN:
        print(f"  -  {label} — datos insuficientes ({len(df)} dias), se omite")
        return 0

    print(f"  Entrenando: {label} ({len(df)} dias historicos)...")

    modelo = Prophet(
        changepoint_prior_scale=0.05,
        seasonality_mode="multiplicative",
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
    )
    modelo.add_seasonality(name="monthly", period=30.5, fourier_order=5)
    modelo.fit(df)

    print(f"  Generando predicciones...")
    futuro   = modelo.make_future_dataframe(periods=DIAS_PREDICCION, freq="D")
    forecast = modelo.predict(futuro)

    filas = []

    # Predicciones futuras
    for _, row in forecast[forecast["ds"] > df["ds"].max()].iterrows():
        filas.append({
            "gasolinera_id":       gasolinera["id"],
            "tipo_combustible_id": combustible["id"],
            "fecha":               row["ds"].date(),
            "litros_predichos":    round(max(float(row["yhat"]),       0.0), 2),
            "litros_minimo":       round(max(float(row["yhat_lower"]), 0.0), 2),
            "litros_maximo":       round(max(float(row["yhat_upper"]), 0.0), 2),
            "tipo":                "prediccion",
        })

    # Ajuste historico
    if INCLUIR_HISTORICO:
        for _, row in forecast[forecast["ds"] <= df["ds"].max()].iterrows():
            filas.append({
                "gasolinera_id":       gasolinera["id"],
                "tipo_combustible_id": combustible["id"],
                "fecha":               row["ds"].date(),
                "litros_predichos":    round(max(float(row["yhat"]),       0.0), 2),
                "litros_minimo":       round(max(float(row["yhat_lower"]), 0.0), 2),
                "litros_maximo":       round(max(float(row["yhat_upper"]), 0.0), 2),
                "tipo":                "historico",
            })

    print(f"  Escribiendo {len(filas)} filas en Supabase...")
    insertar_lote(filas)

    n_pred = len([f for f in filas if f["tipo"] == "prediccion"])
    print(f"  OK: {label} — {n_pred} dias predichos\n")
    return len(filas)


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def main():
    inicio = datetime.now()
    linea  = "=" * 60

    print(f"\n{linea}")
    print(f"  PREDICCION DE DEMANDA — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Horizonte: {DIAS_PREDICCION} dias")
    print(f"{linea}\n")

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("OK Conexion a Supabase exitosa\n")
    except Exception as e:
        print(f"Error de conexion: {e}")
        sys.exit(1)

    gasolineras  = obtener_gasolineras()
    combustibles = obtener_combustibles()
    total_filas  = 0
    errores      = 0

    for g in gasolineras:
        print(f"\n{'─'*60}")
        print(f"  Gasolinera: {g['nombre']}")
        print(f"{'─'*60}")
        for c in combustibles:
            try:
                n = entrenar_y_predecir(g, c)
                total_filas += n
            except Exception as e:
                print(f"  ERROR en {g['nombre']} | {c['tipo']}: {e}")
                errores += 1

    fin      = datetime.now()
    duracion = int((fin - inicio).total_seconds())
    estatus  = "exitoso" if errores == 0 else "warning"

    log_etl("prediccion_demanda", inicio, fin, duracion,
            estatus, total_filas,
            f"{total_filas} filas escritas, {errores} errores")

    print(f"\n{linea}")
    print(f"  COMPLETADO en {duracion}s")
    print(f"  Total filas escritas : {total_filas}")
    print(f"  Errores              : {errores}")
    print(f"{linea}\n")


if __name__ == "__main__":
    main()
