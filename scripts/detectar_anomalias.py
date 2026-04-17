import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DATABASE_URL, connect_args={"client_encoding": "utf8"})

#UMBRAL Z-SCORE PARA DETECTAR ANOMALÍAS
UMBRAL_MODERADO = 2.0
UMBRAL_ALTA = 3.0
UMBRAL_EXTREMA = 4.0
TAMANO_LOTE = 50

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
 
    df["total_litros"] = df["total_litros"].astype(float)
    return df

def clasificar_severidad(z: float)->str:
    az = abs(z)
    if az > UMBRAL_EXTREMA:
        return "extrema"
    elif az > UMBRAL_ALTA:
        return "alta"
    else:
        return "moderada"
    
def insertar_anomalias(filas: list):
    if not filas:
        return
    q = text("""
        INSERT INTO anomalias_ventas (
            gasolinera_id, tipo_combustible_id, fecha,
            litros_real, litros_media, litros_stddev,
            z_score, tipo_anomalia, severidad, fecha_calculo
        ) VALUES (
            :gasolinera_id, :tipo_combustible_id, :fecha,
            :litros_real, :litros_media, :litros_stddev,
            :z_score, :tipo_anomalia, :severidad, CURRENT_TIMESTAMP
        )
        ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha)
        DO UPDATE SET
            litros_real    = EXCLUDED.litros_real,
            litros_media   = EXCLUDED.litros_media,
            litros_stddev  = EXCLUDED.litros_stddev,
            z_score        = EXCLUDED.z_score,
            tipo_anomalia  = EXCLUDED.tipo_anomalia,
            severidad      = EXCLUDED.severidad,
            fecha_calculo  = CURRENT_TIMESTAMP
        """)
    total = len(filas)
    insertadas = 0
    for i in range(0, total, TAMANO_LOTE):
        lote = filas[i:i+TAMANO_LOTE]
        with engine.connect() as conn:
            for fila in lote:
                conn.execute(q, fila)
            conn.commit()
        insertadas += len(lote)
        print(f"  > Insertadas/Actualizadas {insertadas}/{total} anomalias", end="\r")
    print(f"  > Insertadas/Actualizadas {insertadas}/{total} anomalias")

def detectar_anomalias(gasolinera: dict, combustible: dict) -> int:
    label = f"{gasolinera.get('nombre', '?')} | {combustible.get('tipo', '?')}"
    
    try:
        df = obtener_serie(gasolinera["id"], combustible["id"])

        if df.empty or len(df) < 30:
            print(f"  -  {label} — datos insuficientes ({len(df)} dias), se omite")
            return 0

        serie = df["total_litros"]
        media  = serie.mean()
        stddev = serie.std()

        if stddev == 0:
            print(f"  -  {label} — desviacion estandar cero, se omite")
            return 0

        df["z_score"] = (serie - media) / stddev

        q1  = serie.quantile(0.25)
        q3  = serie.quantile(0.75)
        iqr = q3 - q1
        lim_inferior = q1 - 1.5 * iqr
        lim_superior = q3 + 1.5 * iqr

        df["es_outlier_iqr"] = (serie < lim_inferior) | (serie > lim_superior)

        mask = (df["z_score"].abs() >= UMBRAL_MODERADO) & (df["es_outlier_iqr"])
        anomalias_df = df[mask].copy()

        if anomalias_df.empty:
            print(f"  OK {label} — sin anomalias detectadas")
            return 0

        filas = []
        for _, row in anomalias_df.iterrows():
            z = float(row["z_score"])
            filas.append({
                "gasolinera_id":       gasolinera["id"],
                "tipo_combustible_id": combustible["id"],
                "fecha":               row["fecha"],
                "litros_real":         round(float(row["total_litros"]), 2),
                "litros_media":        round(float(media), 2),
                "litros_stddev":       round(float(stddev), 2),
                "z_score":             round(z, 4),
                "tipo_anomalia":       "alta" if z > 0 else "baja",
                "severidad":           clasificar_severidad(z),
            })

        print(f"  {label} — {len(filas)} anomalias detectadas")

        sev = anomalias_df["z_score"].abs()
        n_extrema  = int((sev > UMBRAL_EXTREMA).sum())
        n_alta     = int(((sev > UMBRAL_ALTA) & (sev <= UMBRAL_EXTREMA)).sum())
        n_moderada = int(((sev >= UMBRAL_MODERADO) & (sev <= UMBRAL_ALTA)).sum())
        print(f"    Extrema: {n_extrema}  Alta: {n_alta}  Moderada: {n_moderada}")

        insertar_anomalias(filas)
        return len(filas)

    except Exception as e:
        print(f"  ERROR en {label}: {e}")
        return 0

def main():
    inicio = datetime.now()
    linea = "="*60
    print(f"\n{linea}")
    print(f"  DETECCION DE ANOMALIAS — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Umbral Z-score: moderada={UMBRAL_MODERADO} alta={UMBRAL_ALTA} extrema={UMBRAL_EXTREMA}")
    print(f"{linea}\n")

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  > Conexión a base de datos exitosa\n")
    except Exception as e:
        print(f"  ! Error de conexión a base de datos: {e}")
        sys.exit(1)

    gasolineras = obtener_gasolineras()
    combustibles = obtener_combustibles()
    total_filas = 0
    errores = 0
    for g in gasolineras:
        print(f"\n{'─'*60}")
        print(f"  Gasolinera: {g['nombre']}")
        print(f"{'─'*60}")
        for c in combustibles:
            try:
                n = detectar_anomalias(g, c)
                total_filas += n
            except Exception as e:
                print(f"  ERROR en {g['nombre']} | {c['tipo']}: {e}")
                errores += 1
    fin= datetime.now()
    duracion = int((fin-inicio).total_seconds())
    estatus = "exitoso" if errores == 0 else "warning"
    log_etl("detectar_anomalias", inicio, fin, duracion, estatus, total_filas,f"{total_filas} anomalias escritas, {errores} errores")

    print(f"\n{linea}")
    print(f"  COMPLETADO en {duracion}s")
    print(f"  Total anomalias detectadas : {total_filas}")
    print(f"  Errores                    : {errores}")
    print(f"{linea}\n")
 
 
if __name__ == "__main__":
    main()
