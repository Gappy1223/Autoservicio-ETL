
CREATE OR REPLACE VIEW vw_resumen_ejecutivo AS
SELECT
    g.nombre                                    AS gasolinera,
    tc.tipo                                     AS combustible,
    -- Ventas últimos 30 días
    COALESCE(v30.total_litros,  0)              AS litros_vendidos_30d,
    COALESCE(v30.total_importe, 0)              AS importe_ventas_30d,
    COALESCE(v30.num_tx,        0)              AS transacciones_30d,
    -- Compras últimos 30 días
    COALESCE(c30.total_litros,  0)              AS litros_comprados_30d,
    COALESCE(c30.total_costo,   0)              AS costo_compras_30d,
    -- Inventario actual
    COALESCE(inv.inventario_final, 0)           AS inventario_actual,
    inv.estatus                                  AS estatus_inventario,
    -- Precio promedio venta (últimos 7 días)
    ROUND(COALESCE(v7.precio_promedio, 0)::numeric, 4) AS precio_prom_litro_7d,
    CURRENT_TIMESTAMP                            AS calculado_en
FROM dim_gasolinera g
CROSS JOIN dim_tipo_combustible tc
-- Ventas 30d
LEFT JOIN (
    SELECT gasolinera_id, tipo_combustible_id,
           SUM(total_litros)  AS total_litros,
           SUM(total_importe) AS total_importe,
           SUM(num_transacciones) AS num_tx
    FROM agg_ventas_diarias
    WHERE fecha >= CURRENT_DATE - 30
    GROUP BY gasolinera_id, tipo_combustible_id
) v30 ON g.id = v30.gasolinera_id AND tc.id = v30.tipo_combustible_id
-- Compras 30d
LEFT JOIN (
    SELECT gasolinera_id, tipo_combustible_id,
           SUM(total_litros) AS total_litros,
           SUM(total_costo)  AS total_costo
    FROM agg_compras_periodo
    WHERE fecha_inicio >= CURRENT_DATE - 30
    GROUP BY gasolinera_id, tipo_combustible_id
) c30 ON g.id = c30.gasolinera_id AND tc.id = c30.tipo_combustible_id
-- Inventario más reciente
LEFT JOIN (
    SELECT DISTINCT ON (gasolinera_id, tipo_combustible_id)
           gasolinera_id, tipo_combustible_id, inventario_final, estatus
    FROM fact_inventario
    ORDER BY gasolinera_id, tipo_combustible_id, fecha DESC
) inv ON g.id = inv.gasolinera_id AND tc.id = inv.tipo_combustible_id
-- Precio prom 7d
LEFT JOIN (
    SELECT gasolinera_id, tipo_combustible_id,
           AVG(precio_promedio) AS precio_promedio
    FROM agg_ventas_diarias
    WHERE fecha >= CURRENT_DATE - 7
    GROUP BY gasolinera_id, tipo_combustible_id
) v7 ON g.id = v7.gasolinera_id AND tc.id = v7.tipo_combustible_id
WHERE g.activo = TRUE AND tc.activo = TRUE;


CREATE OR REPLACE VIEW vw_anomalias_precio AS
WITH stats AS (
    SELECT
        gasolinera_id,
        tipo_combustible_id,
        AVG(precio_promedio)    AS media,
        STDDEV(precio_promedio) AS sigma
    FROM agg_ventas_diarias
    GROUP BY gasolinera_id, tipo_combustible_id
)
SELECT
    g.nombre     AS gasolinera,
    tc.tipo      AS combustible,
    avd.fecha,
    avd.precio_promedio,
    ROUND(s.media::numeric, 4)  AS precio_esperado,
    ROUND(
        ABS(avd.precio_promedio - s.media) / NULLIF(s.sigma, 0)
    , 2)                         AS z_score
FROM agg_ventas_diarias avd
JOIN stats s USING (gasolinera_id, tipo_combustible_id)
JOIN dim_gasolinera       g  ON avd.gasolinera_id      = g.id
JOIN dim_tipo_combustible tc ON avd.tipo_combustible_id = tc.id
WHERE s.sigma > 0
  AND ABS(avd.precio_promedio - s.media) > 2 * s.sigma
ORDER BY z_score DESC;


CREATE INDEX IF NOT EXISTS idx_agg_ventas_fecha      ON agg_ventas_diarias(fecha);
CREATE INDEX IF NOT EXISTS idx_agg_compras_inicio    ON agg_compras_periodo(fecha_inicio);
CREATE INDEX IF NOT EXISTS idx_inventario_estatus    ON fact_inventario(estatus);
CREATE INDEX IF NOT EXISTS idx_alerta_severidad      ON alerta_inventario(severidad);

