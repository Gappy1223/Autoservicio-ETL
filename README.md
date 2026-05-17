## 1. Resumen Ejecutivo y Arquitectura del Servicio (ITIL: Diseño del Servicio)

Este proyecto implementa una solución integral de **Ingeniería de Datos (ETL) y Analítica Avanzada** diseñada bajo principios de **DataOps** y alineada con las directrices de **ITIL v4**. Su propósito fundamental es transformar datos crudos transaccionales en formato XML (CFDI de ventas y compras) en activos de datos estructurados, de alta disponibilidad y con calidad certificada, optimizando la toma de decisiones críticas de negocio en estaciones de servicio (gasolineras).

### Capacidades del Sistema:
* **Gestión de Capacidad e Inventarios:** Monitoreo automatizado de los niveles de combustible en base a balances de masa (Inventario Inicial + Compras - Ventas).
* **Garantía de Continuidad del Servicio:** Identificación en tiempo real de desviaciones operativas críticas (Desbasto / Sobrellenado).
* **Analítica Predictiva y Detección de Anomalías:** Modelado de series temporales para predecir la demanda futura a 30 días e identificación matemática de comportamientos atípicos en los volúmenes transaccionales.

* ## 2. Gobierno de Datos y Calidad (DataOps & ITIL: Operación del Servicio)

El pipeline está diseñado bajo una filosofía de **cero tolerancia a la corrupción de datos** y **monitoreo proactivo de la salud del servicio**, implementando controles específicos para mitigar riesgos operativos:

### A. Idempotencia y Resiliencia (Garantía de Entrega)
* **Manejo de Duplicados:** Mediante una tabla de control (`control_xml`) y el uso estratégico de `ON CONFLICT (gasolinera_id, tipo_combustible_id, fecha) DO UPDATE` (Upsert), el sistema garantiza que la re-ejecución accidental o planificada de cualquier módulo no duplique registros ni altere la consistencia histórica.

### B. Gestión de Incidentes y Eventos (ITIL Event Management)
* **Monitoreo Transaccional Activo:** Cada módulo integra instrumentación nativa que reporta de manera estructurada al componente `log_etl`. Se capturan métricas críticas: tiempo de ejecución exacto, estatus (`exitoso`, `warning`, `error`), volumen de registros procesados y stack traces detallados en caso de excepciones.
* **Mecanismo de Heartbeat:** El sistema actualiza de manera constante la tabla `sistema_heartbeat`, permitiendo que herramientas externas de monitoreo (como Grafana o PagerDuty) detecten caídas del servicio (Silent Failures) instantáneamente.

### C. Clasificación Automatizada de Severidad de Alertas
El cálculo diario de inventarios categoriza el estado de la operación bajo umbrales matemáticos estrictos para activar workflows de mitigación específicos:
* **Crítico:** Balance de inventario negativo ($Inv_{final} < 0$).
* **Alerta:** Capacidad remanente inferior al 10% de la capacidad nominal de la estación ($Inv_{final} < Capacidad \times 0.10$).
* **Normal:** Operación estable dentro de los parámetros de control definidos.

---

## 3. Desglose Técnico de los Componentes (Pipeline Ingestion & Analytics)

El repositorio se estructura de forma modular, separando las responsabilidades de procesamiento de acuerdo con los estándares modernos de ingeniería de software y arquitectura Big Data:

### Módulos de Procesamiento (`/scripts`)

#### 1. `procesar_xmls.py` (Capa de Ingesta / Landing)
* **Tecnología:** `xml.etree.ElementTree` + `SQLAlchemy`.
* **Lógica:** Consume, valida y parsea archivos XML transaccionales correspondientes a CFDI 3.3 y 4.0 mediante mapeo dinámico de Namespaces. Realiza la estandarización e identificación de tipos de combustible (*Regular, Premium, Diesel*) a través de un diccionario de normalización robusto y segmenta temporalmente los registros en turnos operativos operativos (*Matutino, Vespertino, Nocturno*).

#### 2. `actualizar_agregaciones.py` (Capa de Consolidación / Silver)
* **Tecnología:** SQL ANSI Avanzado ejecutado mediante abstracción con `SQLAlchemy`.
* **Lógica:** Consolida el universo micro-transaccional de `fact_ventas` y `fact_compras` en dimensiones temporales agregadas (`agg_ventas_diarias`, `agg_compras_periodo` por semana ISO). Implementa funciones agregadas filtradas y cálculos de precios promedio ponderados para optimizar el rendimiento de las capas analíticas superiores.

#### 3. `calcular_inventario.py` (Capa de Balance / Gold)
* **Lógica:** Genera snapshots diarios correlacionando las entradas de compras y salidas de ventas con el inventario de cierre del día anterior. Delega la lógica de disparo de notificaciones inmediatas a un disparador a nivel de base de datos (`trg_verificar_inventario`) en PostgreSQL, aislando la lógica transaccional de la lógica de aplicación.

#### 4. `detectar_anomalias.py` (Machine Learning Interno - Control Estadístico de Procesos)
* **Tecnología:** `pandas` + `numpy`.
* **Métrica:** Implementa una metodología híbrida que combina el cálculo del **Z-Score** histórico de ventas por estación/combustible con el método del Rango Intercuartílico (**IQR**).
* **Severidad:** Clasifica las anomalías en tres niveles de criticidad de negocio según el grado de desviación estadística: `moderada` ($|Z| > 2.0$), `alta` ($|Z| > 3.0$), y `extrema` ($|Z| > 4.0$).

#### 5. `prediccion_demanda.py` (Capa Predictiva / Data Science)
* **Tecnología:** `Prophet` (Meta).
* **Lógica:** Desarrolla un modelo avanzado de series temporales de aditividad/multiplicatividad estacional para proyectar un horizonte de **30 días de demanda esperada**, generando adicionalmente intervalos de confianza estadísticos (límites mínimos y máximos esperados) para robustecer la planeación de compras y evitar costos excesivos de almacenamiento.

#### 6. `schema_adicional.sql` (Capa de Abstracción y Optimización)
* **Componentes:** Define vistas optimizadas para el consumo de capas de visualización (BI), tales como `vw_resumen_ejecutivo` (que resume los KPIs clave de los últimos 30 días, inventario y precios promedio) y `vw_anomalias_precio`.
* **Indexación:** Implementa índices estratégicos sobre columnas de alta cardinalidad y particionado implícito temporal (`fecha`, `fecha_inicio`, `estatus`) para garantizar tiempos de respuesta sub-segundo en ambientes de producción.

---

## 4. Orquestación y Automatización (ITIL: Transición del Servicio)

Para garantizar la predictibilidad y repetibilidad del servicio, el pipeline cuenta con dos capas de control de flujo:

### Orquestador Centralizado (`pipeline_completo.py`)
Es el punto de entrada unificado que encapsula la secuencia lógica de ejecución de los procesos con control parametrizado mediante banderas (`argparse`):
```bash
# Ejecución integral nocturna por lotes (Default)
python pipeline_completo.py

# Ejecución analítica pura omitiendo la ingesta de archivos físicos
python pipeline_completo.py --skip-xml --fecha-desde 2026-01-01 --fecha-hasta 2026-03-31
