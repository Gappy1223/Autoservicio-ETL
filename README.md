# Autoservicio-ETL 🛢️
### Inteligencia Operativa y Analítica Avanzada para Cadenas de Gasolineras

> **Pipeline ETL end-to-end** que transforma facturas fiscales XML (CFDI) en inteligencia operativa accionable: inventario en tiempo real, predicción de demanda con IA y detección automática de anomalías.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Supabase](https://img.shields.io/badge/Supabase-Cloud-green?logo=supabase)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=powerbi)
![Prophet](https://img.shields.io/badge/Prophet-Meta%20ML-orange)
![Status](https://img.shields.io/badge/Status-Producción-success)

---

## 📋 Tabla de Contenidos

1. [Resumen Ejecutivo](#resumen-ejecutivo)
2. [El Problema de Negocio](#el-problema-de-negocio)
3. [Arquitectura de la Solución](#arquitectura-de-la-solución)
4. [Pipeline ETL — Módulos](#pipeline-etl--módulos)
5. [Inteligencia Artificial y Machine Learning](#inteligencia-artificial-y-machine-learning)
6. [Modelo de Datos](#modelo-de-datos)
7. [Sistema de Alertas](#sistema-de-alertas)
8. [Dashboard Power BI](#dashboard-power-bi)
9. [Resultados e Impacto](#resultados-e-impacto)
10. [Instalación y Configuración](#instalación-y-configuración)
11. [Ejecución del Pipeline](#ejecución-del-pipeline)
12. [Gestión de Incidencias Resueltas](#gestión-de-incidencias-resueltas)
13. [Hoja de Ruta](#hoja-de-ruta)
14. [Estándares y Metodología](#estándares-y-metodología)
15. [Autor](#autor)

---

## Resumen Ejecutivo

**Autoservicio-ETL** es un sistema de datos empresarial construido para la cadena de gasolineras **Atlanta**. Procesa de forma automatizada los Comprobantes Fiscales Digitales por Internet (CFDI) emitidos y recibidos en formato XML para calcular el inventario de combustible en tiempo real, predecir la demanda a 30 días y detectar anomalías operativas, todo sin intervención manual.

| Métrica | Valor |
|---|---|
| Ventas gestionadas | **$139.2M MXN** en el período auditado |
| Litros rastreados | **7.08M litros** de combustible |
| Facturas procesadas | **+200,000 CFDIs** (versiones 3.3 y 4.0) |
| Anomalías detectadas | **215** eventos identificados automáticamente |
| Margen bruto calculado | **8.6%** sobre datos reales |
| Reducción en captura | **90%** menos tiempo de trabajo manual |
| Snapshots de inventario | **5,478** registros diarios generados |
| Predicciones generadas | **5,394 filas** a 30 días por combinación gasolinera × combustible |

---

## El Problema de Negocio

La cadena de gasolineras Atlanta operaba con un control de inventario completamente manual, lo que generaba cuatro ineficiencias críticas:

| Problema | Impacto Medido |
|---|---|
| Desabasto detectado tardíamente | Hasta **30 días** de retraso en la detección de faltantes |
| Reportes consolidados a mano | **4 horas semanales por gerente** dedicadas a Excel |
| Compras sin proyección | Decisiones reactivas cuando el inventario ya estaba crítico |
| Sin benchmarking interno | Cero visibilidad comparada entre las 3 gasolineras |
| Anomalías no detectadas | Fugas, errores de facturación o fraudes identificados semanas después |

**Objetivo:** Transformar datos fiscales CFDI en inteligencia operativa accionable en tiempo casi real.

**Fórmula central del sistema:**
```
Inventario_Actual = Inventario_Inicial + Σ Compras (CFDI recibidos) − Σ Ventas (CFDI emitidos)
```
*Calculado por cada combinación: Gasolinera × Tipo de Combustible × Día.*

---

## Arquitectura de la Solución

El sistema se estructura en **cuatro capas desacopladas**, siguiendo principios de arquitectura limpia y separación de responsabilidades:

```
┌─────────────────────────────────────────────────────────┐
│                   CAPA DE INTELIGENCIA                  │
│         Prophet (Meta) · Z-Score · IQR · DAX            │
├─────────────────────────────────────────────────────────┤
│                  CAPA DE PRESENTACIÓN                   │
│          Power BI Desktop · 8 páginas analíticas        │
├─────────────────────────────────────────────────────────┤
│                  CAPA DE ALMACENAMIENTO                 │
│     Supabase · PostgreSQL 15 · Esquema estrella         │
│           15 tablas · Triggers · Vistas · Logs          │
├─────────────────────────────────────────────────────────┤
│                   CAPA DE INGESTA                       │
│  Python 3.11 · SQLAlchemy · xml.etree.ElementTree       │
│          CFDI 3.3 / 4.0 · python-dotenv                 │
└─────────────────────────────────────────────────────────┘
          ▲
          │  Fuente de datos
┌─────────────────────┐
│  Archivos XML CFDI  │
│  (Ventas y Compras) │
│  Gasolinera Atlanta │
└─────────────────────┘
```

### Stack Tecnológico

| Componente | Tecnología | Propósito |
|---|---|---|
| Lenguaje principal | Python 3.11 | ETL, ML, orquestación |
| ORM y conexión BD | SQLAlchemy | Manejo de transacciones |
| Parser XML | xml.etree.ElementTree | Parseo de CFDIs |
| Base de datos | PostgreSQL 15 (Supabase) | Almacenamiento y automatización |
| ML — Predicción | Prophet (Meta) | Series de tiempo, 30 días |
| ML — Anomalías | pandas + NumPy (Z-score/IQR) | Detección estadística |
| Visualización | Power BI Desktop | 8 páginas analíticas |
| Automatización | Windows Task Scheduler | Ejecución nocturna 02:00 |
| Variables de entorno | python-dotenv | Gestión segura de credenciales |

---

## Pipeline ETL — Módulos

El pipeline está compuesto por **cuatro módulos** ejecutables de forma independiente u orquestada.

### Módulo 1 — `procesar_xmls.py`
**Función:** Parsea los archivos XML CFDI, extrae los campos fiscales (UUID, RFC emisor/receptor, fecha, conceptos, importes) e inserta registros en `fact_ventas` o `fact_compras` según el tipo de documento.

**Características clave:**
- Soporte para CFDI versiones **3.3 y 4.0** (ventas y compras)
- Control de duplicados en **3 tablas** (`control_xml`, `fact_ventas`, `fact_compras`) mediante verificación de UUID (folio fiscal) antes de cada inserción
- Cada XML se procesa en **transacción independiente**: errores no interrumpen el lote
- Archivos procesados exitosamente se mueven a `/procesados`; errores se registran en `log_etl`

> **Nota de calidad:** Esta versión resuelve **6 bugs** identificados en el script original. Ver sección [Gestión de Incidencias](#gestión-de-incidencias-resueltas).

### Módulo 2 — `actualizar_agregaciones.py`
**Función:** Consolida registros de `fact_ventas` en `agg_ventas_diarias` (con desglose por turno) y los de `fact_compras` en `agg_compras_periodo` (agrupados por semana calendario).

**Idempotencia garantizada:** Las inserciones utilizan cláusula `ON CONFLICT DO UPDATE`, asegurando que ejecuciones repetidas no generen duplicados.

**Resultado:** 5,478 filas upserted en `agg_ventas_diarias` y `agg_compras_periodo`.

### Módulo 3 — `calcular_inventario.py`
**Función:** Genera snapshots diarios en `fact_inventario` calculando el inventario por cada combinación gasolinera × tipo de combustible.

**Integración automática con alertas:** Al insertar en `fact_inventario`, el trigger `trg_verificar_inventario` (PostgreSQL) evalúa los umbrales y registra alertas en `alerta_inventario` sin intervención del script Python.

**Resultado:** 5,478 snapshots diarios generados sobre el historial completo.

### Módulo 4 — `pipeline_completo.py` *(Orquestador)*
**Función:** Ejecuta en secuencia `procesar_xmls → actualizar_agregaciones → calcular_inventario` con soporte de flags CLI para control granular.

```bash
# Ejecución completa
python pipeline_completo.py

# Flags disponibles
--skip-xml              # Omitir procesamiento de XMLs
--solo-movimiento       # Solo actualizar agregaciones e inventario
--fecha-desde YYYY-MM-DD
--fecha-hasta YYYY-MM-DD
--continuar-con-errores # No abortar ante fallos individuales
```

**Automatización:** Configurado en Windows Task Scheduler mediante script `.bat` para ejecución nocturna diaria a las **02:00** con `--skip-xml` para procesamiento incremental de nuevas facturas.

---

## Inteligencia Artificial y Machine Learning

| Módulo | Tecnología | Resultado | Estado |
|---|---|---|---|
| `prediccion_demanda.py` | Prophet (Meta) | 5,394 filas · 1,819 días históricos entrenados | Completado |
| `detectar_anomalias.py` | Z-score + IQR | 215 anomalías detectadas | Completado |

### Módulo ML-1 — Predicción de Demanda (`prediccion_demanda.py`)

**Modelo:** Prophet de Meta. Modelo aditivo de series de tiempo que descompone la demanda en:
- Tendencia general
- Estacionalidad semanal y anual
- Estacionalidad mensual (Fourier order 5)

**Configuración:**
```python
Prophet(
    changepoint_prior_scale=0.05,   # Tendencia conservadora
    seasonality_mode='multiplicative', # Estacionalidades proporcionales
    yearly_seasonality=True,
    weekly_seasonality=True
)
```

**Output:** 30 días de predicción con valor central (`yhat`), límite inferior y superior al **80% de confianza**, escritos en `predicciones_demanda`. Tiempo de ejecución: ~1,595 segundos.

### Módulo ML-2 — Detección de Anomalías (`detectar_anomalias.py`)

**Método:** Combinación de **Z-score + IQR**. Un día se clasifica como anomalía únicamente cuando **ambos métodos** lo detectan simultáneamente, reduciendo falsos positivos.

**Umbrales:**
| Severidad | Condición | Ejemplos en datos reales |
|---|---|---|
| Moderada | \|z\| > 2.0 | 97 anomalías |
| Alta | \|z\| > 3.0 | 56 anomalías |
| Extrema | \|z\| > 4.0 | 62 anomalías |

**Resultado:** 215 anomalías detectadas — Regular: 73, Premium: 76, Diesel: 66. Los picos extremos correlacionan con días previos a quincena, puentes o anuncios de aumentos de precio.

---

## Modelo de Datos

**Esquema dimensional** en PostgreSQL 15 con **15 tablas**, diseñado bajo principios de Data Warehouse.

### Tablas de Dimensión
| Tabla | Descripción |
|---|---|
| `dim_gasolinera` | Catálogo maestro de gasolineras con capacidad y ubicación |
| `dim_tipo_combustible` | Regular (SAT: 15101514), Premium (15101515), Diesel (15101516) |
| `dim_proveedor` | Proveedores de combustible identificados por RFC |
| `dim_cliente` | Clientes con flag de frecuencia y datos de contacto |

### Tablas de Hechos
| Tabla | Descripción |
|---|---|
| `fact_ventas` | Registro transaccional de ventas derivadas de CFDIs emitidos |
| `fact_compras` | Registro transaccional de compras derivadas de CFDIs recibidos |
| `fact_inventario` | Snapshot diario de inventario por gasolinera y tipo de combustible |

### Tablas Agregadas
| Tabla | Descripción |
|---|---|
| `agg_ventas_diarias` | Ventas consolidadas por día con desglose por turno |
| `agg_compras_periodo` | Compras agregadas por semana para análisis de abasto |

### Tablas de Machine Learning
| Tabla | Descripción |
|---|---|
| `predicciones_demanda` | Predicciones Prophet a 30 días con intervalo de confianza |
| `anomalias_ventas` | Anomalías detectadas vía Z-score + IQR sobre ventas diarias |

### Tablas Operacionales y de Control
| Tabla | Descripción |
|---|---|
| `alerta_inventario` | Alertas automáticas de inventario con seguimiento de atención |
| `control_xml` | Registro de todos los XMLs procesados y su estatus |
| `log_etl` | Log de ejecuciones del pipeline con métricas de desempeño |
| `sistema_heartbeat` | Monitor de actividad de cada proceso del sistema |

### Automatización en Base de Datos

**Trigger `trg_verificar_inventario`:** Se ejecuta automáticamente al insertar en `fact_inventario`. Evalúa el nivel de inventario contra la capacidad declarada y genera alertas en `alerta_inventario`.

**Función `calcular_turno()`:** Clasifica cada transacción en turno:
- Matutino: 06:00 – 13:59
- Vespertino: 14:00 – 21:59
- Nocturno: 22:00 – 05:59

**Vistas disponibles:** `vw_inventario_actual`, `vw_ventas_hoy`, `vw_alertas_pendientes`, `vw_ultimo_heartbeat`, `vw_resumen_ejecutivo`, `vw_anomalias_precio` — consumibles directamente desde Power BI.

---

## Sistema de Alertas

| Nivel | Condición | Umbral | Acción | Tiempo de Respuesta |
|---|---|---|---|---|
| Normal | Operación estándar | Inventario > 20% | Ninguna | N/A |
| Información | Nivel moderado | 15–20% capacidad | Log sistema | 48 horas |
| Advertencia | Atención requerida | 10–15% capacidad | Email | 24 horas |
| Crítico | Acción inmediata | < 10% capacidad | Email + SMS | 4 horas |
| Error | Condición anómala | Inventario negativo | Alerta urgente | Inmediato |

---

## Dashboard Power BI

**8 páginas analíticas** conectadas a Supabase mediante conector PostgreSQL nativo con certificado SSL.

| Página | Contenido |
|---|---|
| Resumen Ejecutivo | 6 KPIs, tendencia mensual, comparativo compras/ventas, alertas activas |
| Ventas | KPIs, barras mensuales, tendencia diaria, mapa de calor turno × día |
| Combustible | Dona por tipo, barras comparativas, precios promedio |
| Inventario | Curva de inventario, compras vs ventas, semáforo por gasolinera |
| Rentabilidad | Margen bruto por combustible, dispersión precio compra/venta |
| Alertas y Operaciones | Tabla de alertas con semáforo, historial `log_etl` |
| Predicción de Demanda | Gráfica real + ajuste histórico + predicción con banda de confianza |
| Detección de Anomalías | Tarjetas por severidad, anomalías sobre histórico, tabla con Z-score |

**Modelo DAX:** Tabla `_Medidas` centralizada como contenedor. `DimFecha` extendida hasta `2026-12-31` para cubrir el horizonte de predicción. Relación `fact_compras → dim_tipo_combustible` configurada como inactiva y activada con `USERELATIONSHIP` para evitar ambigüedad.

---

## Resultados e Impacto

### Resultados Cuantitativos — Dashboard v0.1.1

| KPI | Valor |
|---|---|
| Ventas totales gestionadas | $139.2M MXN |
| Litros de combustible controlados | 7.08M litros |
| Margen bruto operativo | 8.6% |
| Anomalías detectadas automáticamente | 215 eventos |
| CFDIs procesados | +200,000 facturas |
| Snapshots de inventario | 5,478 registros diarios |
| Predicciones generadas | 5,394 filas a 30 días |
| Bugs corregidos en ETL | 6 errores resueltos |

### Propuesta de Valor

| Dimensión | Antes | Después | Mejora |
|---|---|---|---|
| Tiempo de captura de datos | 4 hrs/semana/gerente (12 hrs totales) | 30 min/semana/gerente | **90% reducción** |
| Detección de desabasto | Hasta 30 días de retraso | Alerta en < 4 horas | **Tiempo real** |
| Decisiones de compra | Reactivas (crisis) | Predictivas (30 días adelante) | **Planificación anticipada** |
| Detección de anomalías | Semanas después del evento | Automática sobre historial | **215 eventos identificados** |

---

## Instalación y Configuración

### Prerrequisitos

```
Python 3.11+
PostgreSQL 15 (o cuenta Supabase)
Power BI Desktop
Windows (para Task Scheduler) o equivalente en Linux/macOS
```

### 1. Clonar el repositorio

```bash
git clone https://github.com/Gappy1223/Autoservicio-ETL.git
cd Autoservicio-ETL
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

Dependencias principales:
```
sqlalchemy
python-dotenv
pandas
numpy
prophet
lxml
supabase-py
```

### 3. Configurar variables de entorno

Crear archivo `.env` en la raíz del proyecto:

```env
# Supabase / PostgreSQL
DB_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT_REF].supabase.co:5432/postgres
SUPABASE_URL=https://[PROJECT_REF].supabase.co
SUPABASE_KEY=[ANON_KEY]

# Rutas de carpetas XML
XML_INPUT_PATH=./data/xmls/pendientes
XML_PROCESSED_PATH=./data/xmls/procesados
XML_ERROR_PATH=./data/xmls/errores
```

### 4. Inicializar base de datos

Ejecutar el DDL completo en tu instancia de PostgreSQL / Supabase SQL Editor. El schema incluye las 15 tablas, índices, triggers y vistas.

### 5. Configurar automatización (Windows)

Crear tarea en Windows Task Scheduler apuntando al script `.bat` en la carpeta `/scripts` para ejecución nocturna a las **02:00 AM**.

---

## Ejecución del Pipeline

```bash
# Ejecución completa (ETL + Agregaciones + Inventario)
python scripts/pipeline_completo.py

# Solo procesar XMLs nuevos
python scripts/procesar_xmls.py

# Solo actualizar agregaciones
python scripts/actualizar_agregaciones.py

# Solo recalcular inventario
python scripts/calcular_inventario.py

# Predicción de demanda (Prophet)
python scripts/prediccion_demanda.py

# Detección de anomalías (Z-score + IQR)
python scripts/detectar_anomalias.py

# Ejecución incremental (sin reprocesar XMLs)
python scripts/pipeline_completo.py --skip-xml

# Ejecución por rango de fechas
python scripts/pipeline_completo.py --fecha-desde 2026-01-01 --fecha-hasta 2026-04-30
```

---

## Gestión de Incidencias Resueltas

Documentación de incidencias siguiendo el ciclo ITIL de gestión de problemas.

| ID | Incidencia | Causa Raíz | Resolución |
|---|---|---|---|
| INC-001 | `UniqueViolation` en `fact_ventas` al reiniciar ETL | La verificación de duplicados consultaba únicamente `control_xml`; registros en `fact_ventas` sin entrada en `control_xml` generaban conflicto de clave | Extender `uuid_existente()` para consultar también `fact_ventas` y `fact_compras` previo a cada inserción |
| INC-002 | Error SSL al conectar Power BI con Supabase | Certificado raíz de Supabase no reconocido por el almacén de certificados de Windows | Importación manual del certificado raíz al almacén de confianza del sistema operativo |
| INC-003 | Relación ambigua entre `fact_compras` y `dim_tipo_combustible` en Power BI | Detección automática de relaciones generaba cardinalidad incorrecta | Desactivar detección automática; configurar todas las relaciones manualmente con `USERELATIONSHIP` en DAX |
| INC-004 | 6 bugs en `procesar_xmls.py` versión original | Uso de paréntesis en lugar de corchetes en diccionarios, lógica invertida en validación de complemento CFDI, typo en columna `subtota`, `timbre.get` sin argumento, imports `shutil` y `Path` ausentes | Corrección completa del script con validación de todos los flujos de ejecución |
| INC-005 | `prediccion_demanda.py` se colgaba al escribir en Supabase | `conn.execute(q, filas)` no acepta lista en SQLAlchemy moderno; error de encoding UTF-8 | Reemplazar upsert masivo por inserción fila a fila en lotes de 100 con commit por lote; agregar `client_encoding=utf8` al `create_engine` |
| INC-006 | Error de scope en `detectar_anomalias.py` (Python 3.14) | Variable `label` no accesible en bloque `except` por cambio de comportamiento de scoping en Python 3.14 | Inicializar `label` antes del bloque `try` y envolver la lógica en `try/except` interno |
| INC-007 | `DimFecha` no cubría fechas futuras de predicción | `CALENDAR()` generado con `MAX(fact_ventas[fecha_operacion])` como límite, excluyendo los 30 días predichos | Extender `DimFecha` hasta `DATE(2026,12,31)` para cubrir el horizonte de predicción de Prophet |

---

## Hoja de Ruta

### Fase Actual — Completada 
- [x] Schema PostgreSQL (15 tablas, índices, triggers, vistas)
- [x] Pipeline ETL completo con orquestación CLI
- [x] Procesamiento de +200,000 CFDIs (ventas y compras)
- [x] Módulo de predicción de demanda (Prophet)
- [x] Módulo de detección de anomalías (Z-score + IQR)
- [x] Dashboard Power BI con 8 páginas analíticas
- [x] Automatización nocturna vía Task Scheduler

### Fase 2 — Próximos Pasos 
- [ ] Validación cruzada Prophet (`cross_validation()` + `performance_metrics()`)
- [ ] Notificaciones push automáticas a dispositivos móviles
- [ ] Chat con datos en lenguaje natural (text-to-SQL con IA)
- [ ] Expansión a múltiples gasolineras (arquitectura ya preparada)
- [ ] Clustering de comportamiento de clientes (scikit-learn K-Means)
- [ ] Integración con sistemas ERP existentes

> **Nota de escalabilidad:** La arquitectura está preparada para incorporar gasolineras adicionales sin cambios estructurales, únicamente agregando entradas en `dim_gasolinera` y rutas en `pipeline_completo.py`.

---

## Estándares y Metodología

Este proyecto fue desarrollado alineado a los siguientes marcos de referencia:

| Marco | Aplicación en el Proyecto |
|---|---|
| **ITIL v4** | Gestión de incidencias documentada (INC-001 a INC-007), log de ejecuciones (`log_etl`), heartbeat de servicios (`sistema_heartbeat`) |
| **DAMA-DMBOK** | Esquema dimensional, control de calidad de datos en 3 capas (técnica, negocio, integridad), tasa de error < 5% |
| **ISO/IEC 25010** | Confiabilidad (control de duplicados), mantenibilidad (módulos desacoplados), eficiencia (ON CONFLICT, índices optimizados) |
| **PMBOK** | Cronograma por fases, gestión de alcance definida, entregables documentados por componente |
| **12 Factor App** | Variables de entorno en `.env`, logs en stdout, procesos sin estado |
| **Principios Big Data (5 V's)** | Volumen (+200K docs), Velocidad (< 4 hrs latencia), Variedad (XML semi-estructurado), Veracidad (UUID SAT), Valor (ROI cuantificado) |

---

## Autor

**Juan Fernando Macías Mandujano**
Estudiante de Ingeniería en Sistemas Computacionales — Universidad Tecmilenio (8vo semestre)

[![GitHub](https://img.shields.io/badge/GitHub-Gappy1223-black?logo=github)](https://github.com/Gappy1223)
[![Email](https://img.shields.io/badge/Email-jmacias1223%40outlook.com-blue?logo=microsoft-outlook)](mailto:jmacias1223@outlook.com)

**Proyecto académico:** Proyecto Integrador de Big Data
**Profesor:** Jonathan Alexis Puente Guerrero
**Universidad:** Tecmilenio — Campus Virtual
**Período:** Febrero – Abril 2026

---

