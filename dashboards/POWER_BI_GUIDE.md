# Guía Rápida — Dashboard en Power BI Desktop

> Esta guía conecta **Power BI Desktop** directamente a PostgreSQL (`happiness_db`) utilizando las vistas ya desplegadas por [`sql/views_bi.sql`](../sql/views_bi.sql) (sin usar archivos CSV).

---

## Paso 1: Conectar Power BI a PostgreSQL

1. Abre **Power BI Desktop** (`C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe`).
2. Haz clic en **Obtener datos** → **Más...** → **Base de datos** → **Base de datos PostgreSQL** → **Conectar**.
3. Ingresa los parámetros exactos:
   * **Servidor:** `127.0.0.1:5433`
   * **Base de datos:** `happiness_db`
   * **Modo de conectividad:** `Importar` (recomendado) o `DirectQuery`
4. En autenticación selecciona **Base de datos**:
   * **Nombre de usuario:** `etl_user`
   * **Contraseña:** `etl_password`
5. *(Si aparece un aviso de cifrado/SSL)*: ve a **Archivo → Opciones y configuración → Configuración de origen de datos → Editar permisos** y desmarca **Cifrar conexiones**.

---

## Paso 2: Seleccionar las vistas en el Navegador

En el navegador selecciona estas **6 vistas listas para graficar** y pulsa **Cargar**:
* `vw_kpi_global_summary`
* `vw_kpi_predictions_by_country`
* `vw_kpi_predicted_vs_actual`
* `vw_kpi_trends_over_time`
* `vw_kpi_trends_by_year`
* `vw_streaming_audit`

*(Alternativa modelo estrella: si prefieres cargar las tablas `fact_predictions`, `dim_country` y `dim_date` directamente, al final están las medidas DAX equivalentes).*

---

## Paso 3: Configurar los Visuales del Dashboard

### 1. Tarjetas KPI Superiores (KPI 1 — Error Promedio)
Usa **Tarjeta (Card)** con `vw_kpi_global_summary`:
* **Error Promedio (MAE):** campo `mae` *(valor esperado ~`0.4258`)*
* **RMSE Global:** campo `rmse` *(valor esperado ~`0.5479`)*
* **Total Predicciones:** campo `total_predictions` *(valor esperado `782`)*
* **Países Analizados:** campo `total_countries` *(valor esperado `164`)*

### 2. Predicciones por País (KPI 2)
* **Visual:** **Gráfico de barras horizontales apiladas** o **Mapa coroplético** con `vw_kpi_predictions_by_country`:
  * **Eje Y / Ubicación:** `country_name`
  * **Eje X / Tamaño:** `predictions_count` (y en información sobre herramientas / color: `mae`, `avg_actual_score`, `avg_predicted_score`)
  * *(Sugerencia)*: añade un filtro Top N o segmentador de `country_name` para explorar países con mayor error (`mae`).

### 3. Puntaje Predicho vs. Puntaje Real (KPI 3)
* **Visual:** **Gráfico de dispersión (Scatter Chart)** con `vw_kpi_predicted_vs_actual`:
  * **Eje X:** `actual_score` (sin resumir / Promedio)
  * **Eje Y:** `predicted_score` (sin resumir / Promedio)
  * **Valores / Detalles:** `prediction_id` (para ver los 782 puntos individuales)
  * **Leyenda:** `accuracy_band`
  * **Información sobre herramientas (Tooltips):** `country_name`, `observation_year`, `prediction_error`

### 4. Tendencias en el Tiempo (KPI 4)
* **Visual A — Flujo de Streaming en Vivo:** **Gráfico de líneas y columnas agrupadas** con `vw_kpi_trends_over_time`:
  * **Eje X:** `event_time_bucket` *(asegúrate de mostrar la marca de tiempo continua, no solo la fecha del día)*
  * **Eje Y de columnas:** `predictions_in_bucket` (o **Eje Y de línea:** `cumulative_predictions`)
  * **Eje Y secundario de línea:** `mae`
* **Visual B — Evolución por Año Histórico (2015–2019):** **Gráfico de líneas** con `vw_kpi_trends_by_year`:
  * **Eje X:** `observation_year`
  * **Eje Y:** `avg_actual_score` y `avg_predicted_score`

### 5. Salud y Tolerancia a Fallos del Pipeline (Bonus Auditoría)
* **Visual:** **Gráfico de anillos (Donut Chart)** o **Tabla** con `vw_streaming_audit`:
  * **Leyenda:** `processing_status` (`VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`)
  * **Valores:** `events_count`

---

## Paso 4: Guardar Entregables

Guarda en esta misma carpeta [`dashboards/`](./):
1. El archivo **`dashboards/happiness_dashboard.pbix`**.
2. Al menos 1–2 capturas de pantalla (por ejemplo **`dashboards/dashboard_overview.png`**).

---

## Anexo: Medidas DAX (si cargas `fact_predictions` + `dim_country` + `dim_date` directamente)

```dax
Total Predicciones = COUNT(fact_predictions[prediction_id])

MAE = AVERAGEX(fact_predictions, ABS(fact_predictions[actual_score] - fact_predictions[predicted_score]))

RMSE = SQRT(AVERAGEX(fact_predictions, POWER(fact_predictions[actual_score] - fact_predictions[predicted_score], 2)))

Promedio Score Real = AVERAGE(fact_predictions[actual_score])

Promedio Score Predicho = AVERAGE(fact_predictions[predicted_score])
```
