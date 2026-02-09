# 📊 Mini Proyecto ETL con Python + BigQuery

Este repositorio contiene un **pipeline ETL (Extract, Transform, Load)** desarrollado en Python para cargar archivos Excel almacenados en una carpeta (SharePoint sincronizado) hacia una **tabla staging en BigQuery**, aplicando validaciones, limpieza y transformaciones de datos.

El objetivo principal es **automatizar la carga diaria de reportes**, evitar duplicados y garantizar que los datos lleguen a BigQuery con un esquema consistente.

---

## 🏗️ Arquitectura del flujo

1. **Fuente**: Archivos Excel `.xlsb` almacenados en SharePoint (sincronizado localmente)
2. **Extract**: Lectura automática con Pandas
3. **Transform**: Limpieza, estandarización y casting de datos
4. **Validation**: Consulta previa a BigQuery para evitar duplicados
5. **Load**: Carga incremental (`WRITE_APPEND`) a tabla staging en BigQuery

---

## 🧰 Tecnologías utilizadas

* **Python 3**
* **Pandas / NumPy** → transformación de datos
* **google-cloud-bigquery** → consultas y cargas
* **BigQuery** → Data Warehouse
* **Regex** → extracción de fecha desde el nombre del archivo

---

## 🔐 Configuración de credenciales

BigQuery utiliza autenticación mediante una **Service Account**.

```python
credenciales_bigquery = r'C:\ruta\a\credenciales.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
```

> ⚠️ **Buena práctica**: No subir el archivo de credenciales a GitHub. Agregar la ruta al `.gitignore`.

---

## 📥 Función: Realizar_Consulta

Ejecuta consultas SQL en BigQuery y devuelve el resultado como un DataFrame.

```python
def Realizar_Consulta(sql_query):
    """
    Ejecuta una consulta SQL en BigQuery y retorna el resultado como DataFrame.
    Se utiliza principalmente para validar si una fecha ya fue cargada.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
    cliente = bigquery.Client()

    query_job = cliente.query(sql_query)
    resultado = [dict(fila) for fila in query_job.result()]

    return pd.DataFrame(resultado)
```

### ¿Por qué es importante?

* Permite **cargas idempotentes**
* Evita duplicar información por fecha

---

## 🔄 Función: Transformar_datos

Aplica todas las transformaciones necesarias antes de enviar los datos a BigQuery.

```python
def Transformar_datos(df_Data, fecha_archivo):
    """
    Limpia y transforma el DataFrame para que sea compatible con BigQuery.
    - Conversión de fechas Excel
    - Renombrado de columnas
    - Casting de tipos
    - Limpieza de nulos
    """
    try:
        # Conversión de fechas desde formato Excel (serial date)
        df_Data['Fecha'] = pd.to_datetime(df_Data['Fecha'], unit='D', origin='1899-12-30', errors='coerce')
        df_Data['Fecha_HU'] = pd.to_datetime(df_Data['Fecha_HU'], unit='D', origin='1899-12-30', errors='coerce')
        df_Data['CreadoEl'] = pd.to_datetime(df_Data['CreadoEl'], unit='D', origin='1899-12-30', errors='coerce')
        df_Data['FECHA VENCIMIENTO'] = pd.to_datetime(df_Data['FECHA VENCIMIENTO'], unit='D', origin='1899-12-30', errors='coerce')

        # Renombrado de columnas (data governance)
        mapeo_columnas = {
            'Fecha': 'Fecha_Reporte',
            'FECHA VENCIMIENTO': 'Fecha_Vencimiento',
            'Dias': 'DiasDePermanencia',
            'Dias Redireccion': 'DiasRedireccion',
            'CodGA': 'CodigoArticulo',
            'GrAr': 'NombreArticulo',
            'DescMaterial': 'NombreMaterial',
            'PUnit': 'PrecioUnidad',
            'CodProveedor': 'RUC_Proveedor',
            'Stock Tda Unidades': 'Stock_Tda_Unidades',
            'Cob Tda': 'Cob_Tda',
            'cob <=5': 'cob_menor_o_igual_a_5',
            'Tipo tda': 'TipoTienda',
            'Ce.Origen': 'CentroOrigen',
            'Ce': 'CentroDistribucion',
            'Status HU': 'StatusHU',
            'Gerencia Paleta': 'GerenciaPaleta',
            'Dueño Paleta': 'DunioPaleta',
            'VentaPromedio 30D': 'VentaPromedio30D',
            'Grp Abast.': 'Grupo_Abastecimiento',
            'Alerta Paleta': 'AlertaPaleta'
        }

        df_Data.rename(columns=mapeo_columnas, inplace=True)

        # Columna de control
        df_Data['Envio'] = 3

        # Cálculo de grupo de compra desde el código de artículo
        codigos_articulo = df_Data['CodigoArticulo'].astype(str)
        df_Data['GrComp'] = pd.to_numeric(
            np.where(
                codigos_articulo.str.len() < 8,
                codigos_articulo.str[:1],
                codigos_articulo.str[:2]
            ),
            errors='coerce'
        )

        # Columnas que deben ser STRING en BigQuery
        columnas_string = ['CentroDistribucion', 'Almacen', 'StatusHU', 'Hora', 'Gestor']
        for col in columnas_string:
            df_Data[col] = df_Data[col].astype(str).replace(['nan', 'None', 'NaT'], None)

        # Eliminación de columnas innecesarias
        df_Data.drop(columns=['Contar'], inplace=True, errors='ignore')

        return True, df_Data

    except Exception as e:
        return False, f"Error en transformación ({fecha_archivo}): {e}"
```

### Buenas prácticas aplicadas

* Conversión explícita de tipos
* Renombrado estandarizado
* Manejo de errores por archivo

---

## 🚀 Función: Enviar_datos_por_lote

Carga el DataFrame limpio a BigQuery usando el motor de PyArrow.

```python
def Enviar_datos_por_lote(df, nombre_tabla, fecha_archivo):
    """
    Carga un DataFrame a BigQuery usando carga por lote.
    La tabla debe existir previamente.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credenciales_bigquery
    cliente = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    try:
        job = cliente.load_table_from_dataframe(df, nombre_tabla, job_config=job_config)
        job.result()
        print(f"✔ {len(df)} filas cargadas | Fecha: {fecha_archivo}")
    except Exception as e:
        print(f"✖ Error carga {fecha_archivo}: {e}")
```

---

## 🔁 Orquestación principal

```python
for archivo in os.listdir(ruta_carpeta):
    if archivo.lower().startswith('priorización'):
        # Extraer fecha desde el nombre del archivo
        extraer_fecha = re.search(r'(\d{2}\.\d{2}\.\d{4})', archivo).group(1)
        fecha_nueva = pd.to_datetime(extraer_fecha, dayfirst=True).strftime('%Y-%m-%d')

        # Validar si la fecha ya existe en BigQuery
        sql = f"SELECT COUNT(*) AS conteo_filas FROM {Tabla_bigquery} WHERE Fecha_Reporte = '{fecha_nueva}'"
        df_validacion = Realizar_Consulta(sql)

        if df_validacion['conteo_filas'][0] > 0:
            continue

        # Lectura del archivo
        df_data = pd.read_excel(os.path.join(ruta_carpeta, archivo), sheet_name='DATA', engine='pyxlsb')

        # Transformación
        ok, df_data = Transformar_datos(df_data, fecha_nueva)
        if not ok:
            continue

        # Carga a BigQuery
        Enviar_datos_por_lote(df_data, Tabla_bigquery, fecha_nueva)
```

---

## 📈 Posibles mejoras futuras

* Definir **esquema explícito** en BigQuery
* Implementar **Data Quality Checks**
* Logging estructurado (Cloud Logging)
* Orquestación con **Cloud Functions / Airflow**

---

## 🎯 Propósito del Proyecto (Caso de Uso Real)

El propósito final de este mini proyecto es **integrarlo dentro del proceso de generación del reporte de paletas**, migrando un flujo tradicional basado en archivos Excel hacia una arquitectura **centralizada y escalable en BigQuery**.

Con esta implementación:

* 📥 Los archivos operativos continúan llegando en Excel (origen real del negocio)
* 🔄 El proceso ETL automatiza la limpieza y estandarización
* 🗄️ BigQuery actúa como **fuente única de la verdad (Single Source of Truth)**
* 📊 Múltiples usuarios pueden conectarse simultáneamente desde **Power BI**

---

## 📊 Consumo de datos en Power BI

Los datos cargados en BigQuery están pensados para ser consumidos directamente desde Power BI, permitiendo:

* 🔹 Conexión directa vía **conector nativo de BigQuery**
* 🔹 Análisis en **tiempo casi real** (sin depender de archivos locales)
* 🔹 Dashboards compartidos para áreas de logística, BI y operaciones
* 🔹 Eliminación de reprocesos manuales y versiones inconsistentes

Este enfoque habilita:

* Mejor toma de decisiones
* Mayor trazabilidad de la información
* Escalabilidad a futuro (nuevos reportes / nuevos usuarios)

---

## ✅ Conclusión

Este proyecto representa un **primer paso hacia una arquitectura moderna de datos**:

* Centralización en BigQuery
* Automatización del proceso ETL
* Consumo multiusuario desde herramientas BI

Aunque es un proyecto pequeño, refleja un escenario **realista de transformación de reportes operativos** hacia analítica en tiempo real, siendo una base sólida para evolucionar hacia soluciones más avanzadas (orquestación, calidad de datos y modelos analíticos).
