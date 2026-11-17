# Pipeline Serverless ETL: CSV a JSON en AWS (S3 + Lambda + Glue + Athena)

Este proyecto implementa una arquitectura *serverless* en AWS para procesar archivos CSV almacenados en **Amazon S3**, transformar cada fila en un objeto JSON independiente en formato NDJSON (*Newline Delimited JSON*) mediante **AWS Lambda** con procesamiento concurrente, catalogar los datos con **AWS Glue** y consultarlos mediante SQL estándar en **Amazon Athena**.

---

## Arquitectura del Flujo

```text
[S3 Input Bucket]                     [AWS Lambda (Python)]                  [S3 Output Bucket]               [Glue + Athena]
users_dataset_100.csv  ──(GetObject)──▶  Lectura y ThreadPool  ──(PutObject)──▶  registro_001..100.json  ──▶  Consultas SQL
```

### Componentes utilizados

| Servicio | Recurso / Nombre | Función en el flujo |
| :--- | :--- | :--- |
| **Amazon S3 (Input)** | `bld-tseed-workshop-input-semillero-2026-q4-c` | Almacena el archivo origen `DavidTorres/users_dataset_100.csv`. |
| **AWS Lambda** | `bld-tseed-workshop-DavidTorres` | Lee el CSV, convierte cada fila a JSON de una sola línea y los sube en paralelo. |
| **AWS IAM** | `bld-tseed-workshop-DavidTorres-role-9gwnk81k` | Rol de ejecución con permisos mínimos necesarios sobre los buckets S3. |
| **Amazon S3 (Output)** | `bld-tseed-workshop-output-semillero-2026-q4-c` | Almacena los 100 archivos JSON resultantes en la carpeta `DavidTorres/`. |
| **AWS Glue & Athena** | Base de datos: `bld-tseed-workshop-davidtorres` | Cataloga el esquema de los JSON y permite ejecutar consultas SQL. |

---

## Guía Paso a Paso

### 1. Configuración de Permisos IAM (Principio de Mínimo Privilegio)

Para que la función Lambda pueda leer el CSV de entrada y escribir los archivos JSON de salida sin errores `AccessDenied`, se adjunta una política insertada (*Inline Policy*) al rol de ejecución de la función:

1. Ir a **AWS Lambda** $\rightarrow$ **Configuración** $\rightarrow$ **Permisos**.
2. Abrir el enlace del **Rol de ejecución** (`bld-tseed-workshop-DavidTorres-role-9gwnk81k`) en la consola de **IAM**.
3. Seleccionar **Agregar permisos** $\rightarrow$ **Crear política insertada** $\rightarrow$ pestaña **JSON** y pegar:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "LeerCSVInput",
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bld-tseed-workshop-input-semillero-2026-q4-c/*"
        },
        {
            "Sid": "EscribirJSONOutput",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::bld-tseed-workshop-output-semillero-2026-q4-c/*"
        }
    ]
}
```

4. Guardar la política con el nombre `PermisosS3Workshop`.

---

### 2. Ajuste de Tiempo de Espera (Timeout) en Lambda

El *timeout* por defecto de Lambda es de **3 segundos**, insuficiente para realizar 100 peticiones individuales hacia S3 de forma secuencial.

1. Ir a **AWS Lambda** $\rightarrow$ **Configuración** $\rightarrow$ **Configuración general** $\rightarrow$ **Editar**.
2. Cambiar el **Tiempo de espera** de `3 seg` a **`30 seg`**.
3. Hacer clic en **Guardar**.

---

### 3. Código de la Función Lambda (`lambda_function.py`)

El código utiliza `ThreadPoolExecutor` para subir hasta 20 archivos en paralelo (reduciendo el tiempo total de ejecución a menos de 1 segundo) y omite `indent=2` en `json.dumps` para garantizar compatibilidad nativa con el motor `JsonSerDe` de Amazon Athena.

```python
from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import boto3

# Inicialización del cliente fuera del handler para reutilizar conexiones entre invocaciones
s3_client = boto3.client('s3')


def subir_archivo(output_bucket, output_key, row):
    """Serializa una fila como JSON de una sola línea y la sube a S3."""
    json_data = json.dumps(row, ensure_ascii=False)
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
    )
    return output_key


def lambda_handler(event, context):
    source_bucket = event.get(
        'source_bucket', 'bld-tseed-workshop-input-semillero-2026-q4-c'
    )
    source_key = event.get(
        'source_key', 'DavidTorres/users_dataset_100.csv'
    )
    output_bucket = event.get(
        'output_bucket', 'bld-tseed-workshop-output-semillero-2026-q4-c'
    )
    output_subfolder = event.get('output_subfolder', 'DavidTorres/')

    if output_subfolder and not output_subfolder.endswith('/'):
        output_subfolder += '/'

    try:
        # 1. Descargar y decodificar el archivo CSV en memoria
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = response['Body'].read().decode('utf-8-sig')
        filas = list(csv.DictReader(io.StringIO(csv_content)))

        # 2. Subir cada registro como un archivo JSON independiente en paralelo
        with ThreadPoolExecutor(max_workers=20) as executor:
            futuros = [
                executor.submit(
                    subir_archivo,
                    output_bucket,
                    f'{output_subfolder}registro_{index:03d}.json',
                    row,
                )
                for index, row in enumerate(filas, start=1)
            ]
            resultados = [f.result() for f in futuros]

        return {
            'statusCode': 200,
            'body': json.dumps(
                f'Proceso exitoso. Se crearon {len(resultados)} archivos JSON'
                f' en {output_bucket}/{output_subfolder}'
            ),
        }

    except Exception as e:
        print(f'Error procesando el archivo: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error interno: {str(e)}'),
        }
```

> **Nota:** Después de pegar el código en el editor de Lambda, presionar siempre el botón **Deploy** antes de ejecutar pruebas.

---

### 4. Ejecución del Evento de Prueba (Test Event)

En la pestaña **Probar** (*Test*) de AWS Lambda, configurar el evento `inputTest` con el siguiente *payload*:

```json
{
  "source_bucket": "bld-tseed-workshop-input-semillero-2026-q4-c",
  "source_key": "DavidTorres/users_dataset_100.csv",
  "output_bucket": "bld-tseed-workshop-output-semillero-2026-q4-c",
  "output_subfolder": "DavidTorres/"
}
```

**Respuesta esperada (`statusCode: 200`):**

```json
{
  "statusCode": 200,
  "body": "\"Proceso exitoso. Se crearon 100 archivos JSON en bld-tseed-workshop-output-semillero-2026-q4-c/DavidTorres/\""
}
```

---

### 5. Consultas Analíticas en Amazon Athena

Una vez catalogada la carpeta `DavidTorres/` mediante un Crawler de **AWS Glue** en la tabla `"AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"`, se pueden ejecutar las siguientes consultas SQL:

#### Validación del total de registros
```sql
SELECT COUNT(*) AS total_registros
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres";
```

#### Salario y experiencia promedio por departamento
```sql
SELECT 
    department,
    COUNT(*) AS total_empleados,
    ROUND(AVG(CAST(salary AS DOUBLE)), 2) AS salario_promedio,
    ROUND(AVG(CAST(experience_years AS INT)), 1) AS experiencia_promedio
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"
GROUP BY department
ORDER BY salario_promedio DESC;
```

#### Distribución de roles según nivel educativo
```sql
SELECT 
    role,
    education,
    COUNT(*) AS cantidad
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"
GROUP BY role, education
ORDER BY role ASC, cantidad DESC;
```

#### Búsqueda de perfiles por habilidades e idiomas específicos
```sql
SELECT 
    user_id,
    name,
    role,
    department,
    skills,
    languages,
    CAST(salary AS DOUBLE) AS salario
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"
WHERE skills LIKE '%SQL%' 
  AND languages LIKE '%Spanish%'
ORDER BY salario DESC;
```

---

## Buenas Prácticas y Lecciones Aprendidas

| Aspecto | Problema común | Solución aplicada |
| :--- | :--- | :--- |
| **Formato JSON para Athena** | Usar `indent=2` genera JSON multilínea y provoca el error `HIVE_CURSOR_ERROR` al consultar. | Serializar cada objeto en **una sola línea** (`json.dumps(row, ensure_ascii=False)`). |
| **Codificación de caracteres** | Tildes o caracteres especiales (`ñ`) corrompen el texto o fallan al decodificar. | Leer con `utf-8-sig` (elimina BOM de Excel) y escribir con `ensure_ascii=False` en `utf-8`. |
| **Concurrencia y latencia** | Subir 100 archivos de forma secuencial tarda ~5 segundos y supera el *timeout* por defecto. | Uso de `ThreadPoolExecutor(max_workers=20)` + cliente `boto3` fuera del `lambda_handler`. |
| **Seguridad IAM** | Otorgar permisos globales (`AmazonS3FullAccess`) expone recursos innecesarios. | Política inline acotada únicamente a `s3:GetObject` en *input* y `s3:PutObject` en *output*. |
| **Tipos de datos en Athena** | Los campos provenientes de CSV se infieren como `string`. | Uso explícito de `CAST(columna AS DOUBLE / INT)` en operaciones matemáticas y filtros. |
