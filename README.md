Pipeline Serverless en AWS: Procesamiento de CSV a JSON y Análisis SQL con Athena

Este proyecto implementa una arquitectura serverless en AWS para ingerir un archivo .csv almacenado en Amazon S3, dividir cada uno de sus 100 registros en archivos .json individuales mediante AWS Lambda (con procesamiento paralelo), catalogar los datos con AWS Glue y consultarlos mediante SQL estándar utilizando Amazon Athena.

Tabla de Contenidos

Arquitectura de la Solución

Requisitos Previos y Estructura en S3

Paso 1: Configuración de Permisos IAM

Paso 2: Implementación de la Función AWS Lambda

Paso 3: Ajuste de Tiempo de Espera (Timeout) y Pruebas

Paso 4: Catalogación y Consultas SQL en Amazon Athena

Lecciones Aprendidas y Buenas Prácticas

1. Arquitectura de la Solución

+-----------------------------------+      +------------------------------------+
|        Amazon S3 (Input)          |      |            AWS Lambda              |
| bld-tseed-workshop-input-...      | ---> | bld-tseed-workshop-DavidTorres     |
| └── DavidTorres/                  |      | - Lectura CSV (csv.DictReader)     |
|     └── users_dataset_100.csv     |      | - Escritura paralela (20 hilos)    |
+-----------------------------------+      | - Formato NDJSON (1 línea/archivo) |
                                           +------------------------------------+
                                                             |
                                                             v
+-----------------------------------+      +------------------------------------+
|          Amazon Athena            |      |        Amazon S3 (Output)          |
| BD: bld-tseed-workshop-davidtorres| <--- | bld-tseed-workshop-output-...      |
| Tabla: davidtorres                |      | └── DavidTorres/                   |
| (Consultas SQL analíticas)        |      |     ├── registro_001.json          |
+-----------------------------------+      |     ├── ...                        |
                                           |     └── registro_100.json          |
                                           +------------------------------------+


2. Requisitos Previos y Estructura en S3

Antes de configurar la función, se requiere contar con dos buckets de Amazon S3:

Bucket de Entrada (Input): bld-tseed-workshop-input-semillero-2026-q4-c

Ruta del archivo: DavidTorres/users_dataset_100.csv (100 registros de usuarios con 17 columnas).

Bucket de Salida (Output): bld-tseed-workshop-output-semillero-2026-q4-c

Subcarpeta de destino: DavidTorres/

Paso 1: Configuración de Permisos IAM

Por seguridad, AWS Lambda aplica el principio de mínimo privilegio y no tiene acceso por defecto a los buckets de S3. Para evitar errores AccessDenied, se debe otorgar permiso de lectura al bucket de origen y permiso de escritura al bucket de destino.

Ingresar a la función Lambda (bld-tseed-workshop-DavidTorres) -> pestaña Configuración -> Permisos.

Hacer clic sobre el Rol de ejecución (bld-tseed-workshop-DavidTorres-role-9gwnk81k) para abrir la consola de IAM.

Seleccionar Agregar permisos -> Crear política insertada -> pestaña JSON y pegar la siguiente política:

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


Guardar la política con un nombre descriptivo (ej. PermisosS3Workshop).

Paso 2: Implementación de la Función AWS Lambda

En la pestaña Código de la función Lambda (lambda_function.py), se implementa el script en Python utilizando boto3 y ThreadPoolExecutor para subir los 100 archivos en paralelo en menos de 1 segundo.

Nota técnica: Los objetos JSON se serializan sin sangría (indent) para que cada archivo ocupe exactamente una línea (formato compatible con el motor JsonSerDe de Amazon Athena).

from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import boto3

# Inicializar el cliente de S3 fuera del handler permite reutilizar conexiones entre invocaciones
s3_client = boto3.client('s3')


def subir_archivo(output_bucket: str, output_key: str, row: dict) -> str:
    """Serializa una fila en formato JSON de una sola línea y la sube a S3."""
    json_data = json.dumps(row, ensure_ascii=False)
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json_data.encode('utf-8'),
        ContentType='application/json',
    )
    return output_key


def lambda_handler(event, context):
    # Permite recibir parámetros desde el evento o usar las rutas por defecto
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

    # Normalizar el prefijo de la carpeta de salida
    if output_subfolder and not output_subfolder.endswith('/'):
        output_subfolder += '/'

    try:
        # 1. Descargar y decodificar el CSV desde S3 en memoria (utf-8-sig elimina BOM si existe)
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = response['Body'].read().decode('utf-8-sig')

        # 2. Parsear el CSV usando la primera fila como claves del diccionario
        filas = list(csv.DictReader(io.StringIO(csv_content)))
        print(f'Total de filas leídas del CSV: {len(filas)}')

        # 3. Subir los archivos JSON en paralelo (hasta 20 hilos simultáneos)
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


Una vez pegado el código, hacer clic en Deploy para publicar los cambios.

Paso 3: Ajuste de Tiempo de Espera (Timeout) y Pruebas

1. Configurar el Timeout

Por defecto, AWS Lambda tiene un tiempo límite de ejecución de 3 segundos, lo cual puede cortar el proceso antes de terminar las 100 peticiones de red hacia S3.

Ir a Configuración -> Configuración general -> Editar.

Cambiar el Tiempo de espera a 30 segundos y hacer clic en Guardar.

2. Ejecutar el Evento de Prueba

Ir a la pestaña Probar (Test) -> Crear nuevo evento.

Asignar el nombre inputTest y utilizar el siguiente JSON:

{
  "source_bucket": "bld-tseed-workshop-input-semillero-2026-q4-c",
  "source_key": "DavidTorres/users_dataset_100.csv",
  "output_bucket": "bld-tseed-workshop-output-semillero-2026-q4-c",
  "output_subfolder": "DavidTorres/"
}


Ejecutar el test y verificar que la respuesta retorne "statusCode": 200 y que en S3 existan los archivos desde registro_001.json hasta registro_100.json.

Paso 4: Catalogación y Consultas SQL en Amazon Athena

Una vez catalogada la carpeta s3://bld-tseed-workshop-output-semillero-2026-q4-c/DavidTorres/ mediante un AWS Glue Crawler en la base de datos bld-tseed-workshop-davidtorres (tabla davidtorres), se pueden ejecutar consultas analíticas en Amazon Athena.

Nota sobre tipos de datos: Dado que los campos provienen de un CSV convertido a JSON, Athena los infiere inicialmente como STRING. Se utiliza CAST(... AS INT) o CAST(... AS DOUBLE) para realizar cálculos matemáticos y ordenamientos numéricos.

1. Validación de conteo total de registros

SELECT COUNT(*) AS total_registros
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres";


2. Salario promedio y experiencia por departamento

SELECT 
    department,
    COUNT(*) AS total_empleados,
    ROUND(AVG(CAST(salary AS DOUBLE)), 2) AS salario_promedio,
    ROUND(AVG(CAST(experience_years AS INT)), 1) AS experiencia_promedio
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"
GROUP BY department
ORDER BY salario_promedio DESC;


3. Búsqueda de talento por habilidades e idiomas

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


4. Segmentación demográfica por rangos de edad

SELECT 
    CASE 
        WHEN CAST(age AS INT) < 30 THEN 'Menor de 30'
        WHEN CAST(age AS INT) BETWEEN 30 AND 45 THEN '30 a 45 años'
        ELSE 'Mayor de 45'
    END AS rango_edad,
    COUNT(*) AS total_usuarios,
    ROUND(AVG(CAST(salary AS DOUBLE)), 2) AS salario_promedio
FROM "AwsDataCatalog"."bld-tseed-workshop-davidtorres"."davidtorres"
GROUP BY 1
ORDER BY total_usuarios DESC;


7. Lecciones Aprendidas y Buenas Prácticas

Compatibilidad JSON con Athena (HIVE_CURSOR_ERROR): Nunca usar indent=2 al generar archivos JSON destinados a Amazon Athena. El SerDe estándar lee línea por línea y falla si un objeto JSON está dividido en múltiples líneas.

Rendimiento con I/O en S3: Realizar 100 llamadas secuenciales put_object toma ~5 segundos; utilizar ThreadPoolExecutor reduce la ejecución a menos de 1 segundo, optimizando costos de facturación por milisegundo en Lambda.

Codificación de caracteres: Usar utf-8-sig al leer archivos CSV evita que caracteres invisibles de marca de orden de bytes (BOM) generados por Excel corrompan el nombre de la primera columna del JSON.

Reutilización del cliente AWS: Instanciar boto3.client('s3') fuera de lambda_handler aprovecha la reutilización del entorno de ejecución (warm starts) de Lambda.