from concurrent.futures import ThreadPoolExecutor
import csv
import io
import json
import boto3

s3_client = boto3.client('s3')


def subir_archivo(output_bucket, output_key, row):
    # SIN indent=2 para que cada registro quede en una sola línea (formato compatible con Athena)
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
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = response['Body'].read().decode('utf-8-sig')
        filas = list(csv.DictReader(io.StringIO(csv_content)))

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