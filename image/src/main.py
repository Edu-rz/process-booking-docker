import json
import boto3
from botocore.stub import Stubber
import uuid
from datetime import datetime, timezone, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# Función para validar que todos los campos requeridos están presentes en el evento
def validate_required_fields(mensaje, required_fields):
    missing_fields = [field for field in required_fields if field not in mensaje]
    if missing_fields:
        raise ValueError(f"Faltan los siguientes campos en el cuerpo de la solicitud: {', '.join(missing_fields)}")

# Función para validar el tipo de datos de cada campo
def validate_field_types(mensaje, field_types):
    for field, expected_type in field_types.items():
        if expected_type == int:
            if not isinstance(mensaje[field], int):
                raise TypeError(f"El campo '{field}' debe ser de tipo entero.")
        elif expected_type == 'timestamp':
            validate_timestamp_format(mensaje[field], field)

# Función para validar el formato de un campo de fecha (timestamp)
def validate_timestamp_format(date_str, field_name):
    try:
        datetime.fromisoformat(date_str)
    except ValueError:
        raise ValueError(f"El campo '{field_name}' debe estar en formato ISO 8601 (YYYY-MM-DDTHH:MM:SS).")

def logger(content):
    print("[LAMBDA] - " + datetime.utcnow().strftime('%d/%m/%Y, %H-%M-%S') + " - LOG " + content)

# Campos requeridos y sus tipos esperados
required_fields = ['booking_id', 'booking_date', 'status', 'user_id', 'salon_id', 'payment_id', 'service_id', 'service_name', 'price']
field_types = {
    'booking_id': str,
    'booking_date': 'timestamp',
    'status': str,
    'user_id': str,
    'salon_id': int,
    'payment_id': str,
    'service_id': int,
    'service_name': str,
    'price': float
}

lima_tz = timezone(timedelta(hours=-5))
timestamp_precision = 'ms'

# Crear un esquema de Parquet
schema = pa.schema([
    ('booking_id', pa.string()),
    ('booking_date', pa.timestamp(timestamp_precision, tz='America/Lima')),
    ('status', pa.string()),
    ('user_id', pa.string()),
    ('salon_id', pa.int16()),
    ('payment_id', pa.string()),
    ('service_id', pa.int16()),
    ('service_name', pa.string()),
    ('price', pa.float32())
])

def handler(event, context):  
    try:
        logger(f"🚀  Iniciando función handler.")
        print(event)
        mensaje = json.loads(event['body'])
        print(mensaje)
        
        mensaje['salon_id'] = int(mensaje['salon_id'])
        mensaje['service_id'] = int(mensaje['service_id'])
        mensaje['price'] = float(mensaje['price'])

        logger("☑️  Mensaje recibido y cargado exitosamente")
        
        # Validación de campos requeridos y tipos
        validate_required_fields(mensaje, required_fields)
        logger("☑️  Validación de campos requeridos exitosa")

        validate_field_types(mensaje, field_types)
        logger("☑️  Validación de tipo de campos exitosa")
        
        BUCKET_NAME = 'analytics-bucket-s3'
        s3_client = boto3.client('s3')
        today = datetime.utcnow().strftime('%Y-%m-%d')

        # Listar archivos que comiencen con la fecha de hoy
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=today)
        existing_key = None

        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].startswith(today):
                    existing_key = obj['Key']
                    break

        date_str = mensaje['booking_date']
        dt_obj = datetime.fromisoformat(date_str)
        dt_obj_lima = dt_obj.replace(tzinfo=timezone.utc).astimezone(lima_tz)

        timestamp_s = int(dt_obj_lima.timestamp() * 1000)

        new_data = pa.Table.from_pydict({
            'booking_id': [mensaje['booking_id']],
            'booking_date': [timestamp_s],
            'status': [mensaje['status']],
            'user_id': [mensaje['user_id']],
            'salon_id': [mensaje['salon_id']],
            'payment_id': [mensaje['payment_id']],
            'service_id': [mensaje['service_id']],
            'service_name': [mensaje['service_name']],
            'price': [mensaje['price']],
        }, schema=schema)

        if existing_key:
            logger(f"☑️  Archivo existente encontrado: {existing_key}")

            # Descargar archivo
            s3_client.download_file(BUCKET_NAME, existing_key, '/tmp/existing.parquet')
            table = pq.read_table('/tmp/existing.parquet')

            combined_table = pa.concat_tables([table, new_data])
            logger("☑️  Datos combinados exitosamente")

        else:
            logger("☑️  No se encontró archivo existente. Creando nuevo.")
            combined_table = new_data

        # Guardar datos combinados en un archivo Parquet temporal
        file_name = f"{today}_data.parquet"
        pq.write_table(combined_table, f"/tmp/{file_name}")

        # Subir archivo a S3
        s3_client.upload_file(f"/tmp/{file_name}", BUCKET_NAME, file_name)
        logger(f"☑️  Archivo {file_name} subido a S3")

        return {
            "statusCode": 200,
            "body": "Reserva procesada satisfactoriamente"
        }

    except (ValueError, TypeError) as e:
        logger(f"🔴  Ocurrió un error: {str(e)}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
}

def testHandler():
    body1 = '''{
        "booking_id": "1",
        "booking_date": "2024-10-06T20:00:00",
        "status": "1",
        "user_id": "1",
        "salon_id": "1",
        "payment_id": "1",
        "service_id": "2",
        "service_name": "Hair cut",
        "price": "100.50"
        }'''
    body = ''' {\r\n  "booking_id": "1",\r\n  "booking_date": "2024-10-06T20:00:00",\r\n  "status": "1",\r\n  "user_id": "1",\r\n  "salon_id": "1",\r\n  "employee_id": "1",\r\n  "payment_id": "1",\r\n  "district_id": "1",\r\n  "service_id": "2",\r\n  "price": "100.50"\r\n  } '''
    response = handler({'body': body1}, None)
    logger(response.get('body'))

# testHandler()
