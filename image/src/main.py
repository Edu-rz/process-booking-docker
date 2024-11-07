import json
import boto3
import uuid
from datetime import datetime
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

def handler(event, context):  
    try:
        mensaje = json.loads(event['body'])
        print(mensaje)

        # Convertir los valores que deberían ser enteros
        mensaje['booking_id'] = int(mensaje['booking_id'])
        mensaje['status'] = int(mensaje['status'])
        mensaje['user_id'] = int(mensaje['user_id'])
        mensaje['salon_id'] = int(mensaje['salon_id'])
        mensaje['employee_id'] = int(mensaje['employee_id'])
        mensaje['payment_id'] = int(mensaje['payment_id'])

        print("#################")
        print(mensaje)
        print("#################")

        # Campos requeridos y sus tipos esperados
        required_fields = ['booking_id', 'booking_date', 'status', 'user_id', 'salon_id', 'employee_id', 'payment_id']
        field_types = {
            'booking_id': int,
            'booking_date': 'timestamp',
            'status': int,
            'user_id': int,
            'salon_id': int,
            'employee_id': int,
            'payment_id': int
        }
        
        
        # Validación de campos requeridos y tipos
        validate_required_fields(mensaje, required_fields)
        print("#################")
        print("PASO VALIDACION 1")
        print("#################")

        validate_field_types(mensaje, field_types)
        print("#################")
        print("PASO VALIDACION 2")
        print("#################")
        
        
        # Crear un esquema de Parquet
        schema = pa.schema([
            ('booking_id', pa.int32()),
            ('booking_date', pa.timestamp('s')),
            ('status', pa.int32()),
            ('user_id', pa.int32()),
            ('salon_id', pa.int32()),
            ('employee_id', pa.int32()),
            ('payment_id', pa.int32())
        ])
        
        print("#################")
        print(schema)
        print("#################")
        
        # Convertir el campo 'booking_date' a un objeto datetime
        booking_date = datetime.fromisoformat(mensaje['booking_date'])
        
        # Crear una tabla de Parquet
        table = pa.Table.from_pydict({
            'booking_id': [mensaje['booking_id']],
            'booking_date': [booking_date],
            'status': [mensaje['status']],
            'user_id': [mensaje['user_id']],
            'salon_id': [mensaje['salon_id']],
            'employee_id': [mensaje['employee_id']],
            'payment_id': [mensaje['payment_id']]
        }, schema=schema)
        
        print("#################")
        print(table)
        print("#################")
        
        # Generar un nombre único para el archivo Parquet
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        file_name = f"{timestamp}_{uuid.uuid4()}.parquet"
        
        print("#################")
        print(timestamp)
        print(file_name)
        print("#################")
        
        # Escribir el archivo Parquet en S3
        BUCKET_NAME = 'analytics-bucket-s3'
        s3_client = boto3.client('s3')
        pq.write_table(table, f"/tmp/{file_name}")
        s3_client.upload_file(f"/tmp/{file_name}", BUCKET_NAME, file_name)
        
        print(f"Reserva {mensaje['booking_id']} almacenada en {file_name}")
        
        return {
            "statusCode": 200,
            "body": "Reserva subida satisfactoriamente"
        }

    except (ValueError, TypeError) as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

def testHandler():
    body = {
        "booking_id": "1",
        "booking_date": "2024-10-06T20:00:00",
        "status": "1",
        "user_id": "1",
        "salon_id": "1",
        "employee_id": "1",
        "payment_id": "1"
    }
    a = ''' {\r\n  "booking_id": "1",\r\n  "booking_date": "2024-10-06T20:00:00",\r\n  "status": "1",\r\n  "user_id": "1",\r\n  "salon_id": "1",\r\n  "employee_id": "1",\r\n  "payment_id": "1"\r\n} '''

    response = handler({'body': a}, None)
    print(response)

# testHandler()
