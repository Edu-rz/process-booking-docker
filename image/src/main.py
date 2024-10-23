import json
import boto3
import uuid
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

def handler(event, context):  
    s3_client = boto3.client('s3')
    BUCKET_NAME = 'analytics-bucket-s3'
    PREFIX = 'bookings/'

    for record in event['Records']:
        # Parsear el mensaje de SQS
        mensaje = json.loads(record['body'])
        
        print("#################")
        print(mensaje)
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
        
        booking_date_str = mensaje['booking_date']
        booking_date = datetime.fromisoformat(booking_date_str)
        
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
        pq.write_table(table, f"/tmp/{file_name}")
        s3_client.upload_file(f"/tmp/{file_name}", BUCKET_NAME, file_name)
        
        print(f"Reserva {mensaje['booking_id']} almacenada en {file_name}")
        
  
