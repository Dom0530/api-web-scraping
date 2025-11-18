import requests
import boto3
import uuid
from decimal import Decimal

def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(v) for v in obj]
    else:
        return obj

def lambda_handler(event, context):
    url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query?where=1=1&outFields=*&f=json&resultRecordCount=200"

    response = requests.get(url)
    data = response.json()

    features = data.get("features", [])

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Limpiar tabla
    scan = table.scan()
    with table.batch_writer() as batch:
        for i in scan.get("Items", []):
            batch.delete_item(Key={"id": i["id"]})

    # Insertar datos nuevos
    with table.batch_writer() as batch:
        for f in features:
            item = f["attributes"]

            # Añadir ID único
            item["id"] = str(uuid.uuid4())

            # Añadir coordenadas
            if "geometry" in f:
                item["x"] = f["geometry"].get("x")
                item["y"] = f["geometry"].get("y")

            # Convertir floats a Decimal
            item = to_decimal(item)

            batch.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": f"{len(features)} registros insertados"
    }
