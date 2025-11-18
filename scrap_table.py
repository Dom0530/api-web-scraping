import requests
import boto3
import uuid

def lambda_handler(event, context):
    # API JSON del IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados/api"

    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder a la API del IGP"
        }

    data = response.json().get("data", [])

    if not data:
        return {
            "statusCode": 404,
            "body": "No se encontraron datos de sismos"
        }

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaWebScrapping")

    # Borrar tabla antes de subir nuevos datos
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar nuevos sismos
    for i, row in enumerate(data, start=1):
        row["#"] = i
        row["id"] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        "statusCode": 200,
        "body": data
    }
