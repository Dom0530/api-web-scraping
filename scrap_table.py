import requests
import boto3
import uuid
import datetime

DDB_TABLE_NAME = "TablaWebScrapping"  # cambia si es necesario

QUERY_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/"
    "monitoreocensis/SismosReportados/MapServer/0/query"
)

def arcgis_time_to_iso(ms):
    if ms is None:
        return None
    # ArcGIS timestamps are milliseconds since epoch
    dt = datetime.datetime.utcfromtimestamp(ms / 1000.0)
    return dt.isoformat() + "Z"

def lambda_handler(event, context):
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 2000
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    r = requests.get(QUERY_URL, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()

    features = data.get("features", [])

    # Preparar DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DDB_TABLE_NAME)

    # Limpiar tabla (opcional)
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    rows = []

    for f in features:
        attr = f.get("attributes", {})
        geom = f.get("geometry", {})

        item = {
            "id": str(uuid.uuid4()),

            # Coordenadas
            "lat": geom.get("y"),
            "lon": geom.get("x"),

            # Campos principales
            "objectid": attr.get("objectid"),
            "fecha": arcgis_time_to_iso(attr.get("fecha")),
            "hora": attr.get("hora"),
            "prof": attr.get("prof"),
            "ref": attr.get("ref"),
            "intensidad": attr.get("int_"),
            "profundidad": attr.get("profundidad"),
            "sentido": attr.get("sentido"),
            "magnitud": attr.get("magnitud"),
            "departamento": attr.get("departamento"),
            "mag": attr.get("mag"),
            "code": attr.get("code"),
            "fechaevento": arcgis_time_to_iso(attr.get("fechaevento")),
            "reporte": attr.get("reporte"),
        }

        # DynamoDB no acepta None → remover claves con None
        item = {k: v for k, v in item.items() if v is not None}

        rows.append(item)
        table.put_item(Item=item)

    return {
        "statusCode": 200,
        "count": len(rows),
        "body": rows[:5]  # devolver solo un preview
    }
