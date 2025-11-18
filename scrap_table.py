import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {"statusCode": response.status_code, "body": "Error al acceder"}

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    if not table:
        return {"statusCode": 404, "body": "Tabla no encontrada"}

    rows = []

    # recorrer filas reales
    for tr in table.find("tbody").find_all("tr"):

        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        # 1. Reporte sísmico: viene con <br>, separar líneas
        reporte_raw = tds[0].get_text("\n", strip=True).split("\n")
        reporte = reporte_raw[0]
        codigo = reporte_raw[1]

        referencia = tds[1].get_text(strip=True)
        fecha_hora = tds[2].get_text(strip=True)
        magnitud = tds[3].get_text(strip=True)

        link = tds[4].find("a")["href"] if tds[4].find("a") else None

        rows.append({
            "reporte": reporte,
            "codigo": codigo,
            "referencia": referencia,
            "fecha_hora_local": fecha_hora,
            "magnitud": float(magnitud),
            "url_reporte": "https://ultimosismo.igp.gob.pe" + link if link else None
        })

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_dynamo = dynamodb.Table("TablaWebScrapping")

    # borrar antiguos
    scan = table_dynamo.scan()
    with table_dynamo.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # insertar nuevos
    for i, row in enumerate(rows, start=1):
        row["#"] = i
        row["id"] = str(uuid.uuid4())
        table_dynamo.put_item(Item=row)

    return {"statusCode": 200, "body": rows}
