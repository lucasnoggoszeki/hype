import pandas as pd
import requests
from typing import Any, Dict
import logging
import time
from google.cloud import bigquery


def get_response(url: str, retries: int = 1) -> Dict[str, Any]:

    if retries == 6:
        logging.warning(
            f"Request has reached the limit of {retries} retries. Reason: {response}."
        )
        return None

    response = requests.get(url)

    if response.status_code not in [200, 429, 500]:
        response.raise_for_status()

    if response.status_code in [429, 500]:
        logging.warning(
            f"Request has failed {retries} time(s). Reason: {response}.")
        time.sleep(2)
        response = get_response(url, retries + 1)

    return response.json()


def get_ipca_from_response(response: dict) -> Dict[str, str]:
    ipca = [
        [k, v]
        for i in response
        for ii in i["resultados"]
        for s in ii["series"]
        for k, v in s["serie"].items()
    ]
    return ipca


def convert_scalar_values_to_df(content: dict, clns: list) -> pd.DataFrame:
    return pd.DataFrame(content, columns=clns)


def trunc_yyyymm_to_datetime(s: pd.Series) -> pd.DataFrame:
    return pd.to_datetime(s, format='%Y%m', errors='coerce')


def convert_to_double(s: pd.Series) -> pd.DataFrame:
    return s.astype(float)


def insert_to_bq(pjct: str, table_id: str, schema: Dict[str, str], data: pd.DataFrame) -> None:

    client = bigquery.Client(project=pjct)
    fields = [bigquery.SchemaField(col, schema[col]) for col in schema]
    config = bigquery.LoadJobConfig(schema=fields)
    job = client.load_table_from_dataframe(data, table_id, job_config=config)
    job.result()


def main(url: str) -> None:

    response = get_response(url)
    if response is None:
        return
    content = get_ipca_from_response(response)
    df = convert_scalar_values_to_df(content, ['ano_mes', 'valor'])
    df.ano_mes = trunc_yyyymm_to_datetime(df.ano_mes)
    df.valor = convert_to_double(df.valor)
    schema = {
        "ano_mes": "DATE",
        "valor": "FLOAT",
    }

    print(df)

    # Insere os dados no Bigquery
    # insert_to_bq("thinking-return-375018", "raw_api.ipca_api", schema, df)


if __name__ == "__main__":

    import os

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "permissions/thinking-return-375018-94abc2fde152.json"

    url = "https://servicodados.ibge.gov.br/api/v3/agregados/1420/periodos/201201%7C201202%7C201203%7C201204%7C201205%7C201206%7C201207%7C201208%7C201209%7C201210%7C201211%7C201212%7C201301%7C201302%7C201303%7C201304%7C201305%7C201306%7C201307%7C201308%7C201309%7C201310%7C201311%7C201312%7C201401%7C201402%7C201403%7C201404%7C201405%7C201406%7C201407%7C201408%7C201409%7C201410%7C201411%7C201412%7C201501%7C201502%7C201503%7C201504%7C201505%7C201506%7C201507%7C201508%7C201509%7C201510%7C201511%7C201512%7C201601%7C201602%7C201603%7C201604%7C201605%7C201606%7C201607%7C201608%7C201609%7C201610%7C201611%7C201612%7C201701%7C201702%7C201703%7C201704%7C201705%7C201706%7C201707%7C201708%7C201709%7C201710%7C201711%7C201712%7C201801%7C201802%7C201803%7C201804%7C201805%7C201806%7C201807%7C201808%7C201809%7C201810%7C201811%7C201812%7C201901%7C201902%7C201903%7C201904%7C201905%7C201906%7C201907%7C201908%7C201909%7C201910%7C201911%7C201912/variaveis/306?localidades=N1%5Ball%5D&classificacao=315%5B7169%5D"

    main(url)
