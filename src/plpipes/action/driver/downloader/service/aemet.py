from ..helpers import download_json

import plpipes.database as database
from plpipes.config import cfg

import json

def download(path, params, db, acfg):
    if path == "prediccion/especifica/municipio/horaria":
        return _download_prediccion_especifica_municipio_horaria(params, db, acfg)
    else:
        raise ValueError("Unknown/unsupported path")

def _download_prediccion_especifica_municipio_horaria(params, db, acfg):
    codmun = params["codmun"]
    res = download_json(f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{codmun}",
                        headers={'api_key': cfg["service.aemet.api_key"]})

    if res["estado"] != 200:
        raise Exception(f"Download failed, code: {res['estado']}, description: {res['descripcion']}")

    res1 = download_json(res["datos"])[0]
    table_name = "aemet_prediccion_especifica_municipio_horaria"
    database.create_empty_table(table_name, "codmun INT, elaborado TEXT, value TEXT, unique(codmun, elaborado)")


    database.execute(f"""
INSERT INTO {table_name} (codmun, elaborado, value) VALUES (?, ?, ?)
    ON CONFLICT(codmun, elaborado) DO UPDATE SET value=excluded.value;
""",
                     codmun, res1["elaborado"], json.dumps(res1), commit=True)
