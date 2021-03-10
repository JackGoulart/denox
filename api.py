import json
from tornado import web, ioloop, httpserver
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from sklearn.metrics.pairwise import haversine_distances
from math import radians


def calcula_distancia_sklearn(lon1, lat1, lon2, lat2):
    inicio = [radians(_) for _ in [lat1, lon1]]
    fim = [radians(_) for _ in [lat2, lon2]]
    result = haversine_distances([inicio, fim])
    return result * 6371


def converte_string_data(st):
    return datetime.strptime(st, "%d/%m/%Y %H:%M:%S").astimezone()


class CalculaMetricasHandler(web.RequestHandler):

    def post(self):
        db = self.settings['db']
        payload = json.loads(self.request.body)

        datahora_inicio = converte_string_data(payload["datahora_inicio"])
        datahora_fim = converte_string_data(payload["datahora_fim"])

        procurda_pelo_serial = db["dados_rastreamento"].find({"serial": payload["serial"]}, {"_id": False})
        consulta_frame = pd.DataFrame(data=procurda_pelo_serial)

        consulta_frame["datahora"] = consulta_frame["datahora"].astype(int)
        consulta_frame["datahora"] = pd.to_datetime(consulta_frame["datahora"], unit='s') \
            .dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')

        filtro_datahora = (consulta_frame["datahora"] >= datahora_inicio) \
                          & (consulta_frame["datahora"] <= datahora_fim)

        resutado_filtro_data_frame = pd.DataFrame(data=consulta_frame.loc[filtro_datahora])

        posicao_inicial = resutado_filtro_data_frame.head(1)
        longitude_inicial = float(posicao_inicial["longitude"].item())
        latitude_inicial = float(posicao_inicial["latitude"].item())

        posicao_final = resutado_filtro_data_frame.tail(1)
        longitude_final = float(posicao_final["longitude"].item())
        latitude_final = float(posicao_final["latitude"].item())

        distancia = calcula_distancia_sklearn(longitude_inicial, latitude_inicial, longitude_final, latitude_final)
        distancia = distancia[0][1]

        frame_situacao_movimento = pd.DataFrame(data=resutado_filtro_data_frame)
        frame_situacao_parado = pd.DataFrame(data=resutado_filtro_data_frame)

        frame_situacao_movimento["situacao_movimento"] = \
            frame_situacao_movimento["situacao_movimento"] == "false"

        situacao_movimento = frame_situacao_movimento["datahora"].tail(1).item() \
                             - frame_situacao_movimento["datahora"].head(1).item()
        situacao_movimento = situacao_movimento.total_seconds()

        frame_situacao_parado["situacao_movimento"] = \
            frame_situacao_parado["situacao_movimento"] == "false"
        situacao_parado = frame_situacao_parado["datahora"].tail(1).item() \
                          - frame_situacao_parado["datahora"].head(1).item()
        situacao_parado = situacao_parado.total_seconds()

        resultado_metricas = dict(distancia_percorrida=distancia,
                                  tempo_em_movimento=situacao_movimento,
                                  tempo_parado=situacao_parado,
                                  centroides_paradas="",
                                  serial=payload["serial"])

        resultado_metricas_payload = json.dumps(resultado_metricas)

        db["resultados_jackson"].insert_one(resultado_metricas)

        self.set_header("content-type", "application/json")
        self.write(resultado_metricas_payload)


class RetornaMetricasHandler(web.RequestHandler):
    def get(self):
        db = self.settings['db']
        resultado = db["resultados_jackson"].find({}, {"_id": False})
        self.set_header("content-type", "application/json")
        self.write(json.dumps([r for r in resultado]))


app = web.Application([
    (r'/api/calcula_metricas', CalculaMetricasHandler),
    (r'/api/retorna_metricas', RetornaMetricasHandler)
])

if __name__ == '__main__':
    server = httpserver.HTTPServer(app)
    server.bind(8888, "0.0.0.0")
    server.start(0)
    app.settings['db'] = MongoClient("mongodb://localhost:27017")['denox']
    print("Disponivel na porta 8888")
    ioloop.IOLoop.current().start()
