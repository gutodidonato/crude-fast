import cx_Oracle
import json

with open("bloco.json") as file:
    data = json.load(file)
    hostname = data["hostname"]
    port = data["port"]
    sid = data["sid"]
    username = data["username"]
    password = data["password"]

dsn_tns = cx_Oracle.makedsn(hostname, port, sid)
connection = cx_Oracle.connect(username, password, dsn_tns)


def listarFocos(latitude, longitude):
    focos = []
    try:
        cursor = connection.cursor()
        valor_total = abs(latitude + longitude) 
        query = "SELECT where ABS(latitude) + ABS(longitude) - valor_total :valor_total <= 0.5 FROM FOCOS_LIXO"
        cursor.execute(query, (valor_total))
        for row in cursor:
            print(row)
            focos.append(row)
    finally:
        cursor.close()
        connection.close()


def atribuir(pessoa, id_empresa):
    try:
        cursor = connection.cursor()
        query = (
            "update EMPRESAS_RED set pessoa = :pessoa where id_empresa = :id_empresa"
        )
        cursor.execute(query, (pessoa, id_empresa))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def alterar_descricao(descricao, id_empresa):
    try:
        cursor = connection.cursor()
        query = "update EMPRESAS_RED set descricao = :descricao where id_empresa = :id_empresa"
        cursor.execute(query, (descricao, id_empresa))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def alterar_prioridade(prioridade, id_empresa):
    try:
        cursor = connection.cursor()
        query = "update EMPRESAS_RED set prioridade = :prioridade where id_empresa = :id_empresa"
        cursor.execute(query, (prioridade, id_empresa))
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def inserir_empresa(prioridade, pessoa, descricao):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT NEXTVAL('EMPRESAS_RED_ID_SEQ')")
        next_id = cursor.fetchone()[0]
        query = "INSERT INTO EMPRESAS_RED (ID_EMPRESA, PRIORIDADE, PESSOA, DESCRICAO) VALUES (:id_empresa, :prioridade, :pessoa, :descricao)"
        cursor.execute(query, (next_id, prioridade, pessoa, descricao))
        connection.commit()

    except cx_Oracle.DatabaseError as e:
        (error,) = e.args
        print("Erro:", error.message)
        connection.rollback()
