from pymongo import MongoClient
import json

def conectar_mongo():
    cliente = MongoClient('localhost', 27017)
    db = cliente['projeto_dados']
    return db

def salvar_no_mongo(data, colecao):
    db = conectar_mongo()
    colecao = db[colecao]
    colecao.insert_many(data)
