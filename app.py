# Standard
from typing import Any
import os
from decimal import Decimal

# Third Party
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

# Local
from queries import query_chiefs_of_departments, query_graduated_students, query_professor_academic_record, query_student_academic_record, query_tcc_group

load_dotenv()

postgres_conn = psycopg2.connect(os.environ['POSTGRES_URL'])
mongo_conn = MongoClient(os.environ['MONGO_URL'])
mongo_db = mongo_conn.projeto_db

def show_tables() -> list[str]:
    print("Buscando nomes das tabelas no banco relacional...")
    with postgres_conn.cursor() as db:
        db.execute("SHOW TABLES;")
        res = db.fetchall()
        postgres_conn.commit()
        return [table[1] for table in res]

def select_all(table_name: str) -> list[Any]:
    print(f"Selecionado registros da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT * FROM {table_name};")
        res = db.fetchall()
        postgres_conn.commit()
        return res
    
def select_columns(table_name: str) -> list[Any]:
    print(f"Selecionando os nomes das colunas da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}';")
        res = db.fetchall()
        postgres_conn.commit()
        return [table[3] for table in res]
    
def transfer_data():
    tables = show_tables()

    print("\n-----------------------------------------------------------------------------\n")

    for table in tables:
        cols = select_columns(table)
        rows = select_all(table)

        records = []

        for row in rows:
            formatted = []
            for value in row:
                if isinstance(value, Decimal):
                    formatted.append(float(value))
                else:
                    formatted.append(value)   

            records.append(dict(zip(cols, formatted)))

        try:
            print(f"Criando a coleção '{table}' no MongoDB...")
            mongo_db.create_collection(table)
        except:
            pass
        
        print(f"Inserindo os valores na coleção '{table}'...")
        mongo_db[table].insert_many(records)

        print("\n-----------------------------------------------------------------------------\n")
    
    print("Transferência concluída!")

    print("\n-----------------------------------------------------------------------------\n")

def drop_all_collections():
    collections = mongo_db.list_collection_names()

    if len(collections) == 0:
        return

    print("Deletando todas as coleções...")
    for collection in mongo_db.list_collection_names():
        mongo_db[collection].drop()
        print(f"Coleção '{collection}' deletada com sucesso!")


if __name__ == '__main__':
    drop_all_collections()
    transfer_data()

    print("Outputs estarão na pasta ./output")
    print("\n-----------------------------------------------------------------------------\n")

    if not os.path.exists('./output'):
        os.mkdir('./output')

    query_student_academic_record()
    query_professor_academic_record()
    query_graduated_students()
    query_chiefs_of_departments()
    query_tcc_group()

    print("\n-----------------------------------------------------------------------------\n")
    print("Outputs das queries disponíveis na pasta output!")
    print("\n-----------------------------------------------------------------------------\n")