# Standard
from typing import Any
import os
from decimal import Decimal

# Third Party
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

postgres_conn = psycopg2.connect(os.environ['POSTGRES_URL'])
mongo_conn = MongoClient(os.environ['MONGO_URL'])
mongo_db = mongo_conn.projeto_db

def show_tables() -> list[str]:
    print("Fetching table names...")
    with postgres_conn.cursor() as db:
        db.execute("SHOW TABLES;")
        res = db.fetchall()
        postgres_conn.commit()
        return [table[1] for table in res]

def select_all(table_name: str) -> list[Any]:
    print(f"Selecting table '{table_name}' records ...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT * FROM {table_name};")
        res = db.fetchall()
        postgres_conn.commit()
        return res
    
def select_columns(table_name: str) -> list[Any]:
    print(f"Selecting table '{table_name}' columns names...")
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
            print(f"Creating '{table}' collection in MongoDB")
            mongo_db.create_collection(table)
        except:
            pass
        
        print(f"Inserting values in '{table}' collection")
        mongo_db[table].insert_many(records)

        print("\n-----------------------------------------------------------------------------\n")
    
    print("Data transference completed!")

def drop_all_collections():
    collections = mongo_db.list_collection_names()

    if len(collections) == 0:
        return

    print("Deleting all collections...")
    for collection in mongo_db.list_collection_names():
        mongo_db[collection].drop()
        print(f"Deleted '{collection}' collection successfully")


if __name__ == '__main__':
    drop_all_collections()
    transfer_data()