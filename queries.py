# Standard
import os
import json

# Third Party
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

mongo_conn = MongoClient(os.environ['MONGO_URL'])
mongo_db = mongo_conn.projeto_db


def query_student_academic_record():
    """
    1. histórico escolar de qualquer aluno, retornando o código e nome da disciplina, 
    semestre e ano que a disciplina foi cursada e nota final
    Buscando o histórico escolar do aluno de RA 100000001
    """
    print("Buscando o histórico escolar do aluno de RA 100000001")

    pipeline = [
        {"$match": {"student_id": "100000001"}}, 
        {"$lookup": {
            "from": "subj",         
            "localField": "subj_id",
            "foreignField": "id", 
            "as": "subject_info"  
        }},
        {"$unwind": "$subject_info"}, 
        {"$project": {
            "student_id": 1,
            "subj_id": 1,
            "subject_info.title": 1, 
            "semester": 1, 
            "year": 1,
            "grade": 1,
            "subjroom": 1
        }}
    ]

    result = mongo_db["takes"].aggregate(pipeline).to_list()

    for i, row in enumerate(result):
        result[i]["_id"] = str(row["_id"])
        result[i]["subj_title"] = row["subject_info"]["title"]
        result[i].pop("subject_info")
    
    with open('./output/query-1.json', 'w') as f:
        json.dump(result, f)


def query_professor_academic_record():
    """
    2. histórico de disciplinas ministradas por qualquer professor, com semestre e ano\
    Buscando o histórico de disciplinas ministradas pelo professor de ID P005
    """
    print("Buscando o histórico de disciplinas ministradas pelo professor de ID P005")

    pipeline = [
        {"$match": {"professor_id": "P005"}}, 
    ]

    result = mongo_db["teaches"].aggregate(pipeline).to_list()

    for i, row in enumerate(result):
        result[i]["_id"] = str(row["_id"])

    with open('./output/query-2.json', 'w') as f:
        json.dump(result, f)

def query_graduated_students():
    """
    3. listar alunos que já se formaram (foram aprovados em todos os cursos de uma matriz curricular) 
    em um determinado semestre de um ano
    Buscando os alunos que se formaram no segundo semestre de 2018
    """
    print("Buscando os alunos que se formaram no segundo semestre de 2018")

    pipeline = [
        {"$match": {"$and": [ { "semester": 2 }, { "year": 2018 } ]}}, 
    ]

    result = mongo_db["graduate"].aggregate(pipeline).to_list()

    for i, row in enumerate(result):
        result[i]["_id"] = str(row["_id"])

    with open('./output/query-3.json', 'w') as f:
        json.dump(result, f)

if __name__ == '__main__':
    print("Outputs estarão na pasta ./output")
    if not os.path.exists('./output'):
        os.mkdir('./output')
    query_student_academic_record()
    query_professor_academic_record()
    query_graduated_students()

