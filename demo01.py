from pymongo import MongoClient


def generate_mongo_collection(db_name, col_name):
    client = MongoClient("47.107.180.248", 27017)
    db = client["{}".format(db_name)]
    col = db["{}".format(col_name)]
    return col


test_coll = generate_mongo_collection("utilstest", "test01")
while True:
    try:
        test_coll.insert({"name": "furuiyang", "age": 24})
    except Exception as e:
        print(e)
