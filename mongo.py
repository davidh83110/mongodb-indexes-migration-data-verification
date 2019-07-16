import pymongo
from connections import Constant

class Mongo():

    def __init__(self, target_uri, target_db):
        ## mognodb+srv://{ACCOUNT}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}
        self.target_uri = target_uri
        self.target_db = target_db

    def get_conn(self):
        return pymongo.MongoClient(self.target_uri).get_database(self.target_db)

    def list_collection_names(self):
        return self.get_conn().list_collection_names()

    def count_documents(self, collection_name):
        return self.get_conn().get_collection(collection_name).count_documents({})

    def list_indexes(self, collection_name):
        return self.get_conn().get_collection(collection_name).list_indexes()

    def create_index(self, collection_name, index_name, sorting):
        return self.get_conn().get_collection(collection_name).create_index([(index_name, sorting)])


atlas = []
dw3 = []

class Handler():
    def __init__(self, source, target, db):
        self.source = source
        self.target = target
        self.db = db
        self.source_conn = Mongo(source, db)
        self.target_conn = Mongo(target, db)


    def count_diff(self):
        source_list = []
        target_list = []

        for col in self.source_conn.list_collection_names():
            source_list.append(col+"_"+str(self.source_conn.count_documents(col)))

        for col in self.target_conn.list_collection_names():
            target_list.append(col+"_"+str(self.target_conn.count_documents(col)))

        return print(set(source_list) - set(target_list))


    def index_migrate(self):
        for indexes in self.source_conn.list_collection_names():
            for index in self.source_conn.list_indexes(indexes):
                execute_index = list(index.to_dict()['key'])
                execute_collection = index.to_dict()['ns'].split('.')[1]

                if execute_index[1] == [-1]:
                    print("-1 index")
                    self.target_conn.create_index(execute_collection, execute_index[0], pymongo.DESCENDING)

                else:
                    print("1 index")
                    self.target_conn.create_index(execute_collection, execute_index[0], pymongo.ASCENDING)
                





if __name__ == "__main__":
    source_uri = Constant.atlas_client
    target_uri = Constant.dw3_client
    db = 'shopline_dev'

    Handler(source_uri, target_uri, db).count_diff()

