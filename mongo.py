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
            source_list.append(col + "_" + str(self.source_conn.count_documents(col)))

        for col in self.target_conn.list_collection_names():
            target_list.append(col + "_" + str(self.target_conn.count_documents(col)))

        return print(set(source_list) - set(target_list))

    
    def index_diff(self):
        source_index_list = []
        target_index_list = []

        for col in self.source_conn.list_collection_names():
            for index in self.source_conn.list_indexes(col):
                ## append "{'_id': 1}_orders" as example
                source_index_list.append(str(index.to_dict()['key']) + "_" + str(index.to_dict()['ns'].split('.')[1]))

        for col in self.target_conn.list_collection_names():
            for index in self.target_conn.list_indexes(col):
                ## append "{'_id': 1}_orders" as example
                target_index_list.append(str(index.to_dict()['key']) + "_" + str(index.to_dict()['ns'].split('.')[1]))

        return print(set(source_index_list) - set(target_index_list))


    def index_migrate(self):
        for col in self.source_conn.list_collection_names():
            for index in self.source_conn.list_indexes(col):
                execute_collection = index.to_dict()['ns'].split('.')[1]
                print(f'starting collection {execute_collection} .....')

                if list(index.to_dict()['key'].values()) == [-1]:
                    pass
                    # print("-1 index")
                    # self.target_conn.create_index(execute_collection, execute_index[0], pymongo.DESCENDING)

                elif list(index.to_dict()['key'].values()) == [1]:
                    pass
                    # print("1 index")
                    # self.target_conn.create_index(execute_collection, execute_index[0], pymongo.ASCENDING)

                else:
                    print(f'exception... {index}, {execute_collection}')
                





if __name__ == "__main__":
    source_uri = Constant.dw3_client
    target_uri = Constant.atlas_client
    db = Constant.db

    Handler(source_uri, target_uri, db).index_diff()

