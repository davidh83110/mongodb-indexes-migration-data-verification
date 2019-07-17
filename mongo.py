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

    def index_information(self, collection):
        return self.get_conn().get_collection(collection).index_information()

    def create_index(self, collection_name, index_name):
        return self.get_conn().get_collection(collection_name).create_index(index_name, background=True)



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


    def single_index_diff(self, collection_name):
        source_index_list = []
        target_index_list = []

        for index in self.source_conn.list_indexes(collection_name):
            ## append "{'_id': 1}_orders" as example
            source_index_list.append(str(index.to_dict()['key']) + "_" + str(index.to_dict()['ns'].split('.')[1]))


        for index in self.target_conn.list_indexes(collection_name):
            ## append "{'_id': 1}_orders" as example
            target_index_list.append(str(index.to_dict()['key']) + "_" + str(index.to_dict()['ns'].split('.')[1]))

        return print((set(target_index_list) - set(source_index_list)), (set(source_index_list) - set(target_index_list)))


    def single_collection_indexes_migrate(self, collection_name):
        print(f'collection: {collection_name}')
        for indexes in list(self.source_conn.index_information(collection_name).values()):
            to_be_create_indexes = []
            print(f"""source index: {indexes.get('key')} """)

            for index in indexes.get('key'):
                list(index)[1] = int(list(index)[1])
                to_be_create_indexes.append((index[0] , int(list(index)[1])))

            print(f'target index: {to_be_create_indexes}')
            self.target_conn.create_index(collection_name, to_be_create_indexes) if to_be_create_indexes != [('_id', 1)] else print(f'exception: {to_be_create_indexes}')

        print(f'{collection_name} collection indexes done migrated')


    def index_migrate(self):
        for col in self.source_conn.list_collection_names():
            print(f'collection: {col}')
            for indexes in list(self.source_conn.index_information(col).values()):
                to_be_create_indexes = []
                print(f"""source index: {indexes.get('key')} """)

                for index in indexes.get('key'):
                    list(index)[1] = int(list(index)[1])
                    to_be_create_indexes.append((index[0] , int(list(index)[1])))

                print(f'target index: {to_be_create_indexes}')
                self.target_conn.create_index(col, to_be_create_indexes) if to_be_create_indexes != [('_id', 1)] else print(f'exception: {to_be_create_indexes}')

            print(f'{col} collection indexes done migrated')
                





if __name__ == "__main__":
    source_uri = Constant.dw3_client
    target_uri = Constant.atlas_client
    db = Constant.db

    # Handler(source_uri, target_uri, db).index_migrate()
    Handler(source_uri, target_uri, db).single_collection_indexes_migrate('orders')
    Handler(source_uri, target_uri, db).single_index_diff('orders')
