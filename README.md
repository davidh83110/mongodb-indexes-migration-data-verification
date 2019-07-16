# MongoDB Migration
- To migrate MongoDB indexes and verify.
- To compare the `db.collection.count()` between the source and targtet database.


## Environment
- Python >= 3.4
- pymongo >=3.6



## Structuring
- Class `Mongo` to call pymongo to operate database
- Class `Handler` to do `count_diff` / `index_migrate` / `index_diff`


## How to start ?
- Add a `connections.py` file and export your source and target uri as well as database name.

example: 
```python
source_client = "mongodb://12.23.24.25:27017/test_db"
target_client = "mongodb://23.44.55.111:27017/test_db"
db = "test_db"
```



- Call the function you want 

example:
```python
if __name__ == "__main__":
    source_uri = Constant.source_cluent
    target_uri = Constant.target_client
    db = Constant.db

    Handler(source_uri, target_uri, db).index_migrate()
    Handler(source_uri, target_uri, db).index_diff()

```

