
import pandas
import sqlalchemy
import sys
import os


def get_loadable_path(base_path: str):
    """Return a list of loadable paths from a directory or
        return the file path or
        return None if the path is invalid"""
    if os.path.isdir(base_path):
        return [os.path.join(base_path, f) for f in os.listdir(base_path) if f.endswith(".csv")]
    elif os.path.isfile(base_path):
        return [base_path]
    else:
        return None
            

def load(path: str) -> pandas.DataFrame:
    """Load a CSV file
    Args: path (str): Path to he CSV file
    Returns: pandas.DataFrame: Return the dataset
    """
    try:
        if not path.endswith(".csv"):
            raise ValueError("File is not a CSV")
        if not path:
            raise ValueError("File path is empty")
        dataset = pandas.read_csv(path)
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    print(f"Loading dataset: {path} of dimensions {dataset.shape}")
    return dataset

def table_exists(engine: sqlalchemy.engine.base.Engine, table_name: str) -> bool:
    """Check if a table exists in the database
    Args: engine (sqlalchemy.engine.base.Engine): The database engine
    Args: table_name (str): The table name
    Returns: bool: Return True if the table exists, False otherwise
    """
    return sqlalchemy.inspect(engine).has_table(table_name)



def push_dataset(dataset: pandas.DataFrame, table_name: str):
    """Push a dataset to the database
    Args: dataset (pandas.DataFrame): The dataset to push
    """
    CHUNK_SIZE = 10000
    try:
        engine = sqlalchemy.create_engine('postgresql://lgiband:mysecretpassword@localhost:5432/piscineds')
        if not table_exists(engine, table_name):
            datatype = {
                'event_time': sqlalchemy.DateTime(),
                'event_type': sqlalchemy.String(255),
                'product_id': sqlalchemy.Integer(),
                'price': sqlalchemy.Float(),
                'user_id': sqlalchemy.BigInteger(),
                'user_session': sqlalchemy.UUID(as_uuid=True),
            }
            print(f"Creating table {table_name}")
            
            tmp_dataset = dataset
            for i in range(0, len(dataset), CHUNK_SIZE):
                print(f"\r[{int(i/len(dataset) * 100)}% -- {len(tmp_dataset)}]   ", end="")
                tmp_dataset[:min(CHUNK_SIZE, len(tmp_dataset))].to_sql(table_name, engine, if_exists='append', index=False, dtype=datatype)
                tmp_dataset = tmp_dataset[CHUNK_SIZE:]
            print(f"\r[100% -- 0]   ")
        else:
            print(f"Table {table_name} already exists")
        
        engine.dispose()
    except Exception as e:
        print(f"Error :{e}")


def main():
    DEFAULT_PATH = "/home/seriots/42_cursus/piscine-data-science-42/subject/customer/data_2022_dec.csv"
    try:
        assert len(sys.argv) <= 2, "Usage: python table.py <file.csv>"
    except AssertionError as e:
        print(e)
        return
    
    if len(sys.argv) == 1:
        path = [DEFAULT_PATH]
    else:
        path = get_loadable_path(sys.argv[1])
    
    if not path:
        print("Invalid path")
        return
    
    for p in path:
        dataset = load(p)
        if dataset is not None:
            name = os.path.basename(p).split(".")[0]
            push_dataset(dataset, name)
        else:
            print(f"{p}: Failed to load the dataset")

    # FILE_PATH = "/home/seriots/42_cursus/piscine-data-science-42/subject/customer/"# "/mnt/nfs/homes/lgiband/sgoinfre/subject/customer/data_2022_dec.csv"
    # FILENAME = "data_2022_oct.csv"
    # try:
    #     engine = sqlalchemy.create_engine('postgresql://lgiband:mysecretpassword@localhost:5432/piscineds')
    #     print("engine loaded")

    #     file = pandas.read_csv(FILE_PATH + FILENAME)
    #     print("file loaded")

    #     datatype = {
    #         'event_time': sqlalchemy.DateTime(),
    #         'event_type': sqlalchemy.String(255),
    #         'product_id': sqlalchemy.Integer(),
    #         'price': sqlalchemy.Float(),
    #         'user_id': sqlalchemy.BigInteger(),
    #         'user_session': sqlalchemy.UUID(as_uuid=True)
    #     }

    #     file.to_sql(FILENAME, engine, if_exists='replace', index=False, dtype=datatype)
    #     print("file loaded into database")

    #     engine.dispose()


    # except Exception as e:
    #     print(e)

if __name__ == '__main__':
   main()