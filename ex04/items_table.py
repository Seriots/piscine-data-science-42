import pandas
import sqlalchemy


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


def table_exists(engine: sqlalchemy.engine.base.Engine, table_name: str):
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
        engine = sqlalchemy.create_engine(
            'postgresql://lgiband:mysecretpassword@localhost:5432/piscineds'
            )
        if not table_exists(engine, table_name):
            datatype = {
                'product_id': sqlalchemy.Integer(),
                'category_id': sqlalchemy.BigInteger(),
                'category_code': sqlalchemy.String(255),
                'brand': sqlalchemy.String(255),
            }
            print(f"Creating table {table_name}")

            tmp_dataset = dataset
            for i in range(0, len(dataset), CHUNK_SIZE):
                print(f"\r[{int(i/len(dataset) * 100)}%\
-- {len(tmp_dataset)}]   ", end="")
                tmp_dataset[:min(CHUNK_SIZE, len(tmp_dataset))].to_sql(
                    table_name,
                    engine,
                    if_exists='append',
                    index=False,
                    dtype=datatype
                    )
                tmp_dataset = tmp_dataset[CHUNK_SIZE:]
            print("\r[100% -- 0]   ")
        else:
            print(f"Table {table_name} already exists")

        engine.dispose()
    except Exception as e:
        print(f"Error :{e}")


def main():
    DEFAULT_PATH = \
        "/mnt/nfs/homes/lgiband/sgoinfre/subject/item/item.csv"

    path = DEFAULT_PATH

    if not path:
        print("Invalid path")
        return

    dataset = load(path)
    if dataset is not None:
        name = "item"
        push_dataset(dataset, name)
    else:
        print(f"{path}: Failed to load the dataset")


if __name__ == '__main__':
    main()
