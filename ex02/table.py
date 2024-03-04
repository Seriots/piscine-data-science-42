
import pandas
import sqlalchemy


def main():
    FILE_PATH = "/mnt/nfs/homes/lgiband/sgoinfre/subject/customer/data_2022_dec.csv"
    try:
        engine = sqlalchemy.create_engine('postgresql://lgiband:mysecretpassword@localhost:5432/piscineds')
        print("engine loaded")
        name = FILE_PATH.split("/")[-1].split(".")[0]

        file = pandas.read_csv(FILE_PATH)
        print("file loaded")

        datatype = {
            'event_time': sqlalchemy.DateTime(),
            'event_type': sqlalchemy.String(),
            'product_id': sqlalchemy.Integer(),
            'price': sqlalchemy.Float(),
            'user_id': sqlalchemy.BigInteger(),
            'user_session': sqlalchemy.UUID(as_uuid=True)
        }

        file.to_sql(name, engine, if_exists='replace', index=False, dtype=datatype)
        print("file loaded into database")



    except Exception as e:
        print(e)

if __name__ == '__main__':
   main()