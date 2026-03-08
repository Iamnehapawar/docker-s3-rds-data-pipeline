import boto3
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

# S3 Configuration
S3_BUCKET = "devops-data-pipeline-bucket-123"
S3_KEY = "data.csv"

# RDS Configuration
RDS_HOST = "devops-mysql-db.ctg8ey0oia95.ap-south-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASSWORD = "Admin12345"
RDS_DB = "devopsdb"
TABLE_NAME = "people"

# Glue Configuration
GLUE_DB = "devops_glue_db"
GLUE_TABLE = "people_table"


def read_from_s3():
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj["Body"])
    print("CSV Loaded from S3")
    return df


def push_to_rds(df):
    try:
        engine = create_engine(
            f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}/"
        )

        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS devopsdb"))

        engine_db = create_engine(
            f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}/{RDS_DB}"
        )

        df.to_sql(TABLE_NAME, con=engine_db, if_exists="replace", index=False)

        print("Data inserted into RDS successfully")
        return True

    except Exception as e:
        print("RDS failed:", e)
        return False


def create_glue_table():
    glue = boto3.client("glue")

    try:
        glue.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                "Name": GLUE_TABLE,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "id", "Type": "int"},
                        {"Name": "name", "Type": "string"},
                        {"Name": "city", "Type": "string"},
                    ],
                    "Location": f"s3://{S3_BUCKET}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )

        print("Glue table created as fallback")

    except Exception as e:
        print("Glue error:", e)


def main():

    df = read_from_s3()

    success = push_to_rds(df)

    if not success:
        create_glue_table()


if __name__ == "__main__":
    main()