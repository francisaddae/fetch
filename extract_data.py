import pandas as pd, sqlalchemy, os
import json, datetime as dt

# with open('datasets/brands.json') as file:
#     response = json.load(file)
#     print(response)
# file.close()

def load_json_data(file_path, column_names, table_name):
    """This function will take in any json file and structure it into an exisiting tabular format for ingestion.
      Respective columns are to be provided and the table name is needed for any injestion

    Args:
        file_path (str): The path of the file
        column_names (list): List of all columns names needed for the table schema
        table_names (str): Name of the table within the datalake
    """

    try:
        with open(file_path) as file:
            data = json.load(file)
        file.close()
    except FileNotFoundError:
        raise(f"Error: File not found at path: {file_path}")
    except json.JSONDecodeError:
        raise (f"Error: Invalid JSON format in file: {file_path}")

    #Empty Dictionary containg all keys for the table
    schema_data = {i:[] for i in column_names}

    #Looking through data
    i = 1
    for record in data:
        print(f'Record: {i}/{len(data)}')
        for key in schema_data.keys():
            if key in record.keys() and isinstance(record[key], dict):
                # print(record[key]['$oid'])
                if '$oid' in record[key].keys():
                    schema_data[key].append(record[key]['$oid'])
                elif '$date' in record[key].keys():
                    time_stamp = record[key]['$date']/ 1e3
                    schema_data[key].append(dt.datetime.fromtimestamp(time_stamp))
                else:
                    schema_data[key].append(record[key])
            elif key in record.keys() and (isinstance(record[key], str) or isinstance(record[key], bool)):
                schema_data[key].append(record[key])
            else:
                schema_data[key].append(pd.NaT)
        i+=1

    df = pd.DataFrame.from_dict(schema_data)
    print(df.sample(10))

    print(df.dtypes)
    df = df.convert_dtypes()
    print(df.dtypes)
    #Setting up connection strings
    engine = sqlalchemy.create_engine(
        f'postgresql://{os.environ.get("POSTGRES_USERNAME")}:{os.environ.get("POSTGRES_PASSWORD")}@{os.environ.get("POSTGRES_HOSTNAME")}:{os.environ.get("POSTGRES_PORTNUM")}/{os.environ.get("POSTGRES_DATABASE")}')
    conn = engine.connect()
    df.to_sql(table_name, conn, if_exists="replace", dtype={"cpg": sqlalchemy.types.JSON})

load_json_data('datasets/brands.json',
               ['_id', 'barcode', 'brandCode', 'category', 'categoryCode', 'cpg', 'topBrand', 'name'], 'fetch_brands')


load_json_data('datasets/users.json', ['_id', 'state', 'createdDate', 'lastLogin', 'role', 'active'], 'fetch_users')

load_json_data('datasets/receipts.json',
               ['_id', 'bonusPointsEarned', 'bonusPointsEarnedReason', 'createDate', 'dateScanned', 'finishedDate', 'modifyDate'
                'pointsAwardedDate', 'pointsEarned', 'purchaseDate', 'purchasedItemCount', 'rewardsReceiptItemList', 'rewardsReceiptStatus',
                'totalSpent', 'userId'], 'fetch_receipts')

