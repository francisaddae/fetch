import pandas as pd, sqlalchemy, os, numpy as np
import json, datetime as dt
import clickhouse_connect

client = clickhouse_connect.get_client(
    host = os.environ.get('CLICKHOUSE_HOST'),
    port = os.environ.get('CLICKHOUSE_NATIVE_PORT'),
    username = os.environ.get('CLICKHOUSE_USER'),
    password = os.environ.get('CLICKHOUSE_CRED'),
    secure=True
)

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
    """
    This loop will take each single record, validate the keys against the specified coloumns
    and append the values if applicable else null.
    """
    for record in data:
        # print(f'Record: {i}/{len(data)}')
        for key in schema_data.keys():
            #checks for embeeded dictionaries and pull relevant key values
            if key in record.keys() and isinstance(record[key], dict):
                if '$oid' in record[key].keys():
                    schema_data[key].append(record[key]['$oid'])
                #formatting datetime from integer to dates
                elif '$date' in record[key].keys():
                    time_stamp = record[key]['$date']/ 1e3
                    schema_data[key].append(dt.datetime.fromtimestamp(time_stamp))
                else:
                    schema_data[key].append(str(record[key]))
            elif key in record.keys() and isinstance(record[key], list):
                schema_data[key].append(str(record[key][0]))
            elif key in record.keys() and (isinstance(record[key], str) or isinstance(record[key], bool)):
                schema_data[key].append(str(record[key]))
            elif key in record.keys() and (isinstance(record[key], int) or isinstance(record[key], float)):
                schema_data[key].append(record[key])
            else:
                schema_data[key].append(np.nan)
        i+=1


    df = pd.DataFrame.from_dict(schema_data)
    df = df.convert_dtypes()
    print(df.head(10))
    print(df.dtypes)

    # Setting up connection strings
    engine = sqlalchemy.create_engine(
        f'postgresql://{os.environ.get("POSTGRES_USERNAME")}:{os.environ.get("POSTGRES_PASSWORD")}@{os.environ.get("POSTGRES_HOSTNAME")}:{os.environ.get("POSTGRES_PORTNUM")}/{os.environ.get("POSTGRES_DATABASE")}')

    df.to_sql(table_name, engine, if_exists="replace", index=False)


    try:
        client.insert(table_name, df)
    except Exception as e:
        print(f"Error inserting data: {e}")

    df.to_csv(f'datasets/{table_name}.csv', index=False)

    return [df, table_name]

load_json_data('datasets/brands.json',
               ['_id', 'barcode', 'brandCode', 'category', 'categoryCode', 'cpg', 'topBrand', 'name'], 'fetch_brands')


load_json_data('datasets/users.json', ['_id', 'state', 'createdDate', 'lastLogin', 'role', 'active'], 'fetch_users')

load_json_data('datasets/receipts.json',
               ['_id', 'bonusPointsEarned', 'bonusPointsEarnedReason', 'createDate', 'dateScanned', 'finishedDate', 'modifyDate',
                'pointsAwardedDate', 'pointsEarned', 'purchaseDate', 'purchasedItemCount', 'rewardsReceiptItemList', 'rewardsReceiptStatus',
                'totalSpent', 'userId'], 'fetch_receipts')



