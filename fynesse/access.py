from config import *
from pymysql import connect
from pymysql.connections import Connection
import pandas as pd


def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = connect(user=user,
                        passwd=password,
                        host=host,
                        port=port,
                        local_infile=1,
                        db=database
                      )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def drop_and_create_table_for_priced_paid_data(conn: Connection, table_name: str):
  cur = conn.cursor()
  query1 = f"DROP TABLE IF EXISTS `{table_name}`"
  query2 = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
      `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
      `price` int(10) unsigned NOT NULL,
      `date_of_transfer` date NOT NULL,
      `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
      `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
      `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
      `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
      `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
      `street` tinytext COLLATE utf8_bin NOT NULL,
      `locality` tinytext COLLATE utf8_bin NOT NULL,
      `town_city` tinytext COLLATE utf8_bin NOT NULL,
      `district` tinytext COLLATE utf8_bin NOT NULL,
      `county` tinytext COLLATE utf8_bin NOT NULL,
      `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
      `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
      `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
      PRIMARY KEY (`db_id`),
      INDEX (`postcode`) USING HASH,
      INDEX (`date_of_transfer`) USING HASH
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
  """
  cur.execute(query1)
  cur.execute(query2)


def upload_csv_to_aws(conn: Connection, table_name: str, file_name: str):
  cur = conn.cursor()
  query = f"""
    LOAD DATA LOCAL INFILE '{file_name}' INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' 
    LINES STARTING BY '' TERMINATED BY '\n'
  """
  cur.execute(query)
  rows_affected=cur.rowcount
  return rows_affected

def get_data(year: int, part: int):
  url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}-part{part}.csv"
  df = pd.read_csv(url, header=None)
  return df

def upload_datasets(
    conn: Connection, 
    table_name: str, 
    sample_table_name: str, 
    year_from: int, 
    year_to_excl: int, 
    sample_portion = 0.01
  ):
    num_fetched_rows = []
    num_uploaded_rows = []
    for year in range(year_from, year_to_excl):
      for part in [1, 2]:
        print(f"fetching data from year: {year}, part: {part}")
        df = get_data(year, part)
        num_fetched_rows.append(len(df))
        filename = f"price_paid_data_year_{year}_part_{part}.csv"
        df.to_csv(filename, header=False, index=False)
        df.sample(frac=sample_portion).to_csv("sample_" + filename, header=False, index=False)
        print(f"uploading the data")
        num_uploaded_rows.append(
            upload_csv_to_aws(conn, table_name, filename)
        )
        upload_csv_to_aws(conn, sample_table_name, "sample_" + filename)
    return num_fetched_rows, num_uploaded_rows


def select_top(conn: Connection, table_name: str,  n: int = 5):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table_name} LIMIT {n}')

    rows = cur.fetchall()
    column_names = [i[0] for i in cur.description]
    return rows, column_names


def custom_simple_select(conn, table, what_to_select="COUNT(*)"):
  cur = conn.cursor()
  cur.execute(f"""
    SELECT {what_to_select} FROM {table}
  """)
  result = cur.fetchall()
  return result

def custom_query(conn: Connection, query: str):
  cur = conn.cursor()
  cur.execute(query)
  result = cur.fetchall()
  return result
