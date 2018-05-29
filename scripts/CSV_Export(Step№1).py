import pandas
import pypyodbc as db
import os
from connection_setting import connect_setting

relative_path = os.path.dirname(__file__)
FILENAME = str(relative_path).replace(relative_path[relative_path.rfind("/") + 1:], connect_setting.FILENAME)
connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""
    IF (OBJECT_ID('CSV_Export') IS NULL)
        SELECT 0;
    ELSE 
        SELECT 1;
    """)
    result = cursor.execute(sql_query)
    existence = list(result.fetchone())[0]
    if (existence != 1):
        df = pandas.read_csv(FILENAME)
        df.drop(df.columns[[0]], axis=1, inplace=True)
        rows = list(tuple(row) for row in df.head(len(df)).values)

        sql_query = ("""           
        CREATE TABLE CSV_Export (
            event_id BIGINT,
            cli_id BIGINT,
            trans_date NVARCHAR(50),
            trans_amount_rub DECIMAL(38,2),
            document_channel INT,
            document_channel_group INT,
            our_device BIT,
            merchant_id BIGINT,
            code INT,
            [group] NVARCHAR(50)
        );
        """)
        cursor.execute(sql_query)
        cursor.commit()

        sql_query = ("""
        INSERT INTO CSV_Export(event_id, cli_id, trans_date, trans_amount_rub, document_channel, 
        document_channel_group, our_device, merchant_id, code, [group])
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """)
        cursor.executemany(sql_query, rows)
        cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
