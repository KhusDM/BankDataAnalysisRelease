import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""    
    IF (OBJECT_ID('Starting_Table') IS NULL)
    BEGIN
        SELECT
            cli_id,
            trans_date,
            trans_amount_rub,
            document_channel,
            document_channel_group,
            our_device,
            merchant_id,
            code,
            [group]
        INTO Starting_Table
        FROM CSV_Export
    END
    """)
    cursor.execute(sql_query)
    cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
