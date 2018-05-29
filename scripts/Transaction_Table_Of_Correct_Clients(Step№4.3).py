import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""    
    IF(OBJECT_ID('Transaction_Table_Of_Correct_Clients') IS NULL)
    BEGIN
        SELECT 
            A.cli_id,
            A.trans_date,
            A.trans_amount_rub,
            A.document_channel,
            A.document_channel_group,
            A.our_device,
            A.merchant_id,
            A.code,
            A.[group]
        INTO Transaction_Table_Of_Correct_Clients
        FROM Starting_Table_For_The_Period AS A 
        INNER JOIN Aggregation_Of_Count_And_Sum_Of_Trans_By_Correct_Clients AS B
        ON A.cli_id=B.cli_id
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
