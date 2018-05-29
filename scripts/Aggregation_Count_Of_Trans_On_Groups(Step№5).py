import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""
    IF(OBJECT_ID('Aggregation_Count_Of_Trans_On_Groups') IS NULL)
    BEGIN
        SELECT [group], COUNT(trans_amount_rub) as count_trans, SUM(trans_amount_rub) as sum_trans
        INTO Aggregation_Count_Of_Trans_On_Groups
        FROM Transaction_Table_Of_Correct_Clients
        GROUP BY [group]     
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
