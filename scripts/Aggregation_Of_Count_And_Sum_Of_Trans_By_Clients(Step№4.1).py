import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""    
    IF(OBJECT_ID('Aggregation_Of_Count_And_Sum_Of_Trans_By_Clients') IS NULL)
    BEGIN
        SELECT cli_id, COUNT(trans_amount_rub) as count_trans, SUM(trans_amount_rub) as sum_trans
        INTO Aggregation_Of_Count_And_Sum_Of_Trans_By_Clients
        FROM Starting_Table_For_The_Period
        GROUP BY cli_id
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
