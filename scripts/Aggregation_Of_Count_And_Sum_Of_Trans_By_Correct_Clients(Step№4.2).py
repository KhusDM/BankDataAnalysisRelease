import pypyodbc as db
from datetime import datetime
from dateutil import relativedelta
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""
    IF(OBJECT_ID('Aggregation_Of_Count_And_Sum_Of_Trans_By_Correct_Clients') IS NULL)
        SELECT 0;
    ELSE 
        SELECT 1;
    """)
    result = cursor.execute(sql_query)
    existence = list(result.fetchone())[0]
    if (existence != 1):
        date_begin = connect_setting.DATE_BEGIN
        date_end = connect_setting.DATE_END
        date1 = datetime.strptime(date_begin, "%Y-%m-%d")
        date2 = datetime.strptime(date_end, "%Y-%m-%d")
        difference = relativedelta.relativedelta(date2, date1)
        months = 0
        if (difference.days != 0):
            months = abs(difference.years) * 12 + abs(difference.months) + 1
        else:
            months = abs(difference.years) * 12 + abs(difference.months)
        sql_query = ("""
        SELECT cli_id, count_trans, sum_trans
        INTO Aggregation_Of_Count_And_Sum_Of_Trans_By_Correct_Clients
        FROM Aggregation_Of_Count_And_Sum_Of_Trans_By_Clients
        WHERE count_trans>=4*{0}
        """.format(months))
        cursor.execute(sql_query)
        cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
