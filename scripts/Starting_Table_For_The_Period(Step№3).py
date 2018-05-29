import pypyodbc as db
from datetime import timedelta, datetime
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    date_begin = connect_setting.DATE_BEGIN
    date_end = connect_setting.DATE_END
    delta = timedelta(days=1)
    date_end = str(datetime.strptime(date_end, '%Y-%m-%d').date() + delta)
    sql_query = ("""    
    IF(OBJECT_ID('Starting_Table_For_The_Period') IS NULL)
    BEGIN
        SELECT *
        INTO Starting_Table_For_The_Period
        FROM Starting_Table
        WHERE trans_date BETWEEN '{0}' AND '{1}'
    END
    """.format(date_begin, date_end))
    cursor.execute(sql_query)
    cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
