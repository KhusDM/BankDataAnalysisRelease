import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    sql_query = ("""
    IF(OBJECT_ID('CountOfTransForMonthOnClients') IS NULL)
        SELECT 0;
    ELSE 
        SELECT 1;
    """)
    result = cursor.execute(sql_query)
    existence = list(result.fetchone())[0]
    if (existence != 1):
        sql_query = ("""
        CREATE PROCEDURE CountOfTransForMonthOnClients @name NVARCHAR(100), @year INT, @month INT, @group NVARCHAR(100)
        AS
        BEGIN
            DECLARE @SQL NVARCHAR(1000), @columnName NVARCHAR(250), @yearStr NVARCHAR(15), @monthStr NVARCHAR(10), @where NVARCHAR(250);
            SET @yearStr=CAST(@year AS NVARCHAR(15));
            SET @monthStr=CAST(@month AS NVARCHAR(10));
            IF (@group='')
            BEGIN
                SET @columnName=CONCAT(@name, '_', @yearStr, '_', @monthStr);
                SET @where=CONCAT('WHERE YEAR(trans_date)=', @yearStr, ' AND ', 'MONTH(trans_date)=', @monthStr);
            END
            ELSE
            BEGIN
                SET @columnName=CONCAT(@name, '_', @group, '_', @yearStr, '_', @monthStr);
                SET @where=CONCAT('WHERE YEAR(trans_date)=', @yearStr, ' AND ', 'MONTH(trans_date)=', @monthStr, ' AND ', '[group]=', CHAR(39), @group, CHAR(39))
            END
            SET @SQL=
                    CONCAT(
                        'SELECT cli_id, COUNT(trans_amount_rub) AS ',
                        @columnName,
                        ' INTO ',
                        @columnName,
                        ' FROM Transaction_Table_Of_Correct_Clients ',
                        @where,
                        ' GROUP BY cli_id'
                    );
            EXECUTE(@SQL);
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
