import pypyodbc as db
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    groups = connect_setting.GROUPS
    groups.insert(0, "")
    for group in groups:
        into_table = ""

        postfix_for_group = ""
        where_sql = ""
        if (group != ""):
            postfix_for_group = "_{0}".format(group)
            where_sql = "AND [group]='{0}'".format(group)

        into_table = "Comparison_Table{0}".format(postfix_for_group)

        sql_query = ("""
        IF(OBJECT_ID('{0}') IS NULL)
            SELECT 0;
        ELSE 
            SELECT 1;
        """.format(into_table))
        result = cursor.execute(sql_query)
        existence = list(result.fetchone())[0]
        if (existence != 1):
            sql_query = ("""
            SELECT DISTINCT
                A.merchant_id,
                A.cluster_number{1} AS cluster_number_merchant{1},
                A.coefficient_of_variation_of_trans{1} AS coefficient_of_variation_of_trans_merchant{1},
                B.cli_id,
                B.cluster_number{1} AS cluster_number_client{1},
                B.coefficient_of_variation_of_trans{1} AS coefficient_of_variation_of_trans_client{1},
                COUNT(C.trans_amount_rub) AS count_trans_client{1},
				SUM(C.trans_amount_rub) AS sum_trans_client{1}
            INTO {0}
            FROM 
                Metric_Table{1}_On_Merchants_Extended AS A, 
                Metric_Table{1}_Extended AS B,
                Transaction_Table_Of_Correct_Clients_On_Merchants AS C
            WHERE A.merchant_id=C.merchant_id AND B.cli_id=C.cli_id {2}
            GROUP BY
                A.merchant_id,
                A.cluster_number{1},
                A.coefficient_of_variation_of_trans{1},
                B.cli_id,
                B.cluster_number{1},   
                B.coefficient_of_variation_of_trans{1}
            """.format(into_table, postfix_for_group, where_sql))
            cursor.execute(sql_query)
            cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
