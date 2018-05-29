import pypyodbc as db
import numpy as np
import math
from datetime import datetime
from dateutil import relativedelta
from sklearn.cluster import KMeans
from connection_setting import connect_setting

connection_string = connect_setting.CONNECTION_STRING

try:
    connection = db.connect(connection_string)
    cursor = connection.cursor()

    date_begin = datetime.strptime(connect_setting.DATE_BEGIN, '%Y-%m-%d').date()
    date_end = datetime.strptime(connect_setting.DATE_END, '%Y-%m-%d').date()
    difference = relativedelta.relativedelta(date_end, date_begin)
    months = 0
    if (difference.days != 0):
        months = abs(difference.years) * 12 + abs(difference.months) + 1
    else:
        months = abs(difference.years) * 12 + abs(difference.months)
    delta = relativedelta.relativedelta(months=1)
    groups = connect_setting.GROUPS
    groups.insert(0, "")
    columns_metrics_trans = ""
    left_joins_metrics = ""
    for group in groups:
        into_table = ""

        postfix_for_group = ""
        if (group != ""):
            postfix_for_group = "_{0}".format(group)

        into_table = "Count_Of_Trans{0}_On_Merchants_For_Months_Table".format(postfix_for_group)
        sql_query = ("""
        IF(OBJECT_ID('{0}') IS NULL)
            SELECT 0;
        ELSE 
            SELECT 1;
        """.format(into_table))
        result = cursor.execute(sql_query)
        existence = list(result.fetchone())[0]
        if (existence != 1):
            sql_query = ""
            columns_trans = ""
            left_joins = ""
            date_current = date_begin
            while ((date_current.year < date_end.year) or (
                    date_current.year == date_end.year and date_current.month <= date_end.month)):
                year_current = date_current.year
                month_current = date_current.month
                sql_query += "EXECUTE CountOfTransForMonthOnMerchants @name='count_of_trans_for', @year={0}, @month={1}, @group='{2}';\n".format(
                    year_current, month_current, group)
                columns_trans += "\t\tcount_of_trans_for{0}_{1}_{2},\n".format(postfix_for_group, year_current,
                                                                               month_current)
                left_joins += "LEFT JOIN count_of_trans_for{0}_{1}_{2} AS for_{1}_{2} ON aggregation_table.merchant_id=for_{1}_{2}.merchant_id\n".format(
                    postfix_for_group, year_current, month_current)
                date_current += delta

            columns_trans = columns_trans[:len(columns_trans) - 2] + "\n"
            sql_query += "\n"
            sql_query += "SELECT\n\t\taggregation_table.merchant_id,\n\t\taggregation_table.count_trans{0},\n\t\taggregation_table.sum_trans{0},\n".format(
                postfix_for_group)
            sql_query += columns_trans
            sql_query += "INTO {0}\n".format(into_table)

            subquery = ""
            subquery += "SELECT\n\t\tmerchant_id,\n\t\tCOUNT(trans_amount_rub) AS count_trans{0},\n\t\tSUM(trans_amount_rub) AS sum_trans{0}\n".format(
                postfix_for_group)
            subquery += "\t\tFROM Transaction_Table_Of_Correct_Clients_On_Merchants\n"
            if (group != ""):
                subquery += "\t\tWHERE [group]='{0}'\n".format(group)
            subquery += "\t\tGROUP BY merchant_id\n\t"

            sql_query += "FROM ({0}) AS aggregation_table\n".format(subquery)
            sql_query += left_joins
            sql_query += "\n"
            sql_query += "DROP TABLE\n"
            sql_query += columns_trans
            cursor.execute(sql_query)
            cursor.commit()

        metrics_table_name = "Metric_Table{0}_On_Merchants".format(postfix_for_group)
        sql_query = ("""
        IF(OBJECT_ID('{0}_Extended') IS NULL)
            SELECT 0;
        ELSE 
            SELECT 1;
        """.format(metrics_table_name))
        result = cursor.execute(sql_query)
        existence = list(result.fetchone())[0]
        if (existence != 1):
            sql_query = "SELECT * FROM {0}".format(into_table)
            rows = cursor.execute(sql_query)
            rows = rows.fetchall()
            signs_of_clustering = []
            metrics_table = []
            for i in range(0, len(rows)):
                row = list(rows[i])
                merchant_id = M_count = M_sum = D_count = S_count = V_count = 0
                count_trans = 0
                for j in range(3, len(row)):
                    if (row[j] != None):
                        count_trans += row[j]
                    else:
                        count_trans += 0

                metrics = []
                merchant_id = row[0]
                if (count_trans != 0):
                    M_count = count_trans / months

                    sum = 0
                    for j in range(3, len(row)):
                        if (row[j] != None):
                            sum += pow(row[j] - M_count, 2)
                        else:
                            sum += pow(0 - M_count, 2)

                    M_count = round(M_count, 2)
                    M_sum = round(float(row[2]) / months, 2)
                    D_count = round(sum / months, 2)
                    S_count = round(math.sqrt(D_count), 2)
                    V_count = round(S_count / M_count * 100, 2)
                else:
                    M_count = 0
                    M_sum = 0
                    D_count = 0
                    S_count = 0
                    V_count = 0

                metrics.append(merchant_id)
                metrics.append(M_count)
                metrics.append(M_sum)
                metrics.append(D_count)
                metrics.append(S_count)
                metrics.append(V_count)
                metrics_table.append(metrics)

                signs_of_clustering.append([M_count, M_sum])

            kmeans = KMeans(n_clusters=4, n_init=25, max_iter=2000)
            kmeans.fit(np.array(signs_of_clustering))
            labels = list(kmeans.labels_)

            for i in range(len(labels)):
                metrics_table[i].append(int(labels[i]))
                metrics_table[i] = tuple(metrics_table[i])

            sql_query = ("""    
            CREATE TABLE {0} (
                merchant_id BIGINT,
                average_count_of_trans{1} DECIMAL(38,2),
                average_sum_of_trans{1} DECIMAL(38,2),
                variance_of_trans{1} DECIMAL(38,2),
                mean_square_deviation_of_trans{1} DECIMAL(38,2),
                coefficient_of_variation_of_trans{1} DECIMAL(38,2),
                cluster_number{1} INT
            )""".format(metrics_table_name, postfix_for_group))
            cursor.execute(sql_query)
            cursor.commit()

            sql_query = ("""
            INSERT INTO {0}( merchant_id, average_count_of_trans{1}, average_sum_of_trans{1}, variance_of_trans{1}, 
            mean_square_deviation_of_trans{1}, coefficient_of_variation_of_trans{1}, cluster_number{1})
            VALUES(?,?,?,?,?,?,?)
            """.format(metrics_table_name, postfix_for_group))
            cursor.executemany(sql_query, metrics_table)
            cursor.commit()

            sql_query = ("""
            SELECT 
                A.merchant_id,
                A.count_trans{2},
                B.average_count_of_trans{2},
                A.sum_trans{2},
                B.average_sum_of_trans{2},
                B.variance_of_trans{2},
                B.mean_square_deviation_of_trans{2},
                B.coefficient_of_variation_of_trans{2},
                B.cluster_number{2}
            INTO {1}_Extended   
            FROM {0} AS A, {1} AS B
            WHERE A.merchant_id=B.merchant_id
            """.format(into_table, metrics_table_name, postfix_for_group))
            cursor.execute(sql_query)
            cursor.commit()

            sql_query = ("""DROP TABLE {0}""".format(metrics_table_name))
            cursor.execute(sql_query)
            cursor.commit()
except Exception as exc:
    print(exc)
    raise Exception(exc)
finally:
    cursor.close()
    connection.close()
