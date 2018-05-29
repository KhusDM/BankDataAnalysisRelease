import subprocess
import os

RELATIVE_PATH = str(os.path.dirname(__file__))
INTERPRETER = RELATIVE_PATH.replace(RELATIVE_PATH[RELATIVE_PATH.rfind("/") + 1:], "venv\Scripts\python.exe")


def run_script(file_path, step_number, args=None):
    program = [INTERPRETER, file_path]
    if (args != None):
        for argument in args:
            program.append(argument)
    code = subprocess.check_call(program)
    if (code == 0):
        print("Successful {0}".format(step_number))
    else:
        raise Exception("Fail {0}".format(step_number))


try:
    file_path = RELATIVE_PATH + "/CSV_Export(Step№1).py"
    run_script(file_path, "Step№1")

    file_path = RELATIVE_PATH + "/Starting_Table(Step№2).py"
    run_script(file_path, "Step№2")

    file_path = RELATIVE_PATH + "/Starting_Table_For_The_Period(Step№3).py"
    run_script(file_path, "Step№3")

    file_path = RELATIVE_PATH + "/Aggregation_Of_Count_And_Sum_Of_Trans_By_Clients(Step№4.1).py"
    run_script(file_path, "Step№4.1")

    file_path = RELATIVE_PATH + "/Aggregation_Of_Count_And_Sum_Of_Trans_By_Correct_Clients(Step№4.2).py"
    run_script(file_path, "Step№4.2")

    file_path = RELATIVE_PATH + "/Transaction_Table_Of_Correct_Clients(Step№4.3).py"
    run_script(file_path, "Step№4.3")

    file_path = RELATIVE_PATH + "/Aggregation_Count_Of_Trans_On_Groups(Step№5).py"
    run_script(file_path, "Step№5")

    file_path = RELATIVE_PATH + "/Create_Procedure_CountOfTransForMonthOnClients(Step№6.1).py"
    run_script(file_path, "Step№6.1")

    file_path = RELATIVE_PATH + "/Create_Main_Metric_Table_For_Clients(Step№6.2).py"
    run_script(file_path, "Step№6.2")

    file_path = RELATIVE_PATH + "/Aggregation_Of_Count_And_Sum_Of_Trans_By_Clients_On_Merchants(Step№7.1).py"
    run_script(file_path, "Step№7.1")

    file_path = RELATIVE_PATH + "/Aggregation_Of_Count_And_Sum_Of_Trans_By_Correct_Clients_On_Merchants(Step№7.2).py"
    run_script(file_path, "Step№7.2")

    file_path = RELATIVE_PATH + "/Transaction_Table_Of_Correct_Clients_On_Merchants(Step№7.3).py"
    run_script(file_path, "Step№7.3")

    file_path = RELATIVE_PATH + "/Create_Procedure_CountOfTransForMonthOnMerchants(Step№8.1).py"
    run_script(file_path, "Step№8.1")

    file_path = RELATIVE_PATH + "/Create_Metric_Tables_On_Merchants_Extended(Step№8.2).py"
    run_script(file_path, "Step№8.2")

    file_path = RELATIVE_PATH + "/Create_Comparison_Tables(Step№9).py"
    run_script(file_path, "Step№9")
except Exception as exc:
    print(exc)
