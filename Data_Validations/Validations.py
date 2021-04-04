# ===========================|*|| Required Libraries ||*|=============================
from glob import glob
import os
import json
import shutil
import pandas as pd
import re
from datetime import datetime
# =======================================| *** |======================================

# ==========================|*|| User defined Packages ||*|===========================
from Data_Logs.Logs import Logs
# =======================================| *** |======================================


# ==============================|| Step 02 - Validation ||==============================
class Validations:
    def __init__(self, LogsPath, LogsFileName):
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()

    # ==============================|| FileName Validation ||==============================
    def validating_RawFiles(self, LocalPath, SchemaFileName, FileNames):
        self.LogsList.append(["Validations", f"File names and Describe validations with schema file", datetime.now(), "", "Started", ""])

        # ============>> Load Schema File
        Schema_File = json.load(open(LocalPath['SchemaFile'] + SchemaFileName))
        self.LogsList.append(["Validations", f"Loading Schema File", datetime.now(), f"{SchemaFileName}", "Successfully Loaded", ""])

        # ============>> Load Raw Files
        Local_CsvFiles = glob(os.path.join(LocalPath["RawFiles"], '*.csv'))
        Local_CsvFiles = [file.replace("\\", "/") for file in Local_CsvFiles]
        self.LogsList.append(["Validations", f"Loading Raw Files list", datetime.now(), "{}", "Successfully Loaded", f"{len(Local_CsvFiles)} RawFiles are available"])

        # ----------------->> Loop extracts one-by-one file from RawBatchFile
        for file in Local_CsvFiles:
            FileName = file.split("/")[-1]
            FileNameList = FileName.replace(".csv", "").split("_")
            SchemaFileList = Schema_File["SampleFileName"].replace(".csv", "").split("_")

            # ----------------->> 'Wafer' Name Validation
            if SchemaFileList[0].lower() == FileNameList[0].lower():
                self.LogsList.append(["Validations", f"Name Validation: Wafer", datetime.now(), f"{FileName}", "Validation Success", ""])

                # ----------------->> 'Date' Validation
                DateDigits = (len(re.compile('\d').findall(FileNameList[1])) == len(FileNameList[1]))
                DateLengh = (len(FileNameList[1]) == Schema_File["LengthOfDateStampInFile"])
                if DateDigits and DateLengh:
                    self.LogsList.append(["Validations", f"Name Validation: DateStamp", datetime.now(), f"{FileName}", "Validation Success", ""])

                    # ----------------->> 'Time' Validation
                    TimeDigits = (len(re.compile('\d').findall(FileNameList[2])) == len(FileNameList[2]))
                    TimeLength = (len(FileNameList[2]) == Schema_File["LengthOfTimeStampInFile"])
                    if TimeDigits and TimeLength:
                        self.LogsList.append(["Validations", f"Name Validation: TimeStamp", datetime.now(), f"{FileName}", "Validation Success", ""])

                        # ==============================|| Column Validation ||==============================
                        df = pd.read_csv(file)
                        # ----------------->> Number  of column
                        if len(df.columns) == Schema_File["NumberofColumns"]:
                            self.LogsList.append(["Validations", f"Column Validation: Column Numbers", datetime.now(), f"{FileName}", "Validation Success", ""])
                            schema_ColumnNames = list(Schema_File["ColName"].keys())
                            file_ColumnNames = list(df.columns)
                            schema_ColumnDTypes = list(Schema_File["ColName"].values())
                            file_ColumnDTypes = list(df.dtypes)
                            for i in range(1, Schema_File["NumberofColumns"]):

                                # ----------------->> Names of the columns
                                if schema_ColumnNames[i] == file_ColumnNames[i]:
                                    self.LogsList.append(["Validations", f"Column Validation: Column Names", datetime.now(), f"{FileName}", "Validation Success", ""])

                                    # ----------------->> DataTypes of the columns
                                    if (schema_ColumnDTypes[i] == str(file_ColumnDTypes[i])) or ("int64" == str(file_ColumnDTypes[i])):
                                        self.LogsList.append(["Validations", f"Column Validation: Column Data types", datetime.now(), f"{FileName}", "Validation Success", ""])

                                        # ----------------->> Missing values of columns
                                        # Condition: If Missing values are less than or equal to 20%, then it is valid column.
                                        n_missingValues = df[file_ColumnNames[i]].isnull().sum()
                                        n_totalValues = len(df)
                                        if n_missingValues != n_totalValues:
                                            if i == Schema_File["NumberofColumns"] - 1:
                                                _ = shutil.copy(file, LocalPath["GoodFiles"] + FileName)
                                                self.LogsList.append(["Validations", f"Column Validation: Missing values in Column", datetime.now(), f"{FileName}", "Validation Success", f"{FileName} is moved to GoodFiles folder"])
                                        else:
                                            _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                                            self.LogsList.append(["Validations", f"Column Validation: Missing values in Column", datetime.now(), f"{FileName}", f"Failed: {file_ColumnNames[i]} has {n_missingValues} missing values", f"{FileName} is moved to BadFiles folder"])
                                            break
                                    else:
                                        _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                                        self.LogsList.append(["Validations", f"Column Validation: Data type of Column", datetime.now(), f"{FileName}", f"Failed: {file_ColumnNames[i]} has {str(file_ColumnDTypes[i])}", f"{FileName} is moved to BadFiles folder"])
                                        break
                                else:
                                    _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                                    self.LogsList.append(["Validations", f"Column Validation: Name of Column", datetime.now(), f"{FileName}", f"Failed: {file_ColumnNames[i]} column is failed", f"{FileName} is moved to BadFiles folder"])
                                    break
                        else:
                            _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                            self.LogsList.append(["Validations", f"Column Validation: Number of Columns", datetime.now(), f"{FileName}", f"Failed: {FileName} has {len(df.columns)} columns",f"{FileName} is moved to BadFiles folder"])
                    else:
                        _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                        self.LogsList.append(["Validations", f"Name Validation: Timestamp", datetime.now(), f"{FileName}", f"Failed: TimeDigits and TimeLength -> {FileNameList[2]}", ""])
                else:
                    _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                    self.LogsList.append(["Validations", f"Name Validation: Datestamp", datetime.now(), f"{FileName}",f"Failed: DateDigits & DateLength -> {FileNameList[1]}", ""])
            else:
                _ = shutil.copy(file, LocalPath["BadFiles"] + FileName)
                self.LogsList.append(["Validations", f"Name Validation: Wafer", datetime.now(), f"{FileName}", f"Failed: {FileNameList[0]} != 'Wafer", ""])
        self.LogsList.append(["Validations", f"File names and Describe validations", datetime.now(), "", "Completed", ""])
        self.Logs.storeLogs(self.LogsList)
# =============================================================| *** |=================================================
