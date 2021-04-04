
# ==============================================|*|| Required Libraries ||*|============================================
from glob import glob
import os
from datetime import datetime
import pandas as pd
import json
# =======================================================| *** |========================================================

# ============================================|*|| User Defined Packages ||*|===========================================
from Data_Logs.Logs import Logs
# =======================================================| *** |========================================================

# ============================================|| Step 03 - Transforming ||==============================================
class Transform:

    # ==> 'Unnamed : 0' column to 'Wafer' column
    # ==> Store missing values into a csv file
    # ==> Imputing missing values with 'mean()'
    # ==> Converting 'int64' dtype to 'float64' dtype

    def __init__(self, LogsPath, LogsFileName):
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()


    def transformData(self, LocalPaths, SchemaFileName, FileNames):
        self.LogsList.append(["Transform", f"Transforming files of GoodFiles", datetime.now(), "", "Started", ""])
        fileNameList = list()
        Schema_File = json.load(open(LocalPaths["SchemaFile"] + SchemaFileName))
        self.LogsList.append(["Transform", f"Loading Schema File", datetime.now(), f"{SchemaFileName}", "Successfully loaded", ""])
        schema_ColumnNames = list(Schema_File["ColName"].keys())
        Missing_Df = pd.DataFrame(columns=schema_ColumnNames)

        # ----------------------------------------------|| Load Raw File ||---------------------------------------------
        Local_CsvFiles = glob(os.path.join(LocalPaths["GoodFiles"], '*.csv'))
        self.LogsList.append(["Transform", f"Loading Good files", datetime.now(), "", f"{len(Local_CsvFiles)} are loaded successfully", ""])

        for file in Local_CsvFiles:
            file = file.replace("\\", '/')
            fileName = file.split("/")[-1]
            fileNameList.append(fileName)
            df = pd.read_csv(file)

            # ----------------------------->> 'Unnamed : 0' column to 'Wafer' column <<---------------------------------
            df.rename(columns={"Unnamed: 0": schema_ColumnNames[0]}, inplace=True)
            self.LogsList.append(["Transform", f"Rename of column", datetime.now(), f"{fileName}",f"if any 'Unnamed: 0' -> '{schema_ColumnNames[0]}'", ""])

            # ------------------------------>> Store missing values into a csv file <<----------------------------------
            Missing_Df = pd.concat([Missing_Df, pd.DataFrame(df.isnull().sum()).T])
            self.LogsList.append(["Transform", f"Missing values", datetime.now(), f"{fileName}", f"Missing values of columns for {fileName} are stored", ""])

            converted_cols = list()
            imputed_cols = list()

            for col in schema_ColumnNames:

                # -------------------------->> Imputing missing values with 'mean()' <<---------------------------------
                if df[col].isnull().sum() > 0:
                    df[col].fillna(df[col].mean(), inplace=True)
                    imputed_cols.append(col)
                    self.LogsList.append(["Transform", f"Imputing missing values", datetime.now(), f"{fileName}", f"Missing values in {fileName}->{col} : {df[col].isnull().sum()}", "Imputed with Mean"])

                # ----------------------->> Converting 'int64' DType to 'float64' DType <<------------------------------
                if df[col].dtype == "int64":
                    df[col] = df[col].astype(float)
                    converted_cols.append(col)
                    self.LogsList.append(["Transform", f"Data type conversion", datetime.now(), f"{fileName}", f"Data type of column : {fileName}->{col}", "Convert to 'float64'"])

            # ----------------------->> Store transformed file into  "TransformedFiles" <<------------------------------
            df.to_csv(LocalPaths["TransformedFiles"] + fileName, index=False)
            self.LogsList.append(["Transform", f"Pushing File", datetime.now(), f"{fileName}", f"Transformed File is stored in 'TransformedFiles' folder", ""])

        # ------------------------------>> Storing Missing values data frame into CSV <<--------------------------------
        Missing_Df.index = fileNameList
        Missing_Df.to_csv(LocalPaths["LogFiles"] + FileNames["MissingValues_FileName"])
        self.LogsList.append(["Transform", f"Pushing File", datetime.now(), "", f"Missing values of columns for each file is stored in 'MissingValues_FileName' folder", ""])
        # ---------------------------------------------------| *** |----------------------------------------------------
        self.LogsList.append(["Transform", f"Transforming files of GoodFiles", datetime.now(), "", "Completed", ""])
        self.Logs.storeLogs(self.LogsList)

# ======================================================| *** |=========================================================
