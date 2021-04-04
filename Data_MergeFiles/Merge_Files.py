
# ==============================================|*|| Required Libraries ||*|============================================
from glob import glob
import os
import json
from datetime import datetime
import pandas as pd
# =======================================================| *** |========================================================

# ============================================|*|| User Defined Packages ||*|===========================================
from Data_Logs.Logs import Logs
# =======================================================| *** |========================================================

# ===============================================|| Step 04 - Merging ||================================================
# GoodRawBatch Files path - 'Temp_Files/Training/DataTransform_GoodRawBatchFiles'
# Single csv file path - 'SingleFile/SingleFile.csv'

class Merge:
    def __init__(self, LogsPath, LogsFileName):
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()

    def mergeFiles(self, LocalPaths, SchemaFileName, FileNames):
        # -----------------------------------------|| Load Data Transform Files ||--------------------------------------
        self.LogsList.append(["Merge", f"Merging all Transformed Files into Single File", datetime.now(), "", "Started", ""])
        Schema_File = json.load(open(LocalPaths["SchemaFile"] + SchemaFileName))
        self.LogsList.append(["Merge", f"Loading Schema File", datetime.now(), f"{SchemaFileName}", "Successfully loaded", ""])
        schema_ColumnNames = list(Schema_File['ColName'].keys())
        SingleDf = pd.DataFrame(columns = schema_ColumnNames)

        Local_CsvFiles = glob(os.path.join(LocalPaths["TransformedFiles"], '*.csv'))
        self.LogsList.append(["Merge", f"Loading Transformed Files", datetime.now(), "", f"{len(Local_CsvFiles)} are loaded successfully", ""])
        self.LogsList.append(["Merge", f"Merging all files", datetime.now(), "", "Started", ""])
        for file in Local_CsvFiles:
            file = file.replace("\\", "/")
            df = pd.read_csv(file)
            # --------------------------->> Combining TransformedFiles into 'Single' File <<----------------------------
            SingleDf = pd.concat([SingleDf, df])
        self.LogsList.append(["Merge", f"Merging all files", datetime.now(), "", "Completed", ""])
        SingleDf.to_csv(LocalPaths["SingleFile"] + FileNames["Single_FileName"], index=False)
        self.LogsList.append(["Merge", f"Storing file", datetime.now(), f"{FileNames['Single_FileName']}", "Stored successfully into SingleFile folder", ""])
        self.LogsList.append(["Merge", f"Merging all Transformed Files into Single File", datetime.now(), "", "Completed", ""])
        self.Logs.storeLogs(self.LogsList)
# ======================================================| *** |=========================================================
