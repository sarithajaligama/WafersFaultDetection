
# ==============================================|*|| Required Libraries ||*|============================================
import pandas as pd
import os
# =======================================================| *** |========================================================

# ============================================|*|| User Defined Packages ||*|===========================================
from Global_Variables.Global_Variables import GlobalVariablesPaths
# =======================================================| *** |========================================================

# =================================================|| Storing Logs ||===================================================
class Logs:

    def __init__(self, LogsPath, LogsFileName):
        self.LogsPath = LogsPath
        self.LogsFileName = LogsFileName

        self.GVP = GlobalVariablesPaths()
        self.log_columns_name = list(self.GVP.FileNames["Logs_Tracks"].keys())

    def storeLogs(self, LogsList):

        if self.LogsFileName in os.listdir(self.LogsPath):
            Logs_Df = pd.read_csv(self.LogsPath + self.LogsFileName)
        else:
            Logs_Df = pd.DataFrame(columns=self.log_columns_name)

        # Loop Extracts one-by-one log value from Logs_List
        for log in LogsList:
            Dict = dict()
            for i in range(len(self.log_columns_name)):
                Dict[self.log_columns_name[i]] = log[i]
            Logs_Df = Logs_Df.append(Dict, ignore_index=True)

        Logs_Df.to_csv(self.LogsPath + self.LogsFileName, index=False)

        # Merging all stages log histories together
        # -------------------------------------------
        if "Logs_of_AllTransactions.csv" in os.listdir(self.LogsPath):
            Logs_History = pd.read_csv(self.LogsPath + "Logs_of_AllTransactions.csv")
        else:
            Logs_History = pd.DataFrame(columns=self.log_columns_name)
        Logs_History = pd.concat([Logs_History, Logs_Df])
        Logs_History.to_csv(self.LogsPath + "Logs_of_AllTransactions.csv", index=False)
        return list()
# ----------------------------------------------------------------------------------------------------------------------

