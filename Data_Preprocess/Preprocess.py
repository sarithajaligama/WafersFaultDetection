# ==============================================|*|| Required Libraries ||*|============================================
from glob import glob
import os
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import MinMaxScaler


# =======================================================| *** |========================================================

# ============================================|*|| User Defined Packages ||*|===========================================
from Data_Logs.Logs import Logs
from Global_Variables.Global_Variables import GlobalVariablesPaths
# =======================================================| *** |========================================================

# =============================================|| Step 06 - Preprocess ||===============================================

class Preprocess:
    def __init__(self, LogsPath, LogsFileName):
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()
        GVP = GlobalVariablesPaths()
        self.LocalTrainingPaths = GVP.LocalTrainingPaths

    def preprocesTraningsData(self, LocalPaths, FileNames):
        self.LogsList.append(["Preprocess", "Preprocessing Training data", datetime.now(), "", "Started", ""])
        data = pd.read_csv(LocalPaths['SingleFile'] + FileNames['Single_FileName'])
        self.LogsList.append(["Preprocess", "Loading File", datetime.now(), f"{FileNames['Single_FileName']}", "Data frame created with the file", ""])

        # ------------------------------>> FeatureSelection : Removing unnecessary columns <<---------------------------
        data_desc = data.describe().T
        data = data[list(data_desc[data_desc['std'] > 0].index)]
        self.LogsList.append(["Preprocess", "Removing unnecessary columns", datetime.now(), f"{FileNames['Single_FileName']}", f"{list(data_desc[data_desc['std'] > 0].index)} columns are removed", ""])

        # -------------------------------->> Splitting Predictors and Target Variables <<-------------------------------
        X = data.iloc[:, :-1]
        y = data.iloc[:, -1]
        #self.AcceptedColumns_TrainingData = list(X.columns)
        # --------------------------------------------->> Scaling Predictors <<-----------------------------------------
        X_Scale = pd.DataFrame(MinMaxScaler().fit_transform(X), columns=X.columns)
        self.LogsList.append(["Preprocess", "Scaling values", datetime.now(), f"{FileNames['Single_FileName']}", f"Scaling only Predictors", ""])

        # ----------------------------->> Saving Scaled values into a ScaledFile <<-------------------------------------
        pd.concat([X_Scale, y], axis=1).to_csv(LocalPaths['SingleFile'] + FileNames['SingleScaled_FileName'], index=False)
        self.LogsList.append(["Preprocess", "Storing File", datetime.now(), f"{FileNames['Single_FileName']}", f"SinglScaledFile is stored in SingleFiles fodler", ""])

        self.LogsList.append(["Preprocess", "Preprocessing Training data", datetime.now(), "", "Completed", ""])
        self.Logs.storeLogs(self.LogsList)
        # ======================================================| *** |=================================================

    def preprocesPredictingsData(self, LocalPaths, FileNames):
        self.LogsList.append(["Preprocess", "Preprocessing Predicting data", datetime.now(), "", "Started", ""])
        data = pd.read_csv(LocalPaths['SingleFile'] + FileNames['Single_FileName'])
        self.LogsList.append(["Preprocess", "Loading File", datetime.now(), f"{FileNames['Single_FileName']}", "Data frame created with the file", ""])
        # ------------------------------>> FeatureSelection : Removing unnecessary columns <<---------------------------
        Scaled_Data = pd.read_csv(self.LocalTrainingPaths['SingleFile'] + FileNames['SingleScaled_FileName'])
        self.LogsList.append(["Preprocess", "Loading File", datetime.now(), f"{FileNames['SingleScaled_FileName']}", f"Scaling Predictors", ""])
        Scaled_Data = Scaled_Data.iloc[:,:-1]
        data = data[Scaled_Data.columns]

        # --------------------------------------------->> Scaling Predictors <<-----------------------------------------
        data_Scale = pd.DataFrame(MinMaxScaler().fit_transform(data), columns=data.columns)
        self.LogsList.append(["Preprocess", "Scaling values", datetime.now(), f"{FileNames['Single_FileName']}", f"Scaling Predictors", ""])

        # ----------------------------->> Saving Scaled values into a ScaledFile <<-------------------------------------
        data_Scale.to_csv(LocalPaths['SingleFile'] + FileNames["SingleScaled_FileName"], index=False)
        self.LogsList.append(["Preprocess", "Storing File", datetime.now(), f"{FileNames['SingleScaled_FileName']}", f"SinglScaledFile is stored in SingleFiles fodler", ""])

        self.LogsList.append(["Preprocess", "Preprocessing Predicting data", datetime.now(), "", "Completed", ""])
        self.Logs.storeLogs(self.LogsList)
        # ======================================================| *** |=================================================