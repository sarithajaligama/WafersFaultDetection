import csv

awsfile = csv.reader(open('saritha_AWS.csv', 'r'))
mongodbfile = csv.reader(open('MongoDB.csv', 'r'))
Aws = [row[1] for row in awsfile]
mongoDB = [row[1] for row in mongodbfile]

class GlobalVariablesPaths:
    def __init__(self):

        self.AwsVariables = {"BucketName"               : "faultdetectioninwafer",
                             "aws_access_key_id"        : Aws[0],
                             "aws_secret_access_key"    : Aws[1],
                             "region_name"              : Aws[2].split(' ')[-1]
                             }

        self.MdbVariables = {"UserId"                    : mongoDB[0],
                             "Pw"                        : mongoDB[1],
                             "Cluster"                   : mongoDB[2],
                             "DbName"                    : mongoDB[3]
                             }

        # --------------------------------------------------------------------------------------------------------------

        self.LocalTrainingPaths = {"RawFiles"            : "Temp_Files/Training/RawFiles/",
                                   "GoodFiles"           : "Temp_Files/Training/GoodFiles/",
                                   "BadFiles"            : "Temp_Files/Training/BadFiles/",
                                   "TransformedFiles"    : "Temp_Files/Training/TransformedFiles/",
                                   "SingleFile"          : "Temp_Files/Training/SingleFile/",
                                   "SchemaFile"          : "Temp_Files/Training/SchemaFile/",
                                   "LogFiles"            : "Temp_Files/Training/LogFiles/",
                                   "Clusters"            : "Temp_Files/Training/Models/Clusters/",
                                   "BestModels"          : "Temp_Files/Training/Models/BestModels/"
                                   }

        self.LocalPredictingPaths = {"RawFiles"          : "Temp_Files/Predicting/RawFiles/",
                                   "GoodFiles"           : "Temp_Files/Predicting/GoodFiles/",
                                   "BadFiles"            : "Temp_Files/Predicting/BadFiles/",
                                   "TransformedFiles"    : "Temp_Files/Predicting/TransformedFiles/",
                                   "SingleFile"          : "Temp_Files/Predicting/SingleFile/",
                                   "SchemaFile"          : "Temp_Files/Predicting/SchemaFile/",
                                   "LogFiles"            : "Temp_Files/Predicting/LogFiles/",
                                   "Clusters"            : "Temp_Files/Predicting/Models/Clusters/",
                                   "BestModels"          : "Temp_Files/Predicting/Models/BestModels/",
                                   "PredictedFiles"      : "Temp_Files/Predicting/Predictions/"
                                   }

        # --------------------------------------------------------------------------------------------------------------

        self.CloudTrainingPaths = {"RawFiles"            : "Training/RawFiles/",
                                   "GoodFiles"           : "Training/GoodFiles/",
                                   "BadFiles"            : "Training/BadFiles/",
                                   "TransformedFiles"    : "Training/TransformedFiles/",
                                   "SingleFile"          : "Training/SingleFile/",
                                   "SchemaFile"          : "Training/SchemaFile/",
                                   "LogFiles"            : "Training/LogFiles/",
                                   "Clusters"            : "Training/Clusters/",
                                   "BestModels"          : "Training/BestModels/"
                                   }

        self.CloudPredictingPaths = {"RawFiles"          : "Predicting/RawFiles/",
                                     "GoodFiles"         : "Predicting/GoodFiles/",
                                     "BadFiles"          : "Predicting/BadFiles/",
                                     "TransformedFiles"  : "Predicting/TransformedFiles/",
                                     "SingleFile"        : "Predicting/SingleFile/",
                                     "SchemaFile"        : "Predicting/SchemaFile/",
                                     "LogFiles"          : "Predicting/LogFiles/",
                                     "Clusters"          : "Predicting/Clusters/",
                                     "BestModels"        : "Predicting/BestModels/",
                                     "PredictedFiles"    : "Predicting/Predictions/"
                                     }

        # --------------------------------------------------------------------------------------------------------------

        self.FileNames = {"SchemaTraining_FileName"      : "schema_training.json",
                          "SchemaPredicting_FileName"    : "schema_prediction.json",
                          "MissingValues_FileName"       : "MissingValues_of_Files.csv",
                          "Single_FileName"              : "SingleFile.csv",
                          "SingleScaled_FileName"        : "SingleScaled_File.csv",
                          "Cluster_FileName"             : "KMeans_Cluster.sav",
                          "BestModel_FileName"           : "Model_for_Cluster_",
                          "Prediction_FileName"          : "Predictions.csv",
                          "Logs_Transactions_FileName"   : "Logs_of_AllTransactions.csv",
                          "Logs_LocalFolderCreation_FileName": "Logs_of_LocalFolderCreation.csv",
                          "Logs_Downloading_FileName"    : "Logs_of_Downloading.csv",
                          "Logs_Validation_FileName"     : "Logs_of_Validated.csv",
                          "Logs_Transform_FileName"      : "Logs_of_Transform.csv",
                          "Logs_Merging_FileName"        : "Logs_of_Merging.csv",
                          "Logs_Preprocess_FileName"     : "Logs_of_Preprocess.csv",
                          "Logs_TrainingModel_FileName"  : "Logs_of_TrainingModel.csv",
                          "Logs_Predicting_FileName"     : "Logs_of_Predictions.csv",
                          "Logs_Uploading_FileName"      : "Logs_of_Uploading.csv",
                          "Logs_Tracks"                  : {"Task_Name"    : "",
                                                            "Task_Desc"    : "",
                                                            "Date_Time"    : "",
                                                            "File_Name"    : "",
                                                            "Log_Message"  : "",
                                                            "Action_Taken" : ""
                                                            }
                          }

        # --------------------------------------------------------------------------------------------------------------
        pass