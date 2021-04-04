
# =============================================|*|| Required Libraries ||*|=============================================
import os
from datetime import datetime
import shutil
import boto3
import pandas as pd
from glob import glob
# =======================================================| *** |========================================================

# =============================================|*|| User defined Packages ||*|==========================================
# Ex: from FolderName.PackageName import LibraryName(Class)
from Global_Variables.Global_Variables import GlobalVariablesPaths
from Data_Logs.Logs import Logs
# =======================================================| *** |========================================================

# ============================================||*|| Create Directories ||*|=============================================
class Local:
    def __init__(self, LogsPath, LogsFileName):
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()

    def FindDir(self, DirName):
        True_False = os.path.isdir(DirName)
        return True_False

    def CreateDir(self, DirName):
        if not self.FindDir(DirName):
            os.mkdir(DirName)
            Status = f"{DirName} is created successfully"
        else:
            Status = f"{DirName} is already created"
        return Status

    def removeDir(self, DirName):
        self.LogsList.append(["Local Folder Creation", f"Removing Folder in Local", datetime.now(), "", "Started", ""])
        if self.FindDir(DirName):
            shutil.rmtree(DirName)
            self.LogsList.append(["Local Folder Creation", f"Removing Folder in Local", f"{datetime.now()}", "",f"'{DirName}' is removed", ""])
        else:
            self.LogsList.append(["Local Folder Creation", f"Removing Folder in Local", f"{datetime.now()}", "", f"'{DirName}' is not available", ""])
        self.LogsList.append(["Local Folder Creation", f"Removing Folder in Local", f"{datetime.now()}", "", "Completed", ""])

    def createDirectories(self, Dict):
        try:
            self.LogsList.append(["Local Folder Creation", f"Creating Folders in Local", datetime.now(), "", "Started", ""])
            Org_Dir = os.getcwd()                                                   # Original Directory
            # Loop for creating Training directory in "Temp_Files"
            for path in list(Dict.values()):                                        # ex: "Temp_Files/Training/RawFiles/"
                self.LogsList.append(["Local Folder Creation", f"Creating folder structure: {path}", datetime.now(), "", "Started", ""])
                pathList = path.split('/')[:-1]                                     # pathList = ['"Temp_Files','Training','RawFiles/"]
                # Loop for creating sub-directories in Training Directory
                for DirName in pathList:                                            # DirName = "Temp_Files" in 1st loop
                    Status = self.CreateDir(DirName)                                # Directory is created if it doesn't exist
                    self.LogsList.append(["Local Folder Creation", f"Creating folder: {DirName}", datetime.now(), "", f"{Status}", ""])
                    os.chdir(os.path.join(os.getcwd(), DirName))                    # Adding 'Temp_Files' directory path to the original directory path
                    self.LogsList.append(["Local Folder Creation", f"Creating folder structure: {path}", datetime.now(), "", "Completed",""])
                os.chdir(Org_Dir)                                                   # Change back to Original Directory
            self.LogsList.append(["Local Folder Creation", f"Creating Folders in Local", datetime.now(), "", "Completed", ""])
            self.LogsList = self.Logs.storeLogs(self.LogsList)
        except Exception as e:
            self.LogsList.append(["Local Folder Creation", f"Exception", datetime.now(), "", f"Error: {e}", f"{type(self.LogsList)}"])
            self.LogsList = self.Logs.storeLogs(self.LogsList)
# =======================================================| *** |========================================================

# ===================================================||*|| AWS ||*|=====================================================
class AWS:

    def __init__(self, LogsPath, LogsFileName):
        self.GVP = GlobalVariablesPaths()
        self.AwsVar = self.GVP.AwsVariables
        self.BucketName = self.AwsVar['BucketName']
        self.Logs = Logs(LogsPath, LogsFileName)
        self.LogsList = list()
        self.LogsList.append(["AWS Connection", f"Session, Resource, Client connections", datetime.now(), "", "Started", ""])

        try:
            self.MySession = boto3.Session(aws_access_key_id=self.AwsVar['aws_access_key_id'],
                                           aws_secret_access_key=self.AwsVar['aws_secret_access_key'],
                                           region_name=self.AwsVar['region_name'])
            self.LogsList.append(["AWS Connection", f"Session Connection", datetime.now(), "", "Session connection Successful", ""])
            self.resource = self.MySession.resource(service_name="s3")                                     # Connect to "s3"
            self.LogsList.append(["AWS Connection", f"Resource Connection", datetime.now(), "", "Resource connection Successful", ""])
            self.client = self.MySession.client(service_name="s3")                                         # Connect to server "s3"
            self.LogsList.append(["AWS Connection", f"Client Connection", datetime.now(), "", "Client connection Successful", ""])
        except Exception as e:
            self.LogsList.append(["AWS Connection", f"Session, Resource, Client connections", datetime.now(), "", f"Error: {e}", "Check Error in Function: 'AWS()'"])
            self.LogsList = self.Logs.storeLogs(self.LogsList)


    # -------------------------------------------|| List of Files in a Folder||-----------------------------------------
    def download_Files_From_AWS(self, Cloud_FilesPath, Local_FilesPath):
        self.LogsList.append(["Download", f"Downloading From: {Cloud_FilesPath} To: {Local_FilesPath}", datetime.now(), "", "Started",""])
        try:
            Bucket_Objects_Summary = self.resource.Bucket(self.BucketName)                                              # Connect to a Bucket
            Files_Path = [FilePath.key for FilePath in Bucket_Objects_Summary.objects.all() if FilePath.key[-1]!="/"]   # Getting all files from bucket
            Cloud_CsvFiles = [File_Path for File_Path in Files_Path if File_Path.split(File_Path.split('/')[-1])[0] == Cloud_FilesPath]  # Getting required files from "Cloud_FilesPath"
            self.LogsList.append(["Download", f"Access Files from Bucket: {self.BucketName}", datetime.now(), "", f"{len(Cloud_CsvFiles)} files are available", "Downloading Files"])

            # --------------------------->> Getting list of requires files from "Cloud_FilesPath"
            self.client = self.MySession.client(service_name="s3")                                         # connecting to server - s3
            for CloudFilePath in Cloud_CsvFiles:                                                                        # Loop for all files
                (_, FileName) = os.path.split(CloudFilePath)                                                            # Extracting file from filepath
                LocalFilePath = Local_FilesPath + FileName                                                              # Adding file name to Localfilepath
                self.client.download_file(self.BucketName, CloudFilePath, LocalFilePath)
                self.LogsList.append(["Download", f"Downloading File", datetime.now(), f"{CloudFilePath}", f"Downloaded Successfully", ""])
        except Exception as e:
            self.LogsList.append(["Download", f"Downloading Failed", datetime.now(), "", f"Error: {e}", "Check Error in Function: 'Download_AllFiles_From_AWS_to_Local()'"])
            self.LogsList = self.Logs.storeLogs(self.LogsList)
        self.LogsList.append(["Download", f"Downloading From: {Cloud_FilesPath} To: {Local_FilesPath}", datetime.now(), "", "Completed", f"{len(Cloud_CsvFiles)} files are downloaded successfully"])
        self.LogsList = self.Logs.storeLogs(self.LogsList)
    # ------------------------------------------------------------------------------------------------------------------

    # -------------------------------------------|| List of Files in a Folder||-----------------------------------------
    def uploadAllFiles(self, Local_FilesPath, Cloud_FilesPath):
        self.LogsList.append(["Upload", f"Uploading From: {Local_FilesPath} To: {Cloud_FilesPath}", datetime.now(), "", "Started", ""])
        try:
            ListOfFiles = os.listdir(Local_FilesPath)
            for file in ListOfFiles:                                                                                    # Loop for all files
                LocalFile = Local_FilesPath + file                                                                      # Extracting file from filepath
                CloudFile = Cloud_FilesPath + file                                                                      # Adding file name to Localfilepath
                self.client.upload_file(LocalFile, self.BucketName, CloudFile)                                          # Upload file to the localpath
                self.LogsList.append(["Upload", f"Uploading File", datetime.now(), f"{file}", "Uploaded Successfully", ""])
        except Exception as e:
            self.LogsList.append(["Upload", f"Uploading Failed", datetime.now(), "", f"Error: {e}", "Check Error in Function: 'Upload_AllFiles_from_Local_to_AWS()'"])
            self.LogsList = self.Logs.storeLogs(self.LogsList)
        self.LogsList.append(["Upload", f"Uploading From: {Local_FilesPath} To: {Cloud_FilesPath}", datetime.now(), "", "Completed", f"{len(ListOfFiles)} files are uploaded successfully"])
        self.LogsList = self.Logs.storeLogs(self.LogsList)
    # ------------------------------------------------------------------------------------------------------------------

    def Delete_Folder_from_Bucket(self, Folder_Name):
        Bucket_Objects_Summary = self.resource.Bucket(self.BucketName)                                                  # Connect to a Bucket
        for FilePath in Bucket_Objects_Summary.objects.all():
            if FilePath.key[-1] != '/':
                if FilePath.key.split(FilePath.key.split('/')[-1])[0] == Folder_Name:
                    self.client.delete_object(Bucket=self.BucketName, Key=FilePath.key)

    def uploadAllFolders(self, Local_FoldersPath, Cloud_FoldersPath):
        for key in Local_FoldersPath.keys():
            if key not in ['RawFiles', 'SchemaFile']:
                self.Delete_Folder_from_Bucket(Cloud_FoldersPath[key])                                                  # Delete folder in the Bucket
                self.LogsList.append(["Upload", f"Deleting Folder from Bucket", datetime.now(), "", "Deleted", f"Folder {Cloud_FoldersPath[key]} is successfully deleted from Bucket {self.BucketName}"])
                self.uploadAllFiles(Local_FoldersPath[key], Cloud_FoldersPath[key])

    # ------------------------------------------------------------------------------------------------------------------
# =======================================================| *** |========================================================
