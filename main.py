# ===========================================|*|| Required Libraries ||*|===============================================
from flask import Flask, render_template
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~| *** |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ===========================================|*|| User defined Packages ||*|============================================
# Ex: from FolderName.PackageName import LibraryName(Class)
from Global_Variables.Global_Variables import GlobalVariablesPaths
from Data_Load.Data_Load import AWS
from Data_Load.Data_Load import Local
from Data_MergeFiles.Merge_Files import Merge
from Data_Models.Models import Model
from Data_Preprocess.Preprocess import Preprocess
from Data_Transform.Data_Transform import Transform
from Data_Validations.Validations import Validations
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~| *** |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

app = Flask(__name__)


# ================================================||*|| Home page ||*|==================================================
@app.route("/", methods=['GET'])
def home():
    return render_template('index.html')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~| *** |~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# =================================================||*|| Training ||*|==================================================
@app.route("/train", methods=['GET'])
def trainRouteClient():
    # Loading "LocalTrainingPaths" , "AWS variables path" and "MongoDB variables path"

    GVP = GlobalVariablesPaths()
    LocalTrainingPaths = GVP.LocalTrainingPaths
    CloudTrainingPaths = GVP.CloudTrainingPaths
    FileNames = GVP.FileNames

    # ----------------->> Creating Folder Structure in Local Drive
    local = Local(LocalTrainingPaths["LogFiles"], FileNames["Logs_LocalFolderCreation_FileName"])
    local.removeDir("Temp_Files")
    local.createDirectories(LocalTrainingPaths)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 01 - Download ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    aws = AWS(LocalTrainingPaths["LogFiles"], FileNames["Logs_Downloading_FileName"])
    aws.download_Files_From_AWS(CloudTrainingPaths['RawFiles'], LocalTrainingPaths['RawFiles'])
    aws.download_Files_From_AWS(CloudTrainingPaths["SchemaFile"], LocalTrainingPaths["SchemaFile"])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 02 - Validation ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    validations = Validations(LocalTrainingPaths["LogFiles"], FileNames["Logs_Validation_FileName"])
    validations.validating_RawFiles(LocalTrainingPaths, FileNames["SchemaTraining_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 03 - Transform ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    transform = Transform(LocalTrainingPaths["LogFiles"], FileNames["Logs_Transform_FileName"])
    transform.transformData(LocalTrainingPaths, FileNames["SchemaTraining_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 04 - Merge ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    merge = Merge(LocalTrainingPaths["LogFiles"], FileNames["Logs_Merging_FileName"])
    merge.mergeFiles(LocalTrainingPaths, FileNames["SchemaTraining_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 05 - Preprocess ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    preprocess = Preprocess(LocalTrainingPaths["LogFiles"], FileNames["Logs_Preprocess_FileName"])
    preprocess.preprocesTraningsData(LocalTrainingPaths, FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 06 - Model Building ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    model = Model(LocalTrainingPaths["LogFiles"], FileNames["Logs_TrainingModel_FileName"])
    model.trainingModel(LocalTrainingPaths, FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 07 - Upload ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    aws = AWS(LocalTrainingPaths["LogFiles"], FileNames["Logs_Downloading_FileName"])
    aws.uploadAllFiles(LocalTrainingPaths, CloudTrainingPaths)

# ======================================================| *** |=========================================================


# =================================================||*|| Predict ||*|===================================================
@app.route("/predict", methods=['GET'])
def PredictRoutClient():
    # Loading "LocalPredictingPaths" , "AWS variables path" and "MongoDB variables path"

    GVP = GlobalVariablesPaths()
    LocalPredictingPaths = GVP.LocalPredictingPaths
    CloudPredictingPaths = GVP.CloudPredictingPaths
    FileNames = GVP.FileNames

    # ----------------->> Creating Folder Structure in Local Drive
    local = Local(LocalPredictingPaths["LogFiles"], FileNames["Logs_LocalFolderCreation_FileName"])
    local.createDirectories(LocalPredictingPaths)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 01 - Download ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    aws = AWS(LocalPredictingPaths["LogFiles"], FileNames["Logs_Downloading_FileName"])
    aws.download_Files_From_AWS(CloudPredictingPaths['RawFiles'], LocalPredictingPaths['RawFiles'])
    aws.download_Files_From_AWS(CloudPredictingPaths['SchemaFile'], LocalPredictingPaths['SchemaFile'])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 02 - Validation ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    validations = Validations(LocalPredictingPaths["LogFiles"], FileNames["Logs_Validation_FileName"])
    validations.validating_RawFiles(LocalPredictingPaths, FileNames["SchemaPredicting_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 03 - Transform ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    transform = Transform(LocalPredictingPaths["LogFiles"], FileNames["Logs_Transform_FileName"])
    transform.transformData(LocalPredictingPaths, FileNames["SchemaPredicting_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 04 - Merge ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    merge = Merge(LocalPredictingPaths["LogFiles"], FileNames["Logs_Merging_FileName"])
    merge.mergeFiles(LocalPredictingPaths, FileNames["SchemaPredicting_FileName"], FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 05 - Preprocess ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    preprocess = Preprocess(LocalPredictingPaths["LogFiles"], FileNames["Logs_Preprocess_FileName"])
    preprocess.preprocesPredictingsData(LocalPredictingPaths, FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 06 - Model Building ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    model = Model(LocalPredictingPaths["LogFiles"], FileNames["Logs_Predicting_FileName"])
    model.predictingModel(LocalPredictingPaths, FileNames)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|| Step 07 - Upload ||~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    aws = AWS(LocalPredictingPaths["LogFiles"], FileNames["Logs_Downloading_FileName"])
    aws.uploadAllFolders(LocalPredictingPaths, CloudPredictingPaths)
# ======================================================| *** |=========================================================

# =================================================||*|| Port ||*|======================================================
if __name__ == "__main__":
    app.run(debug=True, port=8001)
# ======================================================| *** |=========================================================
