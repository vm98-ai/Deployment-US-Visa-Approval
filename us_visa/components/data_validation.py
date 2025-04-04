import json
import sys
import pandas as pd
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection
from pandas import DataFrame
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.entity.artifact_entity import DataValidationArtifact
from us_visa.entity.config_entity import DataValidationConfig, DataIngestionConfig
from us_visa.constants import SCHEMA_FILE_PATH
from us_visa.utils.main_utils import read_yaml_file, write_yam_file



class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionConfig, data_validation_config:DataValidationConfig):
        try :
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e,sys)


    def validate_number_of_columns(self, dataframe:DataFrame) -> bool:
        try:
            status = len(dataframe.columns) == len(self.schema_config["columns"])
            logging.info(f"Is the required column present {status}")
            return status        
        except Exception as e:
            raise USvisaException(e,sys)
        
    def is_column_exist(self, df:DataFrame) -> bool:

        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self.schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            for column in self.schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if len(missing_categorical_columns) > 0:
                logging.info(f"The missing Categorical columns are {missing_categorical_columns}")

            if len(missing_numerical_columns)>0:
                logging.info(f"The missing numerical columns are {missing_numerical_columns}")

            return False if len(missing_categorical_columns)>0 or len(missing_numerical_columns)>0 else True

        except Exception as e:
            raise USvisaException(e,sys)
    
    @staticmethod
    def read_data(file_path) -> DataFrame:
        try:

            file = pd.read_csv(file_path)
            return file
        except Exception as e:
            raise USvisaException(e,sys)
        

    def detect_data_drift(self, reference_df : DataFrame, current_df : DataFrame, ) -> bool:

        try:
            data_drift_profile = Profile(sections=[DataDriftProfileSection()])
            data_drift_profile.calculate(reference_df, current_df)
            report = data_drift_profile.json()
            json_report = json.loads(report)
            write_yam_file(file_path=self.data_validation_config.drift_report_file_path, content=json_report)
            n_features = json_report["data_drift"]["data"]["metrics"]["n_features"]
            n_drifted_features = json_report["data_drift"]["data"]["metrics"]["n_drifted_features"]

            logging.info(f"{n_drifted_features}/{n_features} drift detected")
            drift_status = json_report["data_drift"]["data"]["metrics"]["dataset_drift"]
            return drift_status
        
        except Exception as e:
            raise USvisaException(e,sys) from e

    def initiate_data_validation(self) -> DataValidationArtifact:

        try:
            validation_error_message = ""
            logging.info("Initiate Data Validation Protocal Started")
            train_df, test_df = (DataValidation.read_data(file_path = self.data_ingestion_artifact.training_file_path),
                                 DataValidation.read_data(file_path = self.data_ingestion_artifact.test_file_path)) 

            status = self.validate_number_of_columns(dataframe=train_df)
            logging.info(f"All Columns are present {status}")
            if not status:
                validation_error_message += f"Columns are missing in the dataframe"

            status = self.validate_number_of_columns(dataframe=test_df)
            logging.info(f"All columns are present {status}")
            if not status:
                validation_error_message += f"Columns are missing in the dataframe"                     

            status = self.is_column_exist(df=train_df)
            logging.info(f"All columns are with correct name {status}")
            if not status:
                validation_error_message += f"Required Columns are missing in the dataframe"

            status = self.is_column_exist(df=test_df)
            logging.info(f"All columns are with correct name {status}")
            if not status:
                validation_error_message += f"Required Columns are missing in the dataframe"

            validation_status = len(validation_error_message) == 0

            
            if validation_status:
                drift_status = self.detect_data_drift(train_df,test_df)
                if drift_status:
                    logging.info(f"Drift detected")
                    validation_error_message = "Drift Detected"
                else:
                    validation_error_message = "Drift Not Detected"    
            else:
                logging.info(f"validation error {validation_error_message}")


            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message=validation_error_message,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            logging.info(f"Data validation Artifact {data_validation_artifact}")
            return data_validation_artifact 


        except Exception as e:
            raise USvisaException(e,sys)


