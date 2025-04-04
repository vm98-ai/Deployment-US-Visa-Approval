import os
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidation
from us_visa.entity.config_entity import DataIngestionConfig, DataValidationConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
import sys


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        


    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info("Started Data Ingestion Protocol")
            logging.info("Getting data from MongoDB")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info("Successfully got the data")

            return data_ingestion_artifact    
        
        except Exception as e:
            raise USvisaException(e,sys) from e


    def start_data_validation(self, data_ingestion_artifact:DataIngestionArtifact) -> DataValidationArtifact:
        try:
            logging.info("Started data validation Protocol")
            data_validation = DataValidation(data_validation_config=self.data_validation_config,data_ingestion_artifact=self.data_ingestion_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info("Successfully Finished Data Validation")
            return data_validation_artifact
        except Exception as e:
            raise USvisaException(e,sys) from e


    def run_pipeline(self,) -> None:
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)            

        except Exception as e:
            raise USvisaException(e,sys) from e