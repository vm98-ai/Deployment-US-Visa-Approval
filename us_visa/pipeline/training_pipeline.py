import os
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.components.data_ingestion import DataIngestion
from us_visa.components.data_validation import DataValidation
from us_visa.components.data_transformation import DataTransformation
from us_visa.components.model_trainer import ModelTrainer
from us_visa.entity.config_entity import (DataIngestionConfig,
                                          DataValidationConfig,
                                          DataTransformationConfig,
                                          ModelTrainerConfig)
from us_visa.entity.artifact_entity import (DataIngestionArtifact,
                                            DataValidationArtifact,
                                            DataTransformationArtifact,
                                            ModelTrainerArtifact)
import sys


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()
        


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
            logging.info("Started data validation Protocol..................")
            data_validation = DataValidation(data_validation_config=self.data_validation_config,data_ingestion_artifact=self.data_ingestion_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info("Successfully Finished Data Validation..............")
            return data_validation_artifact
        except Exception as e:
            raise USvisaException(e,sys) from e

    
    def start_data_transformation(self, data_ingestion_artifact:DataIngestionArtifact, data_validation_artifact:DataValidationArtifact) -> DataTransformationArtifact:
        try:
            logging.info("Started data Transformation Protocol..............")
            data_transformation = DataTransformation(data_validation_artifact=data_validation_artifact,
                                                 data_ingestion_artifact=data_ingestion_artifact,
                                                 data_transformation_config=self.data_transformation_config)
            data_transformation_artifact = data_transformation.initiate_data_transformation()
            logging.info("Successfully Finished Data Transformation...........................")
            return data_transformation_artifact
        except Exception as e:
            raise USvisaException(e,sys) from e
        

    def start_model_training(self, data_transformation_artifact:DataTransformationArtifact) -> ModelTrainerArtifact:
        try:
            logging.info("Started Model Training Protocol....................")
            model_trainer = ModelTrainer(
                data_transformation_artifact=data_transformation_artifact,
                model_trainer_config=self.model_trainer_config                
            )
            model_trainer_artifact = model_trainer.initiate_model_trainer()
            logging.info("Successfully finished model training.....................")
            return model_trainer_artifact
        
        except Exception as e:
            raise USvisaException(e,sys) from e

        

    def run_pipeline(self,) -> None:
        try:
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(data_ingestion_artifact=data_ingestion_artifact, data_validation_artifact=data_validation_artifact)            
            model_trainer_artifact = self.start_model_training(data_transformation_artifact=data_transformation_artifact)
        except Exception as e:
            raise USvisaException(e,sys) from e