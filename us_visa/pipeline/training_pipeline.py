import os
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.components.data_ingestion import DataIngestion
from us_visa.entity.config_entity import DataIngestionConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact
import sys


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        


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


    def run_pipeline(self,) -> None:
        try:
            data_ingestion_artifact = self.start_data_ingestion()            

        except Exception as e:
            raise USvisaException(e,sys) from e