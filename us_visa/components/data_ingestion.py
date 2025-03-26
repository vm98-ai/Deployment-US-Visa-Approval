import os
import sys

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from us_visa.entity.config_entity import DataIngestionConfig     #returns all paths
from us_visa.entity.artifact_entity import DataIngestionArtifact
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.data_access.usvisa_data import USvisaData


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig=DataIngestionConfig()):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise USvisaException(e,sys)
        
    def export_data_into_feature_store(self) -> DataFrame:

        try:
            logging("Taking data from mongodb")
            usvisa_data = USvisaData()
            dataframe = usvisa_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            logging.info(f"Shape of the Dataframe: {dataframe.shape}")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedir(dir_path,exist_ok=True)
            logging.info(f"Saving the data to : {feature_store_file_path} ")
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise USvisaException(e,sys)
        

    def split_data_as_train_test(self,dataframe:DataFrame) -> None:

        logging.info("Entered train_test_split")

        try:
            train_set, test_set = train_test_split(dataframe, self.data_ingestion_config.train_test_split_ratio,random_state=42)
            train_file_path = self.data_ingestion_config.training_file_path
            dir_path = os.path.dirname(train_file_path)
            os.makedir(dir_path,exist_ok = True)
            test_file_path = self.data_ingestion_config.test_file_path
            dir_path1 = os.path.dirname(test_file_path)
            os.makedir(dir_path1,exist_ok = True)
            train_set.to_csv(train_file_path,index=False,header=True)
            test_set.to_csv(test_file_path,indes=False,header=True)
            logging.info("done train and test")

        except Exception as e:
            raise USvisaException(e,sys)    


    def initiate_data_ingestion(self) -> DataIngestionArtifact:

        logging.info("Enter inintiate data ingestion method")

        try:

            dataframe = self.export_data_into_feature_store()

            logging.info("Data to feature store")

            self.split_data_as_train_test(dataframe)

            logging.info("Train Test Split Done. Exiting the data ingestion")

            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                            test_file_path=self.data_ingestion_config.testing_file_path)
            
            logging.info(f"Data ingestion artifact :{data_ingestion_artifact}")
            return data_ingestion_artifact

        except Exception as e:
            raise USvisaException(e,sys)    



