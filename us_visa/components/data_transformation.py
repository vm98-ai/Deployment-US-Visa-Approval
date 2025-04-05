import sys
import numpy as np
import pandas as pd
from us_visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact, DataTransformationArtifact
from us_visa.entity.config_entity import DataTransformationConfig
from us_visa.logger import logging
from us_visa.exception import USvisaException
from us_visa.constants import TARGET_COLUMN, SCHEMA_FILE_PATH, CURRENT_YEAR
from us_visa.utils.main_utils import save_numpy_array_data, save_object, read_yaml_file, drop_columns
from us_visa.entity.estimator import TargetValueMapping
from imblearn.combine import SMOTEENN
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder, PowerTransformer
from sklearn.compose import ColumnTransformer

class DataTransformation:
    def __init__(self, data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_artifact:DataValidationArtifact,
                 data_transformation_config:DataTransformationConfig):
        
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
            self.schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e,sys)


    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            file = pd.read_csv(file_path)
            return file
        except Exception as e:
            raise USvisaException(e,sys)


    def get_data_transfomer_object(self) -> Pipeline:


        logging.info("Initiated the get data transformer object protocol")

        try:
            logging.info("Got numerical columns from schema config")
            numeric_transformer = StandardScaler()
            oh_transformer = OneHotEncoder()
            ordinal_encoder = OrdinalEncoder()

            logging.info("Initialized Standard Scalar, OneHotEncoder, OrdinalEncoder")

            or_columns = self.schema_config["or_columns"]
            oh_columns = self.schema_config["oh_columns"]
            transform_columns = self.schema_config["transform_columns"]
            num_features = self.schema_config["num_features"]

            logging.info("Initiate PowerTransformer Protocol")

            transform_pipe = Pipeline(steps=[('transformer', PowerTransformer(method='yeo-johnson'))])         

            preprocessor = ColumnTransformer(
                [
                    ("OneHotEncoder", oh_transformer, oh_columns),
                    ("Ordinal_Encoder", ordinal_encoder, or_columns),
                    ("Transformer", transform_pipe, transform_columns),
                    ("StandardScaler", numeric_transformer, num_features)
                ]
            ) 

            logging.info("Finished PowerTransformer Protocol Scenario")
            logging.info("Created ColumnTransformer Object")

            return preprocessor

        except Exception as e:
            raise USvisaException(e,sys) from e


    def initiate_data_transformation(self, ) -> DataTransformationArtifact:

        try:
            if self.data_validation_artifact.validation_status:
                logging.info("Initiating Data Transformation Protocol............")
                preprocessor = self.get_data_transfomer_object()
                logging.info("Received Preprocessor Object")

                train_df = DataTransformation.read_data(self.data_ingestion_artifact.trained_file_path)
                test_df = DataTransformation.read_data(self.data_ingestion_artifact.test_file_path)

                input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
                target_feature_train_df = train_df[TARGET_COLUMN]

                logging.info("Separated Feature and Target")

                input_feature_train_df["company_age"] = CURRENT_YEAR - input_feature_train_df["yr_of_estab"]

                logging.info("Added Company age column to the data")

                drop_cols = self.schema_config["drop_columns"]

                input_feature_train_df = drop_columns(df=input_feature_train_df, cols=drop_cols)
                
                logging.info("Dropped the irrevalent columns")
                
                targetvalue = TargetValueMapping()
                target_feature_train_df = target_feature_train_df.replace(
                    targetvalue._asdict()
                )

                input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
                target_feature_test_df = test_df[TARGET_COLUMN]

                logging.info("Separated Feature and Target for test data")

                input_feature_test_df["company_age"] = CURRENT_YEAR - input_feature_test_df["yr_of_estab"]

                logging.info("Added Company age column to the test data")

                drop_cols = self.schema_config["drop_columns"]

                input_feature_test_df = drop_columns(df=input_feature_test_df, cols=drop_cols)
                
                logging.info("Dropped the irrevalent columns in test data")

                target_feature_test_df = target_feature_test_df.replace(
                    targetvalue._asdict()
                )

                input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
                logging.info("Used Preprocessor object to transform train features")
                input_feature_test_arr = preprocessor.transform(input_feature_test_df)
                logging.info("Used Preprocessor object to transform test features")

                logging.info("Starting SMOTENN.........")

                smt = SMOTEENN(sampling_strategy="minority")

                input_feature_train_final,target_feature_train_final = smt.fit_resample(input_feature_train_arr,target_feature_train_df)

                logging.info("Applied SMOTENN on train input features")

                input_feature_test_final, target_feature_test_final = smt.fit_resample(input_feature_test_arr,target_feature_test_df)

                logging.info("Applied SMOTENN on test input features")

                train_arr = np.c_[
                    input_feature_train_final, np.array(target_feature_train_final)
                ]

                test_arr = np.c_[
                    input_feature_test_final, np.array(target_feature_test_final)
                ]

                logging.info("Train and test array making completed")

                save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)
                save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, train_arr)
                save_numpy_array_data(self.data_transformation_config.transformed_test_file_path,test_arr)

                logging.info("Saving preprocess object")
                logging.info("Exiting Data transformation Protocol...........")

                data_transformation_artifact = DataTransformationArtifact(
                    transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                    transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                    transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
                )

                return data_transformation_artifact
            
        except Exception as e:
            raise USvisaException(e,sys) from e





