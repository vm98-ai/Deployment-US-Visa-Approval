import numpy
import pandas as pd
import sys
from us_visa.exception import USvisaException
from sklearn.pipeline import Pipeline
from us_visa.logger import logging

class TargetValueMapping:
    def __init__(self):
        self.Certified:int = 0
        self.Denied:int = 1

    def _asdict(self):
        return self.__dict__

    def reverse_mapping(self):
        mapping_response = self._asdict()
        return dict(zip(mapping_response.values(), mapping_response.keys()))

class USvisaModel:
    def __init__(self, preprocessing_object: Pipeline, trained_model_object: object):
        
        self.preprocessing_object = preprocessing_object
        self.trained_model_object = trained_model_object

    def predict(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        
        logging.info("Entered predict method of UTruckModel class")

        try:
            logging.info("Using the trained model to get predictions")

            transformed_feature = self.preprocessing_object.transform(dataframe)

            logging.info("Used the trained model to get predictions")
            return self.trained_model_object.predict(transformed_feature)

        except Exception as e:
            raise USvisaException(e, sys) from e

    def __repr__(self):
        return f"{type(self.trained_model_object).__name__}()"

    def __str__(self):
        return f"{type(self.trained_model_object).__name__}()"