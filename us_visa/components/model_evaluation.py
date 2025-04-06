from us_visa.entity.config_entity import ModelEvaluationConfig
from us_visa.entity.artifact_entity import ModelEvaluationArtifact, ModelTrainerArtifact, DataIngestionArtifact
from sklearn.metrics import accuracy_score, f1_score
from us_visa.exception import USvisaException
from us_visa.constants import TARGET_COLUMN, CURRENT_YEAR
from us_visa.logger import logging
import sys
import pandas as pd
from typing import Optional
from us_visa.entity.s3_estimator import UsvisaEstimator
from dataclasses import dataclass
from us_visa.entity.estimator import USvisaModel
from us_visa.entity.estimator import TargetValueMapping


@dataclass
class EvaluateModelResponse:
    trained_model_f1_score : float
    best_model_f1_score:float
    is_model_accepted:bool
    difference:float

class ModelEvaluation:

    def __init__(self, model_eval_config:ModelEvaluationConfig, data_ingestion_artifact:DataIngestionArtifact,
                 model_trainer_artifact: ModelTrainerArtifact):

        try:
            self.model_eval_config = model_eval_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.model_trainer_artifact= model_trainer_artifact    

        except Exception as e:
            raise USvisaException(e,sys)

    def get_best_model(self) -> Optional[UsvisaEstimator]:

        try:
            bucket_name = self.model_eval_config.bucket_name
            model_path = self.model_eval_config.s3_model_key_path
            usvisa_estimator = UsvisaEstimator(bucket_name=bucket_name,
                                               model_path=model_path)
            
            if usvisa_estimator.is_model_available(model_path=model_path):
                return usvisa_estimator
            return None
        except Exception as e:
            raise USvisaException(e,sys) from e
        
    def evaluate_model(self) -> EvaluateModelResponse:

        try:
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            test_df['comapny_age'] = CURRENT_YEAR - test_df['yr_of_estab'] 
            x,y = test_df.drop(TARGET_COLUMN, axis=1), test_df[TARGET_COLUMN]
            y = y.replace(
                TargetValueMapping()._asdict()
            )
            trained_model_f1_score = self.model_trainer_artifact.metric_artifact.f1_score

            best_model_f1_score = None
            best_model = self.get_best_model()
            if best_model is not None:
                y_hat_best_model = best_model.predict(x)
                best_model_f1_score = f1_score(y,y_hat_best_model)

            tmp_best_model_score = 0 if best_model_f1_score is None else best_model_f1_score
            result = EvaluateModelResponse(trained_model_f1_score=trained_model_f1_score,
                                           best_model_f1_score=best_model_f1_score,
                                           is_model_accepted=trained_model_f1_score>tmp_best_model_score,
                                           difference=trained_model_f1_score-tmp_best_model_score)

            logging.info(f"result:{result}")
            return result
        except Exception as e:
            raise USvisaException(e,sys) from e       

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:

        try:
            evaluate_model_response = self.evaluate_model()
            s3_model_path = self.model_eval_config.s3_model_key_path


            model_evaluation_artifact = ModelEvaluationArtifact(
                is_model_accepted=evaluate_model_response.is_model_accepted,
                s3_model_path=s3_model_path,
                trained_model_path=self.model_trainer_artifact.trained_model_file_path,
                changed_accuracy=evaluate_model_response.difference
            )
            logging.info(f"Model Evaluation Artifact: {model_evaluation_artifact}")
            return model_evaluation_artifact
        
        except Exception as e:
            raise USvisaException(e,sys) from e