from sensor.entity import artifact_entity, config_entity
from sensor.exception import SensorException
from sensor.logger import logging
import os, sys
from scipy.stats import ks_2samp
import pandas as pd
from typing import Optional
from sensor import utils
import numpy as np 
from sensor.config import TARGET_COLUMN



class DataValidation:
    def __init__(self,data_validation_config:config_entity.DataValidationConfig,
                    data_ingestion_artifact:artifact_entity.DataIngestionArtifact):
        try:
            logging.info(f"{'>>'*20} Data Validation {'<<'*20}")
            self.data_validation_config=data_validation_config
            self.data_ingestion_artifact=data_ingestion_artifact
            self.validation_error=dict()
        except Exception as e:
            raise SensorException(e, sys)

    
    def drop_missing_values_columns(self,df:pd.DataFrame,report_key_name:str)->Optional[pd.DataFrame]:
        """
        This function will drop column which contains missing value more than specified threshold
        df: Accepts a pandas dataframe
        threshold: Percentage criteria to drop a column
        =====================================================================================
        returns Pandas DataFrame if atleast a single column is available after missing columns drop else None
        """

        try:
        
            threshold =self.data_validation_config.missing_threshold
            null_report = df.isna().sum()/df.shape[0]
            #selecting column name which contains null
            logging.info(f"selecting column name which contains null above to {threshold}")
            drop_column_names = null_report[null_report>threshold].index

            logging.info(f"columns to drop: {list(drop_column_names)}")
            self.validation_error[report_key_name]=list(drop_column_names)
            df.drop(list(drop_column_names),axis=1,inplace=True)

            #return None no columns left
            if len(df.columns)==0:
                return None
            return df
        
        except Exception as e:
            raise SensorException(e, sys)


    def is_required_columns_exist(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str)->bool:
        try:
        
            base_colunms =  base_df.columns
            current_columns= current_df.columns

            missing_columns = []
            for base_column in base_colunms:
                if base_column not in base_colunms:
                    logging.info(f"column: [{base}is not available]")
                    missing_columns.append(base_column)

            if len(missing_columns)>0:
                self.validation_error[report_key_name]=missing_columns
                return False
            return True
        except Exception as e:
            raise SensorException(e,sys)


    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str)->bool:
        try:
            drift_report=dict()
            base_colunms =  base_df.columns
            current_columns= current_df.columns

            for base_column in base_colunms:
            
                base_data,current_data=base_df[base_column],current_df[base_column]
                # null hypothesis is that both column data  drawn from same distribution
                
                logging.info(f" Hypothesis {base_column}:{base_data.dtype},{current_data.dtype}")
                same_distribution=ks_2samp(base_data,current_data)

                if same_distribution.pvalue>0.05:
                    # we are accepting null hypothesis
                    drift_report[base_column]={
                        "pvalues":float(same_distribution.pvalue),
                        "same_distribution":True
                    }
                    #same distribution
                else:
                    drift_report[base_column]={
                        "pvalues":float(same_distribution.pvalue),
                        "same_distribution":False
                    }
                    #different distribution
            self.validation_error[report_key_name]=drift_report

        except Exception as e:
            raise SensorException(e,sys)



        
    def initiate_data_validation(self)->artifact_entity.DataValidationArtifact:
        try:
            logging.info(f"Reading base dataframe")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({"na":np.NAN},inplace=True)
            logging.info(f"Replace na values in dataframe")

            #base_df has na as null
            logging.info(f"drop null values from base dataframe")
            base_df=self.drop_missing_values_columns(df=base_df,report_key_name="missing_values_within_base_dataset")

            logging.info(f"Reading train dataframe")
            train_df=pd.read_csv(self.data_ingestion_artifact.train_file_path)
            logging.info(f"Reading test dataframe")
            test_df=pd.read_csv(self.data_ingestion_artifact.test_file_path)

            logging.info(f"drop null values from train dataframe")
            train_df=self.drop_missing_values_columns(df=train_df,report_key_name="missing_values_within_train_dataset")
            logging.info(f"drop null values from test dataframe")
            test_df=self.drop_missing_values_columns(df=test_df,report_key_name="missing_values_within_test_dataset")

            exclude_columns = ["class"]
            base_df = utils.convert_columns_float(df=base_df,exclude_columns=exclude_columns)
            train_df = utils.convert_columns_float(df=train_df,exclude_columns=exclude_columns)
            test_df = utils.convert_columns_float(df=test_df,exclude_columns=exclude_columns)

            
            
            logging.info(f"is all required columns present in train df ")
            train_df_columns_status=self.is_required_columns_exist(base_df=base_df,current_df=train_df,report_key_name="missing_columns_within_train_dataset")
            logging.info(f"is all required columns present in test df ")
            test_df_columns_status=self.is_required_columns_exist(base_df=base_df,current_df=test_df,report_key_name="missing_columns_within_test_dataset")

            if train_df_columns_status:
                logging.info(f"as all column are available in train hence detecting data drift")
                self.data_drift(base_df=base_df,current_df=train_df,report_key_name="data_drift_within_train_dataset")
            if test_df_columns_status:
                logging.info(f"as all column are available in test hence detecting data drift")
                self.data_drift(base_df=base_df,current_df=test_df,report_key_name="data_drift_within_test_dataset")
            
            #write the report
            logging.info(f"writing report in yaml file")
            utils.write_yaml_file(file_path=self.data_validation_config.report_file_path,
            data=self.validation_error)

            data_validation_artifact = artifact_entity.DataValidationArtifact(report_file_path=self.data_validation_config.report_file_path)
            logging.info(f"Data validation artifact:{data_validation_artifact} ")
            return data_validation_artifact
        except Exception as e:
            raise SensorException(e,sys)