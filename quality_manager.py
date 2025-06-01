from . import script_manager

# Dependencias
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3

class QualityManager(script_manager.ScriptManager):

    def __init__(
            self, 
            b3_session,
            spark_session,
            glue_context,
            domain, 
            subdomain, 
            dataset, 
            project_name
        ):
        super().__init__( 
            b3_session, 
            domain, 
            subdomain, 
            dataset, 
            project_name
        )

        # Resources manipulation variables
        self.spark_session = spark_session
        self.glue_context = glue_context

        #TODO Table where results will be saven
        self._sor_database = 'anything'
        self._sor_tablename = 'anything'

    #======================================================================
    # Verify if attrs exists
    #======================================================================

    def has_quality_alert(self, orchestration:dict):
        if orchestration.get('QualityAlert', None):
            return True
        else:
            return False
    
    def has_quality_rules(self, orchestration:dict, index:int):
        if orchestration.get('QualityAlert')[index].get('Rules',None):
            return True
        else:
            return False

    #======================================================================
    # Return attrs values
    #======================================================================

    def get_quality_alert_level(self, quality_alert:dict, index:int):
        return quality_alert[index].get('Stop',0)

    def get_quality_rules(self, quality_alert:dict, index:int):
        """
        TODO
        """
        return quality_alert[index].get('Rules')  

    #======================================================================
    # Processes data quality evaluation
    #======================================================================

    def should_run_quality(self, orchestration:dict):
        run_conditions = [
            self.has_quality_alert(orchestration),
            self.has_quality_rules(orchestration, 0)
        ]
        
        if all(run_conditions):
            return True
        else:
            return False

    def get_quality_errors_quantity(self, quality_dataframe):
        return quality_dataframe.filter(trim(col("Outcome")) == "Failed").count() 

    def get_dataframe_from_quality_outcome(self, outcome):
        outcomes = SelectFromCollection.apply(
            dfc = outcome,
            key = "ruleOutcomes",
            transformation_ctx="outcomes"
        )
        return outcomes.toDF()

    def evaluate_quality(self, quality_rules, dataframe):
        dyf_gdq = DynamicFrame.fromDF(dataframe, self.glue_context, 'dyf_gdq')
        dyf_quality = EvaluateDataQuality().process_rows(
            frame = dyf_gdq,
            ruleset = quality_rules,
            publishing_options={
                "dataQualityEvaluationContext": "dyf_gdq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True
            },
            additional_options={"performanceTuning.caching": "CACHE_INPUT"}
        )

        return dyf_quality

    def run_data_quality(self, quality_alert:dict, index:int, dataframe):
            # Evaluate Rules
            dyf = self.evaluate_quality(
                quality_rules= self.get_quality_rules(quality_alert, index),
                dataframe= dataframe
            )
            df = self.get_dataframe_from_quality_outcome(dyf)
            
            # Get how many data quality errors were gotten
            errors_num = self.get_quality_errors_quantity(df)

            # Also gets the alert_level value
            alert_level = self.get_quality_alert_level(
                quality_alert, 
                index
            )

            return df, errors_num, alert_level     

    #======================================================================
    # Compiles results and write them into DED SOR
    #======================================================================

    def write_data_quality_results(self, dataframe, database:str, table:str):
        """
        TODO
        """
        pass
    
    def run_pipeline_data_quality(self, orchestration:dict, dataframe):
        if self.should_run_quality(orchestration):
            critical_errors = list()
            
            for index, quality_alert in enumerate(orchestration.get('QualityAlert')):
                if index == 0:
                    df_qr, errors_num, alert_level = self.run_data_quality(
                        quality_alert, 
                        index, 
                        dataframe
                    )
                
                else:
                    _df_qr, errors_num, alert_level = self.run_data_quality(
                        quality_alert, 
                        index, 
                        dataframe
                    )
                    df_qr = df_qr.unionByName(_df_qr)

                critical_errors.append(alert_level == 1 and errors_num > 0)

            self.write_data_quality_results(
                df_qr,
                database=self._sor_database,
                table=self._sor_tablename
            )
            return any(critical_errors)
        
        else:
            return False