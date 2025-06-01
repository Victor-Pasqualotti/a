# Group 1
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Group 2
import unittest
from unittest.mock import patch, MagicMock, Mock

# Group 3
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from SimpleETL.quality_manager import QualityManager

class TestQualityManager(unittest.TestCase):

    def setUp(self):
        # SparkSession para criar DataFrame localmente
        self.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Test") \
            .getOrCreate()

        self.df = self.spark.createDataFrame([
            {"coluna": "valor1"},
            {"coluna": "valor2"}
        ])

        # Instância da classe com mock de glue_context
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_session.client.return_value = MagicMock()
        self.mock_glue_context = Mock()
        self.obj = QualityManager(
            b3_session=self.mock_session,
            spark_session = self.spark,
            glue_context = self.mock_glue_context,            
            domain="analytics",
            subdomain="customer",
            dataset="orders",
            project_name="my_project"
        )

    def test_initialization_with_valid_session(self):
        mock_session = MagicMock(spec=boto3.Session)
        mock_session.client.return_value = MagicMock()
        
        instance = QualityManager(
            b3_session=mock_session,
            spark_session = self.spark,
            glue_context = self.mock_glue_context,            
            domain="analytics",
            subdomain="customer",
            dataset="orders",
            project_name="my_project"
        )

        # Check if clients were created
        mock_session.client.assert_any_call('s3')
        mock_session.client.assert_any_call('sts')
        self.assertEqual(instance._domain, "analytics")
        self.assertEqual(instance._dataset, "orders")

    def test_initialization_with_invalid_session(self):
        with self.assertRaises(Exception) as context:
            QualityManager(
                b3_session="not_a_session",
                spark_session = self.spark,
                glue_context = self.mock_glue_context,
                domain="analytics",
                subdomain="customer",
                dataset="orders",
                project_name="my_project"
            )
        self.assertIn("b3_session expected to receive boto3.Session", str(context.exception))

    #@patch('awsgluedq.transforms.EvaluateDataQuality')
    #@patch('awsglue.dynamicframe.DynamicFrame.fromDF')
    @patch('SimpleETL.quality_manager.QualityManager.evaluate_quality.EvaluateDataQuality')  # Patcha no local onde a função é usada
    @patch('SimpleETL.quality_manager.QualityManager.evaluate_quality.DynamicFrame')         # Patcha no local onde é usado
    def test_evaluate_quality(self, mock_DynamicFrame, mock_EvaluateDataQuality):
        # Setup do mock do DynamicFrame.fromDF
        mock_dynamic_frame = Mock()
        mock_DynamicFrame.fromDF.return_value = mock_dynamic_frame

        # Setup do mock de EvaluateDataQuality
        mock_dq_instance = Mock()
        mock_dq_result = Mock()
        mock_dq_instance.process_rows.return_value = mock_dq_result
        mock_EvaluateDataQuality.return_value = mock_dq_instance

        quality_rules = "MinhaRuleset"

        resultado = self.obj.evaluate_quality(quality_rules, self.df)

        # Asserts
        mock_DynamicFrame.fromDF.assert_called_once_with(self.df, self.mock_glue_context, 'dyf_gdq')
        mock_EvaluateDataQuality.assert_called_once()
        mock_dq_instance.process_rows.assert_called_once_with(
            frame=mock_dynamic_frame,
            ruleset=quality_rules,
            publishing_options={
                "dataQualityEvaluationContext": "dyf_gdq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True
            },
            additional_options={"performanceTuning.caching": "CACHE_INPUT"}
        )
        self.assertEqual(resultado, mock_dq_result)


if __name__ == '__main__':
    unittest.main()