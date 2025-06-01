# Group 2
import unittest
from unittest.mock import MagicMock, patch

# Group 3
import json
from io import BytesIO
import boto3

# Group 1
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Mocks dos módulos do AWS Glue
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.dynamicframe'] = MagicMock()
sys.modules['awsgluedq'] = MagicMock()
sys.modules['awsgluedq.transforms'] = MagicMock()

from SimpleETL.quality_manager import QualityManager

class TestQualityManager(unittest.TestCase):

    def setUp(self):
        # Mock para glue e spark context
        self.mock_glue_context = MagicMock()
        self.spark = MagicMock()
        # Mock para boto3 session e clients
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_client = MagicMock()
        self.mock_session.client.return_value = self.mock_client

        # Uma reprensentacao de uma instancia da nossa classe.
        self.quality_manager = QualityManager(
            b3_session=self.mock_session,
            spark_session = self.spark,
            glue_context = self.mock_glue_context,            
            domain="analytics",
            subdomain="customer",
            dataset="orders",
            project_name="my_project"
        )

    def test_initialization_with_valid_session(self):
        
        instance = QualityManager(
            b3_session=self.mock_session,
            spark_session = self.spark,
            glue_context = self.mock_glue_context,            
            domain="analytics",
            subdomain="customer",
            dataset="orders",
            project_name="my_project"
        )

        # Check if clients were created
        self.mock_session.client.assert_any_call('s3')
        self.mock_session.client.assert_any_call('sts')
        self.assertEqual(instance._domain, "analytics")
        self.assertEqual(instance._subdomain, "customer")
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

    @patch("SimpleETL.quality_manager.EvaluateDataQuality")
    @patch("SimpleETL.quality_manager.DynamicFrame")
    def test_evaluate_quality(self, mock_dynamic_frame_class, mock_evaluate_dq_class):
        # Arrange
        mock_df = MagicMock()
        mock_dyf = MagicMock()
        mock_result = MagicMock()

        # Simula retorno do DynamicFrame.fromDF
        mock_dynamic_frame_class.fromDF.return_value = mock_dyf

        # Simula a instância e comportamento de EvaluateDataQuality
        mock_dq_instance = MagicMock()
        mock_dq_instance.process_rows.return_value = mock_result
        mock_evaluate_dq_class.return_value = mock_dq_instance

        # Chamada da função a ser testada
        quality_rules = "ruleset_exemplo"
        result = self.quality_manager.evaluate_quality(quality_rules, mock_df)

        # Assert
        mock_dynamic_frame_class.fromDF.assert_called_once_with(mock_df, self.mock_glue_context, "dyf_gdq")
        mock_dq_instance.process_rows.assert_called_once_with(
            frame=mock_dyf,
            ruleset=quality_rules,
            publishing_options={
                "dataQualityEvaluationContext": "dyf_gdq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True
            },
            additional_options={"performanceTuning.caching": "CACHE_INPUT"}
        )
        self.assertEqual(result, mock_result)


if __name__ == '__main__':
    unittest.main()