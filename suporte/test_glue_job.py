# tests/test_glue_job.py

import sys
import unittest
import json
from unittest.mock import MagicMock, patch
from io import BytesIO

# Mocks dos módulos do AWS Glue
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.transforms'] = MagicMock()
sys.modules['awsglue.dynamicframe'] = MagicMock()
sys.modules['awsgluedq'] = MagicMock()
sys.modules['awsgluedq.transforms'] = MagicMock()

from glue_job import GlueDataProcessor

class TestGlueDataProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_glue_context = MagicMock()
        self.mock_session = MagicMock()
        self.mock_s3_client = MagicMock()
        self.mock_session.client.return_value = self.mock_s3_client

        self.processor = GlueDataProcessor(self.mock_glue_context, self.mock_session)

    def test_carregar_configuracao_do_s3(self):
        # Arrange
        s3_uri = "s3://meu-bucket/configs/config.json"
        config_dict = {"database": "meu_db", "table": "minha_tabela"}
        config_bytes = json.dumps(config_dict).encode("utf-8")
        self.mock_s3_client.get_object.return_value = {
            "Body": BytesIO(config_bytes)
        }

        # Act
        result = self.processor.carregar_configuracao_do_s3(s3_uri)

        # Assert
        self.assertEqual(result, config_dict)
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="meu-bucket", Key="configs/config.json"
        )

    def test_carregar_dynamic_frame(self):
        mock_dynamic_frame = MagicMock()
        self.mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_dynamic_frame

        result = self.processor.carregar_dynamic_frame("meu_db", "minha_tabela")

        self.assertEqual(result, mock_dynamic_frame)
        self.mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="meu_db", table_name="minha_tabela"
        )

    def test_converter_para_dataframe(self):
        mock_df = MagicMock()
        mock_dynamic_frame = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df

        result = self.processor.converter_para_dataframe(mock_dynamic_frame)

        self.assertEqual(result, mock_df)
        mock_dynamic_frame.toDF.assert_called_once()

    @patch("glue_job.EvaluateDataQuality")
    @patch("glue_job.DynamicFrame")
    def test_avaliar_data_quality(self, mock_dynamic_frame_class, mock_evaluate_dq_class):
        mock_df = MagicMock()
        mock_dyf = MagicMock()
        mock_dyf_result = MagicMock()

        mock_dynamic_frame_class.fromDF.return_value = mock_dyf

        mock_evaluate_dq_instance = MagicMock()
        mock_evaluate_dq_instance.process.return_value = mock_dyf_result
        mock_evaluate_dq_class.return_value = mock_evaluate_dq_instance

        result = self.processor.avaliar_data_quality(mock_df, "ruleset_exemplo")

        mock_dynamic_frame_class.fromDF.assert_called_once_with(mock_df, self.mock_glue_context, "avaliacao_dq")
        mock_evaluate_dq_instance.process.assert_called_once_with(mock_dyf, ruleset="ruleset_exemplo")
        self.assertEqual(result, mock_dyf_result)

if __name__ == '__main__':
    unittest.main()
