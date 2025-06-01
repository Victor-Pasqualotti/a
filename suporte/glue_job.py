# glue_job.py

import json
import boto3
from urllib.parse import urlparse

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

class GlueDataProcessor:
    def __init__(self, glue_context: GlueContext, session: boto3.Session):
        self.glue_context = glue_context
        self.session = session
        self.s3 = session.client('s3')

    def carregar_configuracao_do_s3(self, s3_uri: str) -> dict:
        """Lê um JSON do S3 e retorna como dict"""
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        response = self.s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode("utf-8")
        return json.loads(content)

    def carregar_dynamic_frame(self, database: str, table: str) -> DynamicFrame:
        return self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table
        )

    def converter_para_dataframe(self, dynamic_frame: DynamicFrame):
        return dynamic_frame.toDF()

    def avaliar_data_quality(self, dataframe, ruleset: str):
        dynamic_frame = DynamicFrame.fromDF(dataframe, self.glue_context, "avaliacao_dq")
        dq_transform = EvaluateDataQuality()
        return dq_transform.process(dynamic_frame, ruleset=ruleset)
