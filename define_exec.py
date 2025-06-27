import sys
import boto3
#import awswrangler as wr
#import pandas as pd
from datetime import datetime, timezone
import json
import awswrangler as wr

class SimpleCallExecution:
    def __init__(
        self,
        domain          = 'vct',
        subdomain       = "pasq",
        dataset         = "bap",
        project         = 'tbl_coisa',
        bucket          = 'coisa-workspace-bucket',
        workgroup       = 'athena-workgroup',
        work_database   = 'workspace_db',
        project_database = 'db_workspace'
    ):

        self.__domain__         = domain
        self.__subdomain__      = subdomain
        self.__dataset__        = dataset
        self.__project__        = project
        self.__bucket__         = bucket
        self.__workgroup__      = workgroup
        self.__work_database__  = work_database
        self.__project_database__ = project_database
        self.s3 = boto3.client('s3')
        self.glue = boto3.client('glue')

    def get_file_content(self):
        key_path = f"{self.__domain__}/teams/{self.__subdomain__}/orchestration/{self.__dataset__}/{self.__project__}.json"
        response = self.s3.get_object(Bucket=self.__bucket__, Key=key_path)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    
    def get_src_params(self):
        pass

    def get_src_last_partition(self):
        """
        select * from tble$partitions order by partitions desc limit 1
        """
        pass

    def get_last_partition_update(self, database, table):
        """src e project"""
        response = self.glue.get_partitions(DatabaseName= database, TableName= table)
        partitions = response['Partitions']

        # Get the latest partition by CreationTime
        latest_partition = max(partitions, key=lambda x: x['CreationTime'])
        last_update_time = latest_partition['CreationTime']
        last_update_value = latest_partition['Values'] if 'Values' in latest_partition else None

        return last_update_time, last_update_value

    def has_partition_updated(self, database, table):
        """Check if the partition has been updated since the last etl time."""
        catalog_creation_time, _ = self.get_last_partition_update(database, table)
        etl_control_time =  datetime.now(timezone.utc)
        #etl_control_time, _     = self.get_last_partition_update(self.__project_database__, self.__project__)
        return catalog_creation_time > etl_control_time

    def has_need_for_more_verification(self, __file_content):
        return __file_content.get('Trigger',dict()).get('VerifyBy', 'partition_update') == 'partition_update'

    def get_verification_rules(self, __file_content):
        """
        Get the rules to verificarion.
        """
        return __file_content.get('Trigger',dict()).get('VerifyBy', list())

    def evaluate_verification_rules(self, rules):
        """
        Evaluate the rules to verify.
        """
        rules_outcomes = []
        for rule in rules:
            rule_criteria = rule.get('Rule')
            rule_expectation = rule.get('Outcome')
            outcome = wr.athena.read_sql_query(rule_criteria) #TODO
            rules_outcomes.append( rule_expectation == outcome )
        return all(rules_outcomes)

    def start_job_execution(self, job_name, arguments={}):
        response = exec.glue.start_job_run(
            JobName= job_name,
            Arguments= arguments
        )
        print("Started job run:", response['JobRunId'])

if __name__ == "__main__":
    exec            = SimpleCallExecution()
    FILE_CONTENT    = {'file_content':'content'} #exec.get_file_content()
    
    last_update_time, last_update_value = exec.get_last_partition_update("db_workspace", "tbl_clientes")
    print(f"Last update time: {last_update_time}, value: {'/'.join(last_update_value)}.")

    has_updated = exec.has_partition_updated("db_workspace", "tbl_clientes")
    print(f"Has partition updated: {has_updated}.")
    
    need_more_verification, verify_rules = exec.has_need_for_more_verification(FILE_CONTENT)

    if has_updated and not need_more_verification:
        # First glue job ends here.
        print("Partition has been updated since the last ETL run.")
        response = exec.glue.start_job_run(
            JobName='your-glue-job-name',
            Arguments={
                '--key1': 'value1',
                '--key2': 'value2'
            }
        )
        print("Started job run:", response['JobRunId'])
    else:
        # It is another glue job.
        verification_rules = exec.get_verification_rules(FILE_CONTENT)
        can_start_job = exec.evaluate_verification_rules(verification_rules)
        
        if can_start_job:
            print("All verification rules passed. Starting job execution.")
            exec.start_job_execution('your-glue-job-name', {'--key1': 'value1'})
        else:
            print("Verification rules failed. Job execution not started.")