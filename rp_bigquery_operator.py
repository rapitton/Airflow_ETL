# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module contains Google BigQuery operators.
"""

#RP Combines and inserts audit queries between SQLs (separated by ";")

import json
from typing import Iterable

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook, _parse_gcs_url
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults

BIGQUERY_JOB_DETAILS_LINK_FMT = 'https://console.cloud.google.com/bigquery?j={job_id}'



class BigQueryConsoleLink(BaseOperatorLink):
    """
    Helper class for constructing BigQuery link.
    """
    name = 'BigQuery Console'


    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_id = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ''



class BigQueryConsoleIndexableLink(BaseOperatorLink):
    """
    Helper class for constructing BigQuery link.
    """

    def __init__(self, index):
        super(BigQueryConsoleIndexableLink, self).__init__()
        self.index = index

    @property
    def name(self):  # type: () -> str
        return 'BigQuery Console #{index}'.format(index=self.index + 1)


    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_ids = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)

# pylint: disable=too-many-instance-attributes
class RPBigQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database
    :param bql: (Deprecated. Use `sql` parameter instead) the sql code to be
        executed (templated)
    :type bql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param destination_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
        of the query. (templated)
    :type destination_dataset_table: str
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: str
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: str
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: bool
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :type flatten_results: bool
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: bool
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: int
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :type maximum_bytes_billed: float
    :param api_resource_configs: a dictionary that contain params
        'configuration' applied for Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
        for example, {'query': {'useQueryCache': False}}. You could use it
        if you need to provide some params that are not supported by BigQueryOperator
        like args.
    :type api_resource_configs: dict
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: Optional[Union[list, tuple, set]]
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }].
    :type query_params: list
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :type priority: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this query be stored sorted
        by one or more columns. This is only available in conjunction with
        time_partitioning. The order of columns given determines the sort order.
    :type cluster_fields: list[str]
    :param location: The geographic location of the job. Required except for
        US and EU. See details at
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::
            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """

    template_fields = ('bql', 'sql', 'destination_dataset_table', 'labels')

    template_ext = ('.sql', )

    ui_color = '#e4f0e8'


    @property
    def operator_extra_links(self):
        """
        Return operator extra links
        """
        if isinstance(self.sql, str):
            return (
                BigQueryConsoleLink(),
            )
        return (
            BigQueryConsoleIndexableLink(i) for i, _ in enumerate(self.sql)

        )

    # pylint: disable=too-many-arguments, too-many-locals
    @apply_defaults
    def __init__(self,
                 bql=None,
                 sql=None,
                 destination_dataset_table=None,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 flatten_results=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 udf_config=None,
                 use_legacy_sql=True,
                 maximum_billing_tier=None,
                 maximum_bytes_billed=None,
                 create_disposition='CREATE_IF_NEEDED',
                 schema_update_options=(),
                 query_params=None,
                 labels=None,
                 priority='INTERACTIVE',
                 time_partitioning=None,
                 api_resource_configs=None,
                 cluster_fields=None,
                 location=None,
                 encryption_configuration=None,
                 *args,
                 **kwargs):
        super(RPBigQueryOperator, self).__init__(*args, **kwargs)
        self.bql = bql
        self.sql = sql if sql else bql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.bq_cursor = None
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.api_resource_configs = api_resource_configs
        self.cluster_fields = cluster_fields
        self.location = location
        self.encryption_configuration = encryption_configuration

        # TODO remove `bql` in Airflow 2.0
        if self.bql:
            import warnings
            warnings.warn('Deprecated parameter `bql` used in Task id: {}. '
                          'Use `sql` parameter instead to pass the sql to be '
                          'executed. `bql` parameter is deprecated and '
                          'will be removed in a future version of '
                          'Airflow.'.format(self.task_id),
                          category=DeprecationWarning)

        if self.sql is None:
            raise TypeError('{} missing 1 required positional '
                            'argument: `sql`'.format(self.task_id))

    def execute(self, context):
        #RP
        v_dag=context["dag"]
        v_task=context["task"]
        v_dag_run=context["dag_run"]
        sql_init=f"DECLARE v_QUERY_ID STRING; \nDECLARE v_QUERY_ID_COMP DEFAULT (SELECT ABC.FN_JOB_RUN_LOG('{ v_dag }','{ v_task }')); \nASSERT NOT (SELECT 'Q99' IN UNNEST(v_QUERY_ID_COMP)) AS 'Task already completed for the day'; \nINSERT INTO ABC.JOB_RUN_LOG (DAG, TASK, QUERY_ID, QUERY_TYPE, DML_CNT, DAG_RUN, ACTIVE_FLG, INSERT_DTTIME) \nVALUES ('{ v_dag }', '{ v_task }', 'Q0', 'START',  -1, '{ v_dag_run }', 'Y', CURRENT_DATETIME()); \n\n"
        sql_end=f"\nINSERT INTO ABC.JOB_RUN_LOG (DAG, TASK, QUERY_ID, QUERY_TYPE, DML_CNT, DAG_RUN, ACTIVE_FLG, INSERT_DTTIME) \nVALUES ('{ v_dag }', '{ v_task }', 'Q99', 'COMPLETE',  -10, '{ v_dag_run }', 'Y', CURRENT_DATETIME());"
        sql_pre=f"IF NOT (SELECT v_QUERY_ID IN UNNEST(v_QUERY_ID_COMP)) THEN \n\n"
        sql_post=f"\n\nINSERT INTO ABC.JOB_RUN_LOG (DAG, TASK, QUERY_ID, QUERY_TYPE, DML_CNT, DAG_RUN, ACTIVE_FLG, INSERT_DTTIME) \nVALUES ('{ v_dag }', '{ v_task }', v_QUERY_ID, 'SQL',  @@row_count, '{ v_dag_run }', 'Y', CURRENT_DATETIME()); \nEND IF; \n\n"   
        
        if self.bq_cursor is None:
            self.log.info('Executing: %s', self.sql)
            hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to,
                location=self.location,
            )
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
        
        if isinstance(self.sql, Iterable):
            final_sql = sql_init
            for i in range(0, len(self.sql)):
                set_sql = self.sql[i].split(';')
                for j in range(0, len(set_sql)):
                    sing_sql = set_sql[j].strip()
                    if sing_sql != '':
                        final_sql = final_sql + f"SET v_QUERY_ID='Q{i+1}.{j+1}'; \n" + sql_pre + sing_sql + ' ; \n' + sql_post
            final_sql  =final_sql + sql_end
            self.log.info('Executing Final: %s', final_sql)
            
            job_id = self.bq_cursor.run_query(
                sql=final_sql,
                destination_dataset_table=self.destination_dataset_table,
                write_disposition=self.write_disposition,
                allow_large_results=self.allow_large_results,
                flatten_results=self.flatten_results,
                udf_config=self.udf_config,
                maximum_billing_tier=self.maximum_billing_tier,
                maximum_bytes_billed=self.maximum_bytes_billed,
                create_disposition=self.create_disposition,
                query_params=self.query_params,
                labels=self.labels,
                schema_update_options=self.schema_update_options,
                priority=self.priority,
                time_partitioning=self.time_partitioning,
                api_resource_configs=self.api_resource_configs,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration
            )
        else:
            raise AirflowException(
                "argument 'sql' of type {} is neither a string nor an iterable".format(type(str)))
        context['task_instance'].xcom_push(key='job_id', value=job_id)


    def on_kill(self):
        super(RPBigQueryOperator, self).on_kill()
        if self.bq_cursor is not None:
            self.log.info('Cancelling running query')
            self.bq_cursor.cancel_query()
