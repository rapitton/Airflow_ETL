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

#RP Adds the file name as an additional column. Makes entry in audit table with the records loaded.
#RP ToDo - Removal of the source file once the process completes.

import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#RP Adding lib for bash command
import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars

class RPGoogleCloudStorageToBigQueryOperator(BaseOperator):
    """
    Loads files from Google cloud storage into BigQuery.
    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google cloud storage object name. The object in
    Google cloud storage must be a JSON file with the schema fields in it.
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCloudStorageToBigQueryOperator`
    :param bucket: The bucket to load from. (templated)
    :type bucket: str
    :param source_objects: List of Google cloud storage URIs to load from. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :type source_objects: list[str]
    :param destination_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to load data into.
        If ``<project>`` is not included, project will be the project defined in
        the connection json. (templated)
    :type destination_project_dataset_table: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        Should not be set when source_format is 'DATASTORE_BACKUP'.
        Parameter must be defined if 'schema_object' is null and autodetect is False.
    :type schema_fields: list
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
        Parameter must be defined if 'schema_fields' is null and autodetect is False.
    :type schema_object: str
    :param source_format: File format to export.
    :type source_format: str
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :type compression: str
    :param create_disposition: The create disposition if the table doesn't exist.
    :type create_disposition: str
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :type skip_leading_rows: int
    :param write_disposition: The write disposition if the table already exists.
    :type write_disposition: str
    :param field_delimiter: The delimiter to use when loading from a CSV.
    :type field_delimiter: str
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :type max_bad_records: int
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :type quote_character: str
    :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    :type ignore_unknown_values: bool
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :type allow_quoted_newlines: bool
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
    :param max_id_key: If set, the name of a column in the BigQuery table
        that's to be loaded. This will be used to select the MAX value from
        BigQuery after the load occurs. The results will be returned by the
        execute() command, which in turn gets stored in XCom for future
        operators to use. This can be helpful with incremental loads--during
        future executions, you can pick up from the max ID.
    :type max_id_key: str
    :param bigquery_conn_id: Reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: list
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. Default Value is False.
    :type external_table: bool
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this load be stored sorted
        by one or more columns. This is only available in conjunction with
        time_partitioning. The order of columns given determines the sort order.
        Not applicable for external tables.
    :type cluster_fields: list[str]
    :param autodetect: [Optional] Indicates if we should automatically infer the
        options and schema for CSV and JSON sources. (Default: ``True``).
        Parameter must be setted to True if 'schema_fields' and 'schema_object' are undefined.
        It is suggested to set to True if table are create outside of Airflow.
    :type autodetect: bool
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::
            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """
    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table', 'env')

    template_ext = ('.sql',)
    
    #RP FUT change colour
    ui_color = '#f0eee4'


    @apply_defaults
    def __init__(self,
                 bucket,
                 source_objects,
                 destination_project_dataset_table,
                 schema_fields=None,
                 schema_object=None,
                 source_format='CSV',
                 compression='NONE',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 ignore_unknown_values=False,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 max_id_key=None,
                 bigquery_conn_id='bigquery_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 schema_update_options=(),
                 src_fmt_configs=None,
                 external_table=False,
                 time_partitioning=None,
                 cluster_fields=None,
                 autodetect=True,
                 encryption_configuration=None,
                 xcom_push=False,
                 env=None,
                 output_encoding='utf-8',                 
                 *args, **kwargs):

        super(RPGoogleCloudStorageToBigQueryOperator, self).__init__(*args, **kwargs)

        # GCS config
        if src_fmt_configs is None:
            src_fmt_configs = {}
        if time_partitioning is None:
            time_partitioning = {}
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table

        self.max_id_key = max_id_key
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.autodetect = autodetect
        self.encryption_configuration = encryption_configuration
        
        #RP - Bash Config
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.sub_process = None
        
    def execute(self, context):        
        self.execute_File_Prep(context)
        self.execute_Load(context)
        
    def execute_Load(self, context):
        #RP Set use_legacy_sql as False to avoid SQL execution error
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               use_legacy_sql=False,
                               delegate_to=self.delegate_to)

        if not self.schema_fields:
            if self.schema_object and self.source_format != 'DATASTORE_BACKUP':
                gcs_hook = GoogleCloudStorageHook(
                    google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                    delegate_to=self.delegate_to)
                schema_fields = json.loads(gcs_hook.download(
                    self.bucket,
                    self.schema_object).decode("utf-8"))
            elif self.schema_object is None and self.autodetect is False:
                raise ValueError('At least one of `schema_fields`, `schema_object`, '
                                 'or `autodetect` must be passed.')
            else:
                schema_fields = None

        else:
            schema_fields = self.schema_fields

        source_uris = ['gs://{}/{}'.format(self.bucket, source_object)
                       for source_object in self.source_objects]
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        if self.external_table:
            cursor.create_external_table(
                external_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                compression=self.compression,
                skip_leading_rows=self.skip_leading_rows,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                src_fmt_configs=self.src_fmt_configs,
                encryption_configuration=self.encryption_configuration
            )
        else:
            cursor.run_load(
                destination_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                autodetect=self.autodetect,
                create_disposition=self.create_disposition,
                skip_leading_rows=self.skip_leading_rows,
                write_disposition=self.write_disposition,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                schema_update_options=self.schema_update_options,
                src_fmt_configs=self.src_fmt_configs,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration)
            
        if cursor.use_legacy_sql:
            escaped_table_name = '[{}]'.format(self.destination_project_dataset_table)
        else:
            escaped_table_name = '`{}`'.format(self.destination_project_dataset_table)

        if self.max_id_key:
            cursor.run_query(sql='INSERT INTO ABC.FILE_LOAD_LOG(FILE_CNT) SELECT COUNT(1) FROM '+escaped_table_name+' ; ')
        message = "Completed"
        print(message)
        return message
    
    
    def execute_File_Prep(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        #RP Will consider only the first source object even if multiple passed as array. Variables added
        source_file=self.source_objects[0]
        source_load_file='gs://'+self.bucket+'/'+source_file.replace("*","").replace("..",".")+"_plus_file"
        skip_leading_rows=self.skip_leading_rows
        field_delimiter=self.field_delimiter
        
        self.log.info("Tmp dir root location: \n %s", gettempdir())

        # Prepare env for child process.
        env = self.env
        if env is None:
            env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug('Exporting the following env vars:\n%s',
                       '\n'.join(["{}={}".format(k, v)
                                  for k, v in airflow_context_vars.items()]))
        env.update(airflow_context_vars)        

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:
                #RP Assign tmp
                v_tmp_dir = tmp_dir
                
                #RP Mainly handles skip_leading_rows=1. Need to handle for skip_leading_rows=1+
                if skip_leading_rows > 0:
                    create_header='gsutil cat $v_FILE_NM | head -1 | sed \'s/\\r$//\' | sed \'s/$/'+field_delimiter+'FILE_NAME/\' > '+v_tmp_dir+'/tplus_file_header.htpf; '
                    exclude_header=' | tail -n +2 '
                    combine_files='cat '+v_tmp_dir+'/tplus_file_header.htpf '+v_tmp_dir+'/tplus_file_*.tpf > '+v_tmp_dir+'/tplus_final_file.tpf ;'
                else:
                    create_header=''
                    exclude_header=''
                    combine_files='cat '+v_tmp_dir+'/tplus_file_*.tpf > '+v_tmp_dir+'/tplus_final_file.tpf ;'

                bash_cmd = (
                    'VAR=$(gsutil ls gs://'+self.bucket+'/'+source_file+' | wc -l); if [[ $VAR -eq 0 ]]; then echo "Source files are not present. Exiting."; exit 1 ; fi ; '
                    'for v_FILE_NM in $(gsutil ls gs://'+self.bucket+'/'+source_file+'); do v_FILE_ID=$(echo $v_FILE_NM | xargs -n 1 basename); '
                    +create_header+
                    'gsutil cat $v_FILE_NM | sed \'s/\\r$//\' | awk -v file_nm=$v_FILE_ID \'{print $0"'+field_delimiter+'"file_nm}\' '+exclude_header+' > '+v_tmp_dir+'/tplus_file_$v_FILE_ID.tpf; '
                    'done ; '+combine_files+
                    'gsutil mv '+v_tmp_dir+'/tplus_final_file.tpf '+source_load_file+'; '              
                )                     
                self.log.info("Bash Command: \n %s", bash_cmd)
                
                self.lineage_data = bash_cmd
                
                f.write(bytes(bash_cmd, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command: %s", bash_cmd)
                self.sub_process = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=env,
                    preexec_fn=pre_exec)

                self.log.info("Output:")
                line = ''
                for line in iter(self.sub_process.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)
                self.sub_process.wait()
                self.log.info(
                    "Command exited with return code %s",
                    self.sub_process.returncode
                )

                if self.sub_process.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line


    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)    
