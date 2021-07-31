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

#RP Option to add double quotes and auto archival of the file generated.

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#RP
import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class RPBigQueryToCloudStorageOperator(BaseOperator):
    """
    Transfers a BigQuery table to a Google Cloud Storage bucket.

    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    :param source_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to use as the
        source data. If ``<project>`` is not included, project will be the project
        defined in the connection json. (templated)
    :type source_project_dataset_table: str
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :type destination_cloud_storage_uris: list
    :param compression: Type of compression to use.
    :type compression: str
    :param export_format: File format to export.
    :type export_format: str
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :type field_delimiter: str
    :param print_header: Whether to print a header for a CSV file extract.
    :type print_header: bool
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    """
    #RP added archive_destination_cloud_storage_uris
    template_fields = ('source_project_dataset_table',
                       'destination_cloud_storage_uris', 'archive_destination_cloud_storage_uris', 'labels')

    template_ext = ()

    ui_color = '#e4e6f0'

    #RP added add_double_quote, archive_destination_cloud_storage_uris. archive path automate FUT
    @apply_defaults
    def __init__(self,
                 source_project_dataset_table,
                 destination_cloud_storage_uris,
                 archive_destination_cloud_storage_uris,
                 compression='NONE',
                 export_format='CSV',
                 add_double_quote=False,
                 field_delimiter=',',
                 print_header=True,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 labels=None,
                 xcom_push=False,
                 env=None,
                 output_encoding='utf-8',                 
                 *args,
                 **kwargs):
        super(RPBigQueryToCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.archive_destination_cloud_storage_uris = archive_destination_cloud_storage_uris
        self.compression = compression
        self.add_double_quote = add_double_quote
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        
        #RP - Bash Config
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.sub_process = None

    def execute(self, context):        
        self.execute_Export(context)
        self.execute_File_Prep(context)
        
    def execute_Export(self, context):
        self.log.info('Executing extract of %s into: %s',
                      self.source_project_dataset_table,
                      self.destination_cloud_storage_uris)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_extract(
            source_project_dataset_table=self.source_project_dataset_table,
            destination_cloud_storage_uris=self.destination_cloud_storage_uris,
            compression=self.compression,
            export_format=self.export_format,
            field_delimiter=self.field_delimiter,
            print_header=self.print_header,
            labels=self.labels)
        
    def execute_File_Prep(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
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

        #RP
        export_file=self.destination_cloud_storage_uris[0]
        field_delimiter=self.field_delimiter        

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                #RP Command
                quote_add_cmd=(
                    'gsutil cat '+export_file+' | sed \'s/'+field_delimiter+'/"'+field_delimiter+'"/g\' | sed \'s/^/"/\' | sed \'s/$/"/\' > '+tmp_dir+'/tplus_file.tpf; '
                    'gsutil mv '+tmp_dir+'/tplus_file.tpf '+export_file+'; '
                )
                
                archive_cmd='gsutil cp '+export_file+' '+self.archive_destination_cloud_storage_uris[0]+'; '
                
                if self.add_double_quote:
                    bash_cmd=quote_add_cmd+archive_cmd
                else:
                    bash_cmd=archive_cmd
                
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
