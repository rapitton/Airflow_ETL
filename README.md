# Airflow_ETL
Enhance Airflow ETL functionality

1. Add the custom operator files under "dags" or "plugins" directory in your source code repository.
2. During build, it may be copied under the "plugins" directory on GCS or whichever location Airflow fetches the DAG code.
3. Import these modules in the DAG as anyother operator.
      E.g.            
            
            from rp_bigquery_operator import RPBigQueryOperator
            ...
            t1 = RPBigQueryOperator(
                  task_id='example_task',
                  sql=['/example.sql'],
                  use_legacy_sql=False,
                  bigquery_conn_id='bq_conn')
4. Create the required tables and function in the database.
            
