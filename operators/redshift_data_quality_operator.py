from airflow.models import BaseOperator
from postgres_plugin.hooks.localize_postgres_hook import LocalizePostgresHook
from mysql_plugin.hooks.astro_mysql_hook import AstroMySqlHook
from mssql_plugin.hooks.astro_mssql_hook import AstroMsSqlHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from decimal import Decimal


class RedshiftDataQualityOperator(BaseOperator):
    """
    Data Quality Operator
    Performs basic Data Quality checks on particular table column

    :param target_conn_id:       The destination connection id.
    :type target_conn_id:        string
    :param schema_name:          The destination schema for check.
    :type schema_name:           string
    :param target_table_name:    The destination table for check.
    :type target_table_name:     string
    :param column_name:          The destination column for check.
    :type column_name:           string
    :param check_type:           The check type that is going to be executed. The following types can be used 'is_pk', 'is_unique', 'is_not_null', 'is_not_empty_string'
    :type check_type:            string
    :param source_database:      *(optional)* The source database type.
    :type source_database:       string
    :param source_conn_id:       *(optional)* The source connection id.
    :type source_conn_id:        string
    :param source_table_name:    *(optional)* The destination table for check.
    :type source_table_name:     string
    :param incremental_key:      *(optional)* The incrementing key to filter
                                 the source data with. Currently only
                                 accepts a column with type of timestamp..
    :type incremental_key:       string
    :param start:                *(optional)* The start date to filter
                                 records with based on the incremental_key.
                                 Only required if using the incremental_key
                                 field.
    :type start:                 timestamp (YYYY-MM-DD HH:MM:SS)
    :param end:                  *(optional)* The end date to filter
                                 records with based on the incremental_key.
                                 Only required if using the incremental_key
                                 field.
    :type end:                   timestamp (YYYY-MM-DD HH:MM:SS)
    :param inc_type:             *(optional)* Type of increment fetching , in case of `full` source query check will be based only on end,
                                 otherwise in case of `increment` query will be based on start and end date.
    :type inc_type:              string

    """
    template_fields = ['end', 'start']

    @apply_defaults
    def __init__(
            self,
            target_conn_id,
            schema_name,
            target_table_name,
            column_name,
            check_type,
            source_database=None,
            source_conn_id=None,
            source_table_name=None,
            end=None,
            start=None,
            incremental_key=None,
            inc_type=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_conn_id = target_conn_id
        self.target_table_name = target_table_name
        self.schema_name = schema_name
        self.column_name = column_name
        self.check_type = check_type
        self.source_database = source_database
        self.source_conn_id = source_conn_id
        self.source_table_name = source_table_name
        self.end = end
        self.start = start
        self.incremental_key = incremental_key
        self.inc_type = inc_type
        self.hooks_dict = {'postgres': {'hook': LocalizePostgresHook, 'method': 'get_records_as_dict'},
                           'mysql': {'hook': AstroMySqlHook, 'method': 'get_records'},
                           'mssql': {'hook': AstroMsSqlHook, 'method': 'get_records'}}

    def get_db_hook(self, hook_type, conn_id):
        hook = hook_type(conn_id)
        return hook

    def __get_query_by_check_type(self):
        return {
                'is_pk': f"""select {self.column_name} from {self.schema_name}.{self.target_table_name} group by {self.column_name} having count(*) > 1
                             union all
                             select {self.column_name} from {self.schema_name}.{self.target_table_name} where {self.column_name} is null limit 1""",
                'is_unique': f'select {self.column_name}, count(*) from {self.schema_name}.{self.target_table_name} group by {self.column_name} having count(*) > 1 limit 1',
                'is_not_null': f'select {self.column_name} from {self.schema_name}.{self.target_table_name} where {self.column_name} is null limit 1',
                'is_not_empty_string': f'select {self.column_name} from {self.schema_name}.{self.target_table_name} where {self.column_name} = '' limit 1',
                'is_data_integrity': {'target_sql': f'select sum({self.column_name}) as checksum, count(*) as checkcount from {self.schema_name}.{self.target_table_name}',
                                      'source_sql': f"select sum({self.column_name}) as checksum, count(*) as checkcount from {self.source_table_name} where {self.incremental_key} < \'{self.end}\'"
                                      }
        }

    def __data_integrity(self, query):
        # Returning result from target redshift Database
        self.log.info(f'Target sql:{query["target_sql"]}')
        target_hook = self.get_db_hook(self.hooks_dict['postgres']['hook'], self.target_conn_id)
        get_records_target = self.hooks_dict['postgres']['method']
        target_results = list(getattr(target_hook, get_records_target)(query['target_sql']))
        self.log.info(f'Target query result is: {target_results}')
        # Returning result from source redshift Database
        source_hook = self.get_db_hook(self.hooks_dict[self.source_database]['hook'], self.source_conn_id)
        get_records_source = self.hooks_dict[self.source_database]['method']
        source_query = query['source_sql']
        if self.inc_type == 'increment':
            source_query = source_query + f" AND {self.incremental_key} >= \'{self.start}\'"
        self.log.info(f'Source sql:{source_query}')
        source_results = list(getattr(source_hook, get_records_source)(source_query))
        self.log.info(f'Source query result is: {source_results}')
        if target_results[0]['checksum'] != source_results[0]['checksum'] or target_results[0]['checkcount'] != source_results[0]['checkcount']:
            raise AirflowException('The data integrity check is violated')
        else:
            self.log.info('Data integrity check accomplished succesfully')

    def execute(self, context):
        for check in self.check_type:
            self.log.info('****************************************************************************************')
            self.log.info(f'Executing {check} check on {self.schema_name}.{self.target_table_name}.{self.column_name}')

            query = self.__get_query_by_check_type()[check]
            self.log.info(f'The query is: {query}')

            if check != 'is_data_integrity':
                results = self.get_db_hook(self.hooks_dict['postgres']['hook'], self.target_conn_id).get_first(query)
                self.log.info(f'Query result is: {results}')

                if not results:
                    self.log.info(f'The data quality test of {check} on column {self.schema_name}.{self.target_table_name}.{self.column_name} was completed successfully.')
                else:
                    raise AirflowException(f'The data quality test of {check} on column {self.schema_name}.{self.target_table_name}.{self.column_name} has failed.\nQuery:{query}\nResults:{results}')
            else:
                self.__data_integrity(query)
