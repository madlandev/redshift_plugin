from airflow.models import BaseOperator
from postgres_plugin.hooks.localize_postgres_hook import LocalizePostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class RedshiftDataQualityOperator(BaseOperator):
    """
    Data Quality Operator
    Performs basic Data Quality checks on particular table column

    :param postgres_conn_id:     The destination connection id.
    :type postgres_conn_id:      string
    :param schema_name:          The destination schema for check.
    :type schema_name:           string
    :param table_name:           The destination table for check.
    :type table_name:            string
    :param column_name:          The destination column for check.
    :type column_name:           string
    :param check_type:           The check type that is going to be executed. The following types can be used 'is_pk', 'is_unique', 'is_not_null', 'is_not_empty_string'
    :type check_type:            string
    """

    @apply_defaults
    def __init__(
            self,
            postgres_conn_id,
            schema_name,
            table_name,
            column_name,
            check_type,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema_name = schema_name
        self.column_name = column_name
        self.check_type = check_type

    def get_db_hook(self):
        hook = LocalizePostgresHook(postgres_conn_id=self.postgres_conn_id)
        return hook

    def __get_query_by_check_type(self):
        return {
                'is_pk': f"""select {self.column_name} from {self.schema_name}.{self.table_name} group by {self.column_name} having count(*) > 1
                             union all
                             select {self.column_name} from {self.schema_name}.{self.table_name} where {self.column_name} is null limit 1""",
                'is_unique': f'select {self.column_name}, count(*) from {self.schema_name}.{self.table_name} group by {self.column_name} having count(*) > 1 limit 1',
                'is_not_null': f'select {self.column_name} from {self.schema_name}.{self.table_name} where {self.column_name} is null limit 1',
                'is_not_empty_string': f'select {self.column_name} from {self.schema_name}.{self.table_name} where {self.column_name} = '' limit 1'
        }.get(self.check_type, 'There''s no such check type')

    def execute(self, context):
        self.log.info(f'Executing {self.check_type} check on {self.schema_name}.{self.table_name}.{self.column_name}')

        query = self.__get_query_by_check_type()
        self.log.info(f'The query is: {query}')

        results = self.get_db_hook().get_first(query)
        self.log.info(f'Query result is: {results}')

        if not results:
            self.log.info(f'The data quality test of {self.check_type} on column {self.schema_name}.{self.table_name}.{self.column_name} was completed successfully.')
        else:
            raise AirflowException(f'The data quality test of {self.check_type} on column {self.schema_name}.{self.table_name}.{self.column_name} has failed.\nQuery:{query}\nResults:{results}')
