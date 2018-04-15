import json
import random
import string
import logging
import re

from airflow.utils.db import provide_session
from airflow.models import Connection
from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook


class S3ToRedshiftOperator(BaseOperator):
    """
    S3 To Redshift Operator
    :param redshift_conn_id:        The destination redshift connection id.
    :type redshift_conn_id:         string
    :param redshift_schema:         The destination redshift schema.
    :type redshift_schema:          string
    :param table:                   The destination redshift table.
    :type table:                    string
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param copy_params:             The parameters to be included when issuing
                                    the copy statement in Redshift.
    :type copy_params:              list
    :param origin_schema:           The s3 key for the incoming data schema.
                                    Expects a JSON file with an array of
                                    dictionaries specifying name and type.
                                    (e.g. {"name": "_id", "type": "int4"})
    :type origin_schema:            array of dictionaries
    :param schema_location:         The location of the origin schema. This
                                    can be set to 'S3' or 'Local'.
                                    If 'S3', it will expect a valid S3 Key. If
                                    'Local', it will expect a dictionary that
                                    is defined in the operator itself. By
                                    default the location is set to 's3'.
    :type schema_location:          string
    :param origin_datatype:         The incoming database type from which to		
                                    convert the origin schema. Required when		
                                    specifiying the origin_schema. Current		
                                    possible values include "mysql".		
    :type origin_datatype:          string
    :param load_type:               The method of loading into Redshift that
                                    should occur. Options:
                                        - "append"
                                        - "rebuild"
                                        - "truncate"
                                        - "upsert"
                                    Defaults to "append."
    :type load_type:                string
    :param primary_key:             *(optional)* The primary key for the
                                    destination table. Not enforced by redshift
                                    and only required if using a load_type of
                                    "upsert".
    :type primary_key:              string
    :param incremental_key:         *(optional)* The incremental key to compare
                                    new data against the destination table
                                    with. Only required if using a load_type of
                                    "upsert".
    :type incremental_key:          string
    :param foreign_key:             *(optional)* This specifies any foreign_keys
                                    in the table and which corresponding table
                                    and key they reference. This may be either
                                    a dictionary or list of dictionaries (for
                                    multiple foreign keys). The fields that are
                                    required in each dictionary are:
                                        - column_name
                                        - reftable
                                        - ref_column
    :type foreign_key:              dictionary
    :param distkey:                 *(optional)* The distribution key for the
                                    table. Only one key may be specified.
    :type distkey:                  string
    :param sortkey:                 *(optional)* The sort keys for the table.
                                    If more than one key is specified, set this
                                    as a list.
    :type sortkey:                  string
    :param sort_type:               *(optional)* The style of distribution
                                    to sort the table. Possible values include:
                                        - compound
                                        - interleaved
                                    Defaults to "compound".
    :type sort_type:                string
    """

    template_fields = ('s3_key',
                       'origin_schema')

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id,
                 redshift_schema,
                 table,
                 copy_params=[],
                 origin_schema=None,
                 schema_location='s3',
                 origin_datatype=None,
                 load_type='append',
                 primary_key=None,
                 incremental_key=None,
                 foreign_key={},
                 distkey=None,
                 sortkey='',
                 sort_type='COMPOUND',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema.lower()
        self.table = table.lower()
        self.copy_params = copy_params
        self.origin_schema = origin_schema
        self.schema_location = schema_location
        self.origin_datatype = origin_datatype
        self.load_type = load_type
        self.primary_key = primary_key
        self.incremental_key = incremental_key
        self.foreign_key = foreign_key
        self.distkey = distkey
        self.sortkey = sortkey
        self.sort_type = sort_type

        if self.load_type.lower() not in ("append", "rebuild", "truncate", "upsert"):
            raise Exception('Please choose "append", "rebuild", or "upsert".')

        if self.schema_location.lower() not in ('s3', 'local'):
            raise Exception('Valid Schema Locations are "s3" or "local".')

        if not (isinstance(self.sortkey, str) or isinstance(self.sortkey, list)):
            raise Exception('Sort Keys must be specified as either a string or list.')

        if not (isinstance(self.foreign_key, dict) or isinstance(self.foreign_key, list)):
            raise Exception('Foreign Keys must be specified as either a dictionary or a list of dictionaries.')

        if self.distkey and ((',' in self.distkey) or not isinstance(self.distkey, str)):
            raise Exception('Only one distribution key may be specified.')

        if self.sort_type.lower() not in ('compound', 'interleaved'):
            raise Exception('Please choose "compound" or "interleaved" for sort type.')

    def execute(self, context):
        # Append a random string to the end of the staging table to ensure
        # no conflicts if multiple processes running concurrently.
        letters = string.ascii_lowercase
        random_string = ''.join(random.choice(letters) for _ in range(7))
        self.temp_suffix = '_tmp_{0}'.format(random_string)

        if self.origin_schema:
            schema = self.read_and_format()

        pg_hook = PostgresHook(self.redshift_conn_id)

        self.create_if_not_exists(schema, pg_hook)
        self.reconcile_schemas(schema, pg_hook)
        self.copy_data(pg_hook, schema)

    def read_and_format(self):
        if self.schema_location.lower() == 's3':
                hook = S3Hook(self.s3_conn_id)
                schema = (hook.get_key(self.origin_schema,
                                       bucket_name=
                                       '{0}'.format(self.s3_bucket))
                          .get_contents_as_string(encoding='utf-8'))
                schema = json.loads(schema.replace("'", '"'))
        else:
            schema = self.origin_schema
        if self.origin_datatype:		
            if self.origin_datatype.lower() == 'mysql':		
                for i in schema:		
                    i['type'] = self.mysql_to_redshift_type_convert(i['type'])
        return schema

    def reconcile_schemas(self, schema, pg_hook):
        pg_query = \
            """
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = '{0}' AND table_name = '{1}';
            """.format(self.redshift_schema, self.table)

        pg_schema = dict(pg_hook.get_records(pg_query))
        incoming_keys = [column['name'] for column in schema]
        diff = list(set(incoming_keys) - set(pg_schema.keys()))
        print(diff)
        # Check length of column differential to see if any new columns exist
        if len(diff):
            for i in diff:
                for e in schema:
                    if i == e['name']:
                        alter_query = \
                         """
                         ALTER TABLE "{0}"."{1}"
                         ADD COLUMN "{2}" {3}
                         """.format(self.redshift_schema,
                                    self.table,
                                    e['name'],
                                    e['type'])
                        pg_hook.run(alter_query)
                        logging.info('The new columns were:' + str(diff))
        else:
            logging.info('There were no new columns.')

    def copy_data(self, pg_hook, schema=None):
        @provide_session
        def get_conn(conn_id, session=None):
            conn = (
                session.query(Connection)
                .filter(Connection.conn_id == conn_id)
                .first())
            return conn

        def getS3Conn():
            creds = ""
            s3_conn = get_conn(self.s3_conn_id)
            aws_key = s3_conn.extra_dejson.get('aws_access_key_id', None)
            aws_secret = s3_conn.extra_dejson.get('aws_secret_access_key', None)
            # support for cross account resource access
            aws_role_arn = s3_conn.extra_dejson.get('role_arn', None)

            if aws_key and aws_secret:
                creds = ("aws_access_key_id={0};aws_secret_access_key={1}"
                    .format(aws_key, aws_secret))
            elif aws_role_arn:
                creds = ("aws_iam_role={0}"
                    .format(aws_role_arn))

            return creds

        # Delete records from the destination table where the incremental_key
        # is greater than or equal to the incremental_key of the source table
        # and the primary key is the same.
        # (e.g. Source: {"id": 1, "updated_at": "2017-01-02 00:00:00"};
        #       Destination: {"id": 1, "updated_at": "2017-01-01 00:00:00"})

        delete_sql = \
            '''
            DELETE FROM "{rs_schema}"."{rs_table}"
            USING "{rs_schema}"."{rs_table}{rs_suffix}"
            WHERE "{rs_schema}"."{rs_table}"."{rs_pk}" =
            "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_pk}"
            AND "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_ik}" >=
            "{rs_schema}"."{rs_table}"."{rs_ik}"
            '''.format(rs_schema=self.redshift_schema,
                       rs_table=self.table,
                       rs_pk=self.primary_key,
                       rs_suffix=self.temp_suffix,
                       rs_ik=self.incremental_key)

        # Delete records from the source table where the incremental_key
        # is greater than or equal to the incremental_key of the destination
        # table and the primary key is the same. This is done in the edge case
        # where data is pulled BEFORE it is altered in the source table but
        # AFTER a workflow containing an updated version of the record runs.
        # In this case, not running this will cause the older record to be
        # added as a duplicate to the newer record.
        # (e.g. Source: {"id": 1, "updated_at": "2017-01-01 00:00:00"};
        #       Destination: {"id": 1, "updated_at": "2017-01-02 00:00:00"})

        delete_confirm_sql = \
            '''
            DELETE FROM "{rs_schema}"."{rs_table}{rs_suffix}"
            USING "{rs_schema}"."{rs_table}"
            WHERE "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_pk}" =
            "{rs_schema}"."{rs_table}"."{rs_pk}"
            AND "{rs_schema}"."{rs_table}"."{rs_ik}" >=
            "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_ik}"
            '''.format(rs_schema=self.redshift_schema,
                       rs_table=self.table,
                       rs_pk=self.primary_key,
                       rs_suffix=self.temp_suffix,
                       rs_ik=self.incremental_key)

        append_sql = \
            '''
            ALTER TABLE "{0}"."{1}"
            APPEND FROM "{0}"."{1}{2}"
            FILLTARGET
            '''.format(self.redshift_schema, self.table, self.temp_suffix)

        drop_sql = \
            '''
            DROP TABLE IF EXISTS "{0}"."{1}"
            '''.format(self.redshift_schema, self.table)

        drop_temp_sql = \
            '''
            DROP TABLE IF EXISTS "{0}"."{1}{2}"
            '''.format(self.redshift_schema, self.table, self.temp_suffix)

        truncate_sql = \
            '''
            TRUNCATE TABLE "{0}"."{1}"
            '''.format(self.redshift_schema, self.table)

        params = '\n'.join(self.copy_params)

        # Example params for loading json from US-East-1 S3 region
        # params = ["COMPUPDATE OFF",
        #           "STATUPDATE OFF",
        #           "JSON 'auto'",
        #           "TIMEFORMAT 'auto'",
        #           "TRUNCATECOLUMNS",
        #           "region as 'us-east-1'"]

        base_sql = \
            """
            FROM 's3://{0}/{1}'
            CREDENTIALS '{2}'
            {3};
            """.format(self.s3_bucket,
                       self.s3_key,
                       getS3Conn(),
                       params)

        load_sql = '''COPY "{0}"."{1}" {2}'''.format(self.redshift_schema,
                                                     self.table,
                                                     base_sql)
        if self.load_type == 'append':
            pg_hook.run(load_sql)
        elif self.load_type == 'rebuild':
            pg_hook.run(drop_sql)
            self.create_if_not_exists(schema, pg_hook)
            pg_hook.run(load_sql)
        elif self.load_type == 'truncate':
            pg_hook.run(truncate_sql)
            pg_hook.run(load_sql)
        elif self.load_type == 'upsert':
            self.create_if_not_exists(schema, pg_hook, temp=True)
            load_temp_sql = \
                '''COPY "{0}"."{1}{2}" {3}'''.format(self.redshift_schema,
                                                     self.table,
                                                     self.temp_suffix,
                                                     base_sql)
            pg_hook.run(load_temp_sql)
            pg_hook.run(delete_sql)
            pg_hook.run(delete_confirm_sql)
            pg_hook.run(append_sql, autocommit=True)
            pg_hook.run(drop_temp_sql)

    def create_if_not_exists(self, schema, pg_hook, temp=False):
        output = ''
        for item in schema:
            k = "{quote}{key}{quote}".format(quote='"', key=item['name'])
            field = ' '.join([k, item['type']])
            if isinstance(self.sortkey, str) and self.sortkey == item['name']:
                field += ' sortkey'
            output += field
            output += ', '
        # Remove last comma and space after schema items loop ends
        output = output[:-2]
        if temp:
            copy_table = '{0}{1}'.format(self.table, self.temp_suffix)
        else:
            copy_table = self.table
        create_schema_query = \
            '''
            CREATE SCHEMA IF NOT EXISTS "{0}";
            '''.format(self.redshift_schema)

        pk = ''
        fk = ''
        dk = ''
        sk = ''

        if self.primary_key:
            pk = ', primary key("{0}")'.format(self.primary_key)

        if self.foreign_key:
            if isinstance(self.foreign_key, list):
                fk = ', '
                for i, e in enumerate(self.foreign_key):
                    fk += 'foreign key("{0}") references {1}("{2}")'.format(e['column_name'],
                                                                            e['reftable'],
                                                                            e['ref_column'])
                    if i != (len(self.foreign_key) - 1):
                        fk += ', '
            elif isinstance(self.foreign_key, dict):
                fk += ', '
                fk += 'foreign key("{0}") references {1}("{2}")'.format(self.foreign_key['column_name'],
                                                                        self.foreign_key['reftable'],
                                                                        self.foreign_key['ref_column'])
        if self.distkey:
            dk = 'distkey({})'.format(self.distkey)

        if self.sortkey:
            if isinstance(self.sortkey, list):
                sk += '{0} sortkey({1})'.format(self.sort_type, ', '.join(["{}".format(e) for e in self.sortkey]))

        create_table_query = \
            '''
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}"
            ({fields}{primary_key}{foreign_key}) {distkey} {sortkey}
            '''.format(schema=self.redshift_schema,
                       table=copy_table,
                       fields=output,
                       primary_key=pk,
                       foreign_key=fk,
                       distkey=dk,
                       sortkey=sk)

        pg_hook.run([create_schema_query, create_table_query])

    def mysql_to_redshift_type_convert(self, mysql_type):
        mysql_type = mysql_type.lower()
        try:
            sql_type, extra = mysql_type.strip().split(" ", 1)

            # This must be a tricky enum
            if ')' in extra:
                sql_type, extra = mysql_type.strip().split(")")

        except ValueError:
            sql_type = mysql_type.strip()
            extra = ""

        # check if extra contains unsigned
        unsigned = "unsigned" in extra
        # remove unsigned now
        extra = re.sub("character set [\w\d]+\s*", "", extra.replace("unsigned", ""))
        extra = re.sub("collate [\w\d]+\s*", "", extra.replace("unsigned", ""))
        extra = extra.replace("auto_increment", "")
        extra = extra.replace("serial", "")
        extra = extra.replace("zerofill", "")
        extra = extra.replace("unsigned", "")

        if sql_type == "tinyint(1)":
            red_type = "boolean"
        elif sql_type.startswith("tinyint("):
            red_type = "smallint"
        elif sql_type.startswith("smallint("):
            if unsigned:
                red_type = "integer"
            else:
                red_type = "smallint"
        elif sql_type.startswith("mediumint("):
            red_type = "integer"
        elif sql_type.startswith("int("):
            if unsigned:
                red_type = "bigint"
            else:
                red_type = "integer"
        elif sql_type.startswith("bigint("):
            if unsigned:
                red_type = "varchar(80)"
            else:
                red_type = "bigint"
        elif sql_type.startswith("float"):
            red_type = "real"
        elif sql_type.startswith("double"):
            red_type = "double precision"
        elif sql_type.startswith("decimal"):
            # same decimal
            red_type = sql_type
        elif sql_type.startswith("char("):
            size = int(sql_type.split("(")[1].rstrip(")"))
            red_type = "varchar(%s)" % (size * 4)
        elif sql_type.startswith("varchar("):
            size = int(sql_type.split("(")[1].rstrip(")"))
            red_type = "varchar(%s)" % (size * 4)
        elif sql_type == "longtext":
            red_type = "varchar(max)"
        elif sql_type == "mediumtext":
            red_type = "varchar(max)"
        elif sql_type == "tinytext":
            red_type = "text(%s)" % (255 * 4)
        elif sql_type == "text":
            red_type = "varchar(max)"
        elif sql_type.startswith("enum(") or sql_type.startswith("set("):
            red_type = "varchar(%s)" % (255 * 2)
        elif sql_type == "blob":
            red_type = "varchar(max)"
        elif sql_type == "mediumblob":
            red_type = "varchar(max)"
        elif sql_type == "longblob":
            red_type = "varchar(max)"
        elif sql_type == "tinyblob":
            red_type = "varchar(255)"
        elif sql_type.startswith("binary"):
            red_type = "varchar(255)"
        elif sql_type == "date":
            # same
            red_type = sql_type
        elif sql_type == "time":
            red_type = "varchar(40)"
        elif sql_type == "datetime":
            red_type = "timestamp"
        elif sql_type == "year":
            red_type = "varchar(16)"
        elif sql_type == "timestamp":
            # same
            red_type = sql_type
        else:
            # all else, e.g., varchar binary
            red_type = "varchar(max)"

        return('{type} {extra_def}'.format(type=red_type, extra_def=extra))
