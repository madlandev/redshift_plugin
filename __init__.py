from airflow.plugins_manager import AirflowPlugin
from redshift_plugin.operators.s3_to_redshift_operator import S3ToRedshiftOperator, S3ToRedshiftSpectrumOperator
from redshift_plugin.operators.redshift_data_quality_operator import RedshiftDataQualityOperator
from redshift_plugin.macros.redshift_auth import redshift_auth


class S3ToRedshiftPlugin(AirflowPlugin):
    name = "S3ToRedshiftPlugin"
    operators = [S3ToRedshiftOperator, S3ToRedshiftSpectrumOperator, RedshiftDataQualityOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = [redshift_auth]
    admin_views = []
    flask_blueprints = []
    menu_links = []
