from airflow.plugins_manager import AirflowPlugin
from dbt_hook import DbtHook
from dbt_operator import DbtOperator


class DbtPlugin(AirflowPlugin):
    name = "dbt_plugin"
    operators = [DbtOperator]
    hooks = [DbtHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilders = []