from resource_management import *
from resource_management.libraries.functions.default import default
from resource_management.libraries.script.script import Script

config = Script.get_config()

install_dir = config['configurations']['airflow-env']['install_dir']
download_url = config['configurations']['airflow-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')
conf_dir = '/etc/airflow'
airflow_user = airflow_group = config['configurations']['airflow-env']['airflow_user']
hostname = config["hostname"]

airflow_home = install_dir
airflow_base_url = default('configurations/airflow-env/base_url', 'http://' + hostname + ':8082')
airflow_load_examples = default('configurations/airflow-env/load_examples','True')
airflow_base_log_folder = default('configurations/airflow-env/base_log_folder','/var/log/airflow')
airflow_dags_folder = default('configurations/airflow-env/dags_folder','/data/airflow/dags')
airflow_sql_alchemy_conn = default('configurations/airflow-env/sql_alchemy_conn','sqlite:////data/airflow/airflow.db')

celery_broker_url = default('configurations/airflow-env/celery_broker_url', 'redis://127.0.0.1:6379/0')
celery_result_backend = default('configurations/airflow-env/celery_result_backend', 'redis://127.0.0.1:6379/0')

airflow_conf = default('configurations/airflow-env/content', "")

airflow_config_path = "/etc/airflow/airflow.cfg"
airflow_webserver_pidfile_path = "/var/run/airflow/webserver.pid"
airflow_env_path = "/etc/airflow/airflow-env.sh"

scheduler_runs = default('configurations/airflow-env/scheduler_runs', 6)

webserver_pid = '/var/run/airflow/webserver.pid'
flower_pid = '/var/run/airflow/flower.pid'
worker_pid = '/var/run/airflow/worker.pid'

principal_name_webserver = default('configurations/airflow-env/principal_name_webserver', 'airflow')
keytab_path_webserver = default('configurations/airflow-env/keytab_path_webserver', '')
