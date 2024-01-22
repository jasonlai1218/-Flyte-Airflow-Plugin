import logging
import logging.handlers
import os
import shutil
import tempfile
import threading

import dagfactory
import requests
import yaml
from airflow import DAG  # noqa: F401 # imported but unused
from jutopia_airflow_plugin.common.yaml_renderer import YamlRenderer
import traceback

"""
# a_wf.yaml #

a_wf:
  default_args:
    start_date: 2024-01-22T00:00:00.000Z
    retries: 1
    retry_delay_sec: 6000
    nsml_cfg:
      login:
        username: None
        password: None
      docker_image: None
  schedule_interval: 22 20 * * *
  tasks:
    task_a:
      operator: >-
        jutopia_airflow_plugin.jutopia_plugin.operators.mlu_k8s_pod_operator.MLUK8SPodOperator
      on_failure_callback: dags.repo.airflow_plugins.slack_alert.send_task_fail
      arguments:
        - /opt/airflow/ECAC-SMCH/src/jutopia/jutopia_spark_submit.py
        - '--py_path'
        - ECAC-SMCH/src/task/a_task.py
        - '--spark_args'
        - >-
          --query_date {{ macros.ds_format(macros.ds_add(data_interval_end | ds,
          +1), '%Y-%m-%d', '%Y%m%d') }}
        - '--keytab_base64'
        - '{{ var.value.get(''jutopia_cluster_keytab_base64'', '''') }}'
        - '--user_id'
        - '{{ var.value.get(''jutopia_cluster_user_id'', '''') }}'
      cmds:
        - python
      image: 'harbor.linecorp.com/ecacda/smch/service/ft-sample-code'
    task_b:
      operator: >-
        jutopia_airflow_plugin.jutopia_plugin.operators.mlu_k8s_pod_operator.MLUK8SPodOperator
      on_failure_callback: dags.repo.airflow_plugins.slack_alert.send_task_fail
      arguments:
        - /opt/airflow/ECAC-SMCH/src/task/b_task.py
      cmds:
        - python
      image: 'harbor.linecorp.com/ecacda/smch/service/ft-sample-code'
      dependencies:
        - task_a

"""
handler = logging.handlers.SysLogHandler(
    address=("mlu-logging.linecorp.com", 2514),
    facility=logging.handlers.SysLogHandler.LOG_LOCAL4,
)
logger = logging.getLogger("syslog-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

if os.path.abspath(__file__).find("plugins/templates") < 0:
    renderer = YamlRenderer("/opt/airflow/dags")
    temp_path = tempfile.mkdtemp(prefix="dag-factory--")
    log_url = "https://mlu.linecorp.com/portal/callback/pipeline/deploy/log"
    sent = False
    try:
        # Render a jinja2 template using predefined_dict.
        converted_dict = renderer.render_template("a_wf.yaml")

        # Save it.
        converted_yaml_path = os.path.join(temp_path, "a_wf.yaml")
        with open(converted_yaml_path, "w") as f:
            yaml.dump(converted_dict, f, default_flow_style=False)

        # Generate Airflow DAGs dynamically using dag-factory.
        dag_factory = dagfactory.DagFactory(converted_yaml_path)
        dag_factory.generate_dags(globals())
        logging.info("|DagFactory generated|64dee49efc11e208617d7abf)")

    except Exception as e:
        logger.error("|LW14331|DagFactoryError(deployId: 65ae33072fe5bc933d66639d) = {}|64dee49efc11e208617d7abf".format(traceback.format_exc()))
        if len(log_url) > 0 and not sent:
            logging.info("Sending back the dag error")
            try:
                r = requests.post(
                    log_url,
                    json=[{"deployId": "65ae33072fe5bc933d66639d", "error": traceback.format_exc(), "airflowId" : "64dee49efc11e208617d7abf"}],
                    headers={
                        "Authorization": "708e13ff3154487680dd820b96ca5752"
                    },
                )
                if r.status_code == 200:
                    sent = True
                else:
                    logger.error("|LW14331|DagFactoryError(deployId: 65ae33072fe5bc933d66639d) = Sent dag error failed, status code = {}|64dee49efc11e208617d7abf".format(r.status_code))
            except Exception as e1:
                logger.error("|LW14331|DagFactoryError(deployId: 65ae33072fe5bc933d66639d) = {}".format(e1))
    shutil.rmtree(temp_path)