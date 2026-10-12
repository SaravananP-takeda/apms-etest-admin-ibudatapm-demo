# from airflow.hooks.base import BaseHook
# import requests
# from airflow.models import Variable
import time
# import json
from airflow.providers.databricks.hooks.databricks import DatabricksHook

# ════════════════════════════════════════════════════════════════════════════════════════════════
# Code for Databricks Cluster Management (E.g Create Cluster, Terminate Cluster. Cluster State, etc.)
# ════════════════════════════════════════════════════════════════════════════════════════════════

class DatabricksClusterManager:
    def __init__(self, databricks_conn_id):
        self.hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
        self.CREATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/create")

    def create_cluster(self, cluster_config):
        """Creates a cluster and returns the cluster_id."""
        # This returns the cluster_id immediately
        # CREATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/create")
        CREATE_CLUSTER_ENDPOINT = self.CREATE_CLUSTER_ENDPOINT
        # response = self.hook.start_cluster(cluster_config)
        response = self.hook._do_api_call(CREATE_CLUSTER_ENDPOINT, cluster_config)
        cluster_id = response["cluster_id"]
        # return response # The ID needed for your next tasks i.e. Execute NB Task, Delete Cluster
        return cluster_id # The ID needed for your next tasks i.e. Execute NB Task, Delete Cluster

    def get_cluster_state(self, cluster_id):
        cluster_state_obj = self.hook.get_cluster_state(cluster_id)
        cluster_state = cluster_state_obj.state
        cluster_state_message = cluster_state_obj.state_message
        print(f"Cluster state is {cluster_state} with state message {cluster_state_message}")
        return cluster_state

    def terminate_cluster(self, cluster_id):
        """Permanently terminates the cluster."""
        json={"cluster_id": cluster_id}
        self.hook.terminate_cluster(json)
  
def create_dbx_cluster(**context):
    print("Creating Cluster...")
    databricks_conn_id = context.get("databricks_conn_id")
    cluster_config = context.get("cluster_config")
    # print(f"cluster_config is {cluster_config}")
    policy_id = cluster_config.get("policy_id")
    # print(f"policy_id is {policy_id}")
    task_group_id = context.get("task_group_id")
    dbx_mgr = DatabricksClusterManager(databricks_conn_id = databricks_conn_id)
    start_time = time.perf_counter()
    cluster_id = dbx_mgr.create_cluster(cluster_config)
    context["ti"].xcom_push(key=f"cluster_id_{task_group_id}", value=cluster_id)
    print(f"cluster_id of new created cluster is: {cluster_id}")

    while True:
        cluster_state = dbx_mgr.get_cluster_state(cluster_id)
        if cluster_state == "RUNNING":
            elapsed_time = time.perf_counter() - start_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            print(f"Cluster is running. Time taken: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
            return cluster_id # This push cluster_id into XCom, optional
        elif cluster_state in ["TERMINATING", "TERMINATED", "ERROR"]:
            raise RuntimeError(f"Cluster creation failed. Current state: {cluster_state}")
        else:
            print(f"Cluster is in {cluster_state}. Waiting for it to start. Sleep for 20 sec ...")
            time.sleep(20)  # Wait for 20 seconds before checking again

def terminate_dbx_cluster(**context):
    print("Terminating Cluster...")
    databricks_conn_id = context.get("databricks_conn_id")
    task_group_id = context.get("task_group_id")
    create_cluster_task_id = context.get("create_cluster_task_id") # This will be in form of f"START-DATABRICKS-CLUSTER-{task_group_id}
    print(f"create_cluster_task_id is {create_cluster_task_id}")
    cluster_id = context["ti"].xcom_pull(
        task_ids=create_cluster_task_id,
        key=f"cluster_id_{task_group_id}"
    )
    print(f"cluster_id for cluster to terminate is: {cluster_id}")
    dbx_mgr = DatabricksClusterManager(databricks_conn_id = databricks_conn_id)
    cluster_state = dbx_mgr.get_cluster_state(cluster_id)
    if cluster_state == "TERMINATED":
        print("Cluster is already terminated.")
        return
    dbx_mgr.terminate_cluster(cluster_id)
    print("Cluster terminated successfully")