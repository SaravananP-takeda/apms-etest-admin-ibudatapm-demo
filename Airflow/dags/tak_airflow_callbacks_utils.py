import sys
import os
import logging
import fnmatch
from airflow.models import Variable
from airflow.utils.state import State
from airflow.exceptions import AirflowSensorTimeout, AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from airflow_servicenow_handler import notify_api_on_failure
from airflow.utils.email import send_email

LOG_PFX = "TAKEDA: "

def send_email_notification(context):
    try:
        email_to = context.get("email_to")
        email_subject = context.get("email_subject")
        email_message = context.get("email_message")
        send_email(
            to = email_to,
            subject = email_subject,
            html_content = email_message
        )
    except Exception as e:
        print(f"Exception received while sending email notification : {e}" )

def svcnow_create_incident(context, snow_attributes):
    # pass
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    
    logging.info(f"{LOG_PFX}Servicenow Incident being created for {task_id} job part of {dag_id} DAG using CI {snow_attributes.get('configuration_item')} with Severity {snow_attributes.get('severity')}")
    
    notify_api_on_failure(
        context,
        payload=snow_attributes,
    )

    print("Servicenow Incident is created")

def run_handlers(context, dag_callback_action_obj, handlers):

    for handler in handlers:
        try:
            dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
        except Exception as e:
            print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
            logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def get_failure_details(context):

    ti = context["task_instance"]
    dag_run = context["dag_run"]
    exception = context.get("exception")
 
    details = {
        "dag_id": ti.dag_id,
        "job_name": ti.task_display_name,         # "job name"
        "task_id": ti.task_id,                    # task id
        "run_id": ti.run_id,                      # "job id" - identifies the run
        "map_index": ti.map_index,                # -1 unless dynamically mapped
        "try_number": ti.try_number,
        "hostname": ti.hostname,
        "operator": ti.operator,
 
        # --- timings (all timezone-aware UTC datetimes) ---
        "start_time": ti.start_date,
        "end_time": ti.end_date, #or timezone.utcnow(),   # fail time == finish time
        "duration_sec": ti.duration,
        "logical_date": dag_run.logical_date,
        "run_type": dag_run.run_type,
 
        # --- error ---
        "error_type": type(exception).__name__ if exception else None,
        "error_message": str(exception) if exception else context.get("reason"),
        # "log_tail": _log_tail(ti),

        "log_url": ti.log_url,
    }
 
    # logging.error("Task failed: %s", details)
    print(f"Task Failure detail is: {details}")
    return details


def Globalscape_Job_Failure(context):
    # pass
    #Job Failure:  <JobName> <JobID>
    # "<JobName>" Failed at <SysDate.MM/dd/yyyy> <JobFinish.hh:mm:ss AM/PM>

    # Agent: <JobAgent>

    # Command: <JobCommand>

    # Error:
    # <JobErrMsg>
    # <JobOutput>

    task_failure_details = get_failure_details(context)
    DagId = task_failure_details.get("dag_id")
    DagRunId = task_failure_details.get("run_id")
    JobName = task_failure_details.get("job_name")
    JobID = task_failure_details.get("task_id")
    if JobName == "" or JobName == None:
        JobName = JobID
    end_time = task_failure_details.get("end_time")
    end_time_str_format = end_time.strftime("%B %d, %Y at %I:%M:%S %p %Z")
    # end_time_str_format = end_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    # context["email_subject"] = "Job Failure:  " + DagId + " " + JobName + " " + JobID
    context["email_subject"] = "Job Failure:  " + DagId + " " + JobName
    email_message = ""
    email_message +=  JobName +  " Failed at " + end_time_str_format
    email_message += "<br>"
    email_message += "DAG Name: " + DagId
    email_message += "<br>"
    email_message += "DAG Run Id: " + DagRunId
    email_message += "<br>"
    email_message += "Error:" + "<br>"
    email_message += task_failure_details.get("error_message") + "<br>"
    print(f"email_message for Globalscape_Job_Failure notfication is {email_message}")
    context["email_message"] = email_message
    send_email_notification(context)

def on_success_callback_S3(context, af_env, dag_params):

    task_id = context["task_instance"].task_id
    ti = context["ti"]

    try:
        bucket_name = dag_params[af_env][task_id]["bucket_name"]
        key_pattern = dag_params[af_env][task_id]["bucket_key"]

        logging.info(f"{LOG_PFX}on_success_callback_S3 firing for {task_id} - bucket={bucket_name} pattern={key_pattern}")

        if "*" not in key_pattern:
            # Not a wildcard pattern on this sensor - nothing to resolve.
            logging.info(f"{LOG_PFX}{task_id}: bucket_key has no wildcard, nothing to resolve")
            return

        prefix = key_pattern.split("*")[0]
        hook = S3Hook(aws_conn_id="aws_default")
        candidate_keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix) or []
        matched_keys = [k for k in candidate_keys if fnmatch.fnmatch(k, key_pattern)]

        if not matched_keys:
            raise AirflowException(
                f"No S3 key under s3://{bucket_name}/{prefix} matches pattern '{key_pattern}' - "
                f"sensor {task_id} said one existed, but it's gone by the time this ran."
            )

        if len(matched_keys) == 1:
            resolved_key = matched_keys[0]
        else:
            # More than one file currently matches the wildcard - take the most
            # recently modified one rather than guessing from list order, and
            # log the rest so this is easy to spot if it wasn't expected.
            resolved_key = max(
                matched_keys,
                key=lambda k: hook.get_key(k, bucket_name=bucket_name).last_modified,
            )
            logging.warning(
                f"{LOG_PFX}{len(matched_keys)} S3 keys matched pattern '{key_pattern}' under "
                f"s3://{bucket_name}/{prefix} - using the most recently modified one: "
                f"{resolved_key}. All matches: {matched_keys}"
            )

        logging.info(f"{LOG_PFX}Resolved wildcard bucket_key for {task_id}: {resolved_key}")
        ti.xcom_push(key="resolved_key", value=resolved_key)
        logging.info(f"{LOG_PFX}Pushed resolved_key={resolved_key!r} to XCom for {task_id}")

    except Exception as e:
        logging.error(f"{LOG_PFX}on_success_callback_S3 failed to resolve wildcard key for {task_id}: {e}")
        raise

def on_failure_callback_S3(context, dag_callback_action_obj):
    task_id = context["task_instance"].task_id
    ti = context["ti"]

    exception = context.get("exception")

    if isinstance(exception, AirflowSensorTimeout):
        # Files land at ad hoc times during the day - the sensor is given until the
        # SENSOR_TIMEOUT cutoff to see the file show up. Timing out just means nothing
        # arrived *today* - not a real failure, and NOT an incident, so
        # on_failure_abnormal_completion_handlers (SNOW incident / email) are
        # deliberately skipped for this branch.
        message = "Files did not arrive, skipping the task"
        logging.info(f"{LOG_PFX}{task_id}: {message}")
        print(message)

        # Sensor itself is forced to SUCCESS (not FAILED) so this shows as a normal
        # daily outcome rather than a failure.
        ti.set_state(State.SUCCESS)

        # There's nothing for the real processing task downstream of this sensor to do
        # without a file, so mark it SKIPPED explicitly rather than letting it run (it
        # would otherwise run anyway, since its trigger rule only sees the sensor's
        # now-SUCCESS state). The shared "*.SUCCESS" checkpoint markers are left alone -
        # those just record "this branch was attempted", not "a file arrived" - so they
        # still proceed normally.
        dag_run = context["dag_run"]
        for downstream_task in context["task"].downstream_list:
            if downstream_task.task_id.endswith(".SUCCESS"):
                continue
            downstream_ti = dag_run.get_task_instance(downstream_task.task_id)
            if downstream_ti:
                downstream_ti.set_state(State.SKIPPED)
                logging.info(f"{LOG_PFX}{task_id}: marked downstream task {downstream_task.task_id} as SKIPPED (no file)")

    elif isinstance(exception, ClientError):
        error_code = exception.response["Error"]["Code"]
        logging.info(f"{LOG_PFX}AWS ClientError received for {task_id} job: {error_code}")
        print(f"AWS ClientError received for {task_id} job: {error_code}")

        handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])
        for handler in handlers:
            try:
                dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
            except Exception as e:
                print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
                logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    else:
        print("Unknown failure")
        # TO DO - enhance this section

def on_success_callback_python(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id
    # handlers = on_success_completion_handlers.get(task_id, [])
    handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])

    run_handlers(context, dag_callback_action_obj, handlers)

    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_failure_callback_python(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    # svcnow_create_incident(context, dag_callback_action_obj.snow_attributes)

    handlers = dag_callback_action_obj.on_failure_abnormal_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = dag_callback_action_obj.on_failure_error_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_success_callback_lambda(context):
    pass

def on_failure_callback_lambda(context):
    pass

def on_success_callback_iics(context, dag_callback_action_obj):
    task_id = context["task_instance"].task_id
    # handlers = on_success_completion_handlers.get(task_id, [])
    handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_failure_callback_iics(context, dag_callback_action_obj):
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    svcnow_create_incident(context, dag_callback_action_obj.snow_attributes)

    handlers = dag_callback_action_obj.on_failure_abnormal_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = dag_callback_action_obj.on_failure_error_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_success_callback_dbx_lf_job(context, dag_callback_action_obj):
    task_id = context["task_instance"].task_id
    handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)

    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")


def on_failure_callback_dbx_lf_job(context, dag_callback_action_obj):
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    # svcnow_create_incident(context, dag_callback_action_obj.snow_attributes)

    handlers = dag_callback_action_obj.on_failure_abnormal_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = dag_callback_action_obj.on_failure_error_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_success_callback_globalscape(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id
    handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)

    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_failure_callback_globalscape(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    svcnow_create_incident(context, dag_callback_action_obj.snow_attributes)

    handlers = dag_callback_action_obj.on_failure_abnormal_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = dag_callback_action_obj.on_failure_error_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_success_callback_ssh(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id
    handlers = dag_callback_action_obj.on_success_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

def on_failure_callback_ssh(context, dag_callback_action_obj):
    # pass
    task_id = context["task_instance"].task_id

    logging.info(f"{LOG_PFX}Creating SNOW Incident for failed task {task_id}")
    svcnow_create_incident(context, dag_callback_action_obj.snow_attributes)

    handlers = dag_callback_action_obj.on_failure_abnormal_completion_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")

    handlers = dag_callback_action_obj.on_failure_error_handlers.get(task_id, [])
    run_handlers(context, dag_callback_action_obj, handlers)
    # for handler in handlers:
    #     try:
    #         dag_callback_action_obj.handler_callback_fun_map.get(handler)(context)
    #     except Exception as e:
    #         print(f"{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
    #         logging.info(f"{LOG_PFX}{LOG_PFX}{dag_callback_action_obj.handler_callback_fun_map.handler.__name__}failed with error : {e}")
