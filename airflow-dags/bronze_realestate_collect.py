import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import requests

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,
        PythonOperator,
    )
    from airflow.sdk import (
        DAG,
        TaskGroup,
        Variable,
        get_current_context,
    )
except ImportError:
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import (
        BranchPythonOperator,
        PythonOperator,
        get_current_context,
    )
    from airflow.utils.task_group import TaskGroup

DAG_ID = "bronze_realestate_collect"

API_URL = (
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/"
    "getRTMSDataSvcAptTrade"
)

S3_BUCKET = "realestate-parkjiyeon"
S3_REGION = "ap-southeast-2"

LAWD_CODES = [
    "11680",
    "11650",
    "11710",
    "11440",
    "11170",
    "11200",
]


def xml_text(root, tag_name):
    for element in root.iter():
        local_name = element.tag.rsplit("}", 1)[-1]

        if local_name == tag_name and element.text:
            return element.text.strip()

    return None


def xml_item_count(root):
    return sum(
        1
        for element in root.iter()
        if element.tag.rsplit("}", 1)[-1] == "item"
    )


def get_target_yyyymm():
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf or {}) if dag_run else {}

    yyyymm = str(
        conf.get("yyyymm")
        or context["data_interval_start"].strftime("%Y%m")
    )

    if not re.fullmatch(r"\d{6}", yyyymm):
        raise ValueError(
            "yyyymm must contain exactly six digits, "
            "for example 202608."
        )

    return yyyymm


def collect_realestate(lawd_cd):
    import boto3
    print(
        f"collector=박지연, time={datetime.now()}, lawd={lawd_cd}",
        flush=True,
    )

    yyyymm = get_target_yyyymm()

    service_key = unquote(
        Variable.get("REAL_ESTATE_API_KEY").strip()
    )

    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": yyyymm,
        "pageNo": "1",
        "numOfRows": "10000",
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            timeout=(10, 60),
        )
        response.raise_for_status()

    except requests.RequestException as exc:
        print(f"API_REQUEST_FAILED lawd={lawd_cd}: {exc}")

        return {
            "lawd_cd": lawd_cd,
            "yyyymm": yyyymm,
            "status": "request_failed",
            "count": 0,
        }

    raw_xml = response.content

    try:
        root = ET.fromstring(raw_xml)

    except ET.ParseError as exc:
        print(f"XML_PARSE_FAILED lawd={lawd_cd}: {exc}")

        return {
            "lawd_cd": lawd_cd,
            "yyyymm": yyyymm,
            "status": "xml_parse_failed",
            "count": 0,
        }

    result_code = (
        xml_text(root, "resultCode")
        or xml_text(root, "returnReasonCode")
    )

    result_message = (
        xml_text(root, "resultMsg")
        or xml_text(root, "returnAuthMsg")
    )

    if result_code not in {None, "0", "00", "000"}:
        print(
            f"API_RESULT_ERROR lawd={lawd_cd} "
            f"code={result_code} message={result_message}"
        )

        return {
            "lawd_cd": lawd_cd,
            "yyyymm": yyyymm,
            "status": "api_error",
            "count": 0,
        }

    item_count = xml_item_count(root)
    total_count_text = xml_text(root, "totalCount")

    try:
        total_count = (
            int(total_count_text)
            if total_count_text
            else item_count
        )
    except ValueError:
        total_count = item_count

    if total_count == 0 or item_count == 0:
        print(f"NO_DATA lawd={lawd_cd} yyyymm={yyyymm}")

        return {
            "lawd_cd": lawd_cd,
            "yyyymm": yyyymm,
            "status": "empty",
            "count": 0,
        }

    object_key = f"bronze/{yyyymm}/{lawd_cd}.xml"

    s3_client = boto3.client(
        "s3",
        region_name=S3_REGION,
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=object_key,
        Body=raw_xml,
        ContentType="application/xml",
    )

    print(
        f"UPLOADED s3://{S3_BUCKET}/{object_key} "
        f"count={total_count} bytes={len(raw_xml)}"
    )

    return {
        "lawd_cd": lawd_cd,
        "yyyymm": yyyymm,
        "status": "uploaded",
        "count": total_count,
        "s3_key": object_key,
    }


def choose_after_collect(**context):
    task_instance = context["ti"]
    results = []

    for lawd_cd in LAWD_CODES:
        result = task_instance.xcom_pull(
            task_ids=f"collect_group.collect_{lawd_cd}"
        )

        results.append(result)
        print(f"COLLECT_RESULT lawd={lawd_cd}: {result}")

    all_uploaded = all(
        result and result.get("status") == "uploaded"
        for result in results
    )

    if all_uploaded:
        print("BRANCH=summary_done")
        return "summary_done"

    print("BRANCH=skip_upload")
    return "skip_upload"


with DAG(
    dag_id=DAG_ID,
    description="Collect apartment trade XML into S3 bronze",
    start_date=datetime(
        2026,
        8,
        1,
        tzinfo=timezone.utc,
    ),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "parkjiyeon",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["q1", "realestate", "bronze"],
) as dag:

    with TaskGroup(
        group_id="collect_group"
    ) as collect_group:

        for lawd_cd in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{lawd_cd}",
                python_callable=collect_realestate,
                op_kwargs={
                    "lawd_cd": lawd_cd,
                },
            )

    branch_after_collect = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=choose_after_collect,
        trigger_rule="all_done",
    )

    skip_upload = EmptyOperator(
        task_id="skip_upload"
    )

    summary_done = EmptyOperator(
        task_id="summary_done"
    )

    collect_group >> branch_after_collect
    branch_after_collect >> [
        skip_upload,
        summary_done,
    ]
