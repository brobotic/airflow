from functools import partial

from airflow.operators.python import PythonOperator

try:
    from notifications import notify_discord_success
except ModuleNotFoundError:
    from dags.notifications import notify_discord_success


def create_standard_etl_tasks(
    create_table_callable,
    extract_and_load_callable,
    verify_load_callable,
    table: str,
    success_title: str,
    extract_outlets=None,
):
    t1 = PythonOperator(task_id="create_table", python_callable=create_table_callable)
    t2 = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load_callable,
        outlets=extract_outlets,
    )
    t3 = PythonOperator(task_id="verify_load", python_callable=verify_load_callable)
    t4 = PythonOperator(
        task_id="notify_discord",
        python_callable=partial(
            notify_discord_success,
            verify_task_id="verify_load",
            table=table,
            title=success_title,
        ),
    )

    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(t4)

    return t1, t2, t3, t4

