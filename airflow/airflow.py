from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import popular_videos
import update_channel
from datetime import datetime

dag = DAG('youtube_regulary', description='get popular_videos',
          schedule_interval='0 11,23 * * *',
          start_date=datetime(2020, 11, 1))

popular_videos_task = PythonOperator(
    task_id = 'popular_vidoes_task',
    provide_context=False,
    python_callable= popular_videos.run,
    dag=dag
)

update_channel_task = PythonOperator(
    task_id = 'update_channel_task',
    provide_context=False,
    python_callable= update_channel.run,
    dag=dag
)

popular_videos_task >> update_channel_task
