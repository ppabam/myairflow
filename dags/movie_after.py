from pprint import pprint
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonVirtualenvOperator,
)

from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie after",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=["api", "movie", "sensor"],
) as dag:
    REQUIREMENTS = ["git+https://github.com/ppabam/movie.git@2503.20.1"]
    BASE_DIR = f"/home/tom/data/{DAG_ID}"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_done = FileSensor(
        task_id="check.done",
        filepath="/home/tom/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )

    def fn_gen_meta(ds_nodash, base_path, **kwargs):
        import pandas as pd
        from movie.api.after import fillna_meta, read_df_or_none, save_with_mkdir
        
        meta_path = f"{base_path}/meta/meta.parquet"
        previous_df = read_df_or_none(meta_path)
            
        current_df = pd.read_parquet(f"/home/tom/data/movies/merge/dt={ds_nodash}")[["movieCd", "multiMovieYn", "repNationCd"]]
        update_meta_df = fillna_meta(previous_df, current_df)

        save_with_mkdir(update_meta_df, meta_path)
        print(update_meta_df)
        

    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR},
    )

    def fn_gen_movie(ds_nodash, base_path, **kwargs):
        import pandas as pd
        from movie.api.call import save_df
        
        meta_path = f"{base_path}/meta/meta.parquet"
        meta_df = pd.read_parquet(meta_path)
        current_df = pd.read_parquet(f"/home/tom/data/movies/merge/dt={ds_nodash}")
        merged_df = meta_df.merge(current_df, on="movieCd", how="left", suffixes=("_meta", "_current"))
        merged_df["multiMovieYn"] = merged_df["multiMovieYn_meta"].combine_first(merged_df["multiMovieYn_current"])
        merged_df["repNationCd"] = merged_df["repNationCd_meta"].combine_first(merged_df["repNationCd_current"])
        final_df = merged_df[current_df.columns]
        final_df['dt'] = ds_nodash
        save_df(final_df, f'{base_path}/dailyboxoffice', ['dt', 'multiMovieYn', 'repNationCd'])
        print(final_df)
        

    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR},
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={"BASE_DIR": BASE_DIR},
        append_env=True,
    )

    start >> check_done >> gen_meta >> gen_movie >> make_done >> end
