columns = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    # "exception", "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    # "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    # "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    # "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    # "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
]

max_length = max(len(c) for c in columns)
cmds = []
for c in columns:
    # 가변적인 공백 추가 (최대 길이에 맞춰 정렬)
    padding = " " * (max_length - len(c))
    cmds.append(f'echo "{c}{padding} : ====> {{{{ {c} }}}}"')

print("\n".join(cmds))