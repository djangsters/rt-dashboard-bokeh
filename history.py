import datetime
import os
from collections import defaultdict

import pytz
from redis_tasks.conf import settings
from redis_tasks.registries import (failed_task_registry,
                                    finished_task_registry, worker_registry)
from redis_tasks.task import Task
from redis_tasks.utils import utcnow
from redis_tasks.worker import Worker

if not settings._initialized:
    settings.configure_from_dict({'REDIS_URL': os.getenv('MONITOR_REDIS_URL'),
                             })

def get_tasks(until_ts=None):
    max_finished_tasks = 1000
    max_failed_tasks = 1000
    finished_tasks = finished_task_registry.get_tasks(-max_finished_tasks, -1)
    failed_tasks = failed_task_registry.get_tasks(-max_failed_tasks, -1)

    now = utcnow()
    running_tasks = []
    for wid, tid in worker_registry.get_running_tasks().items():
        task = Task.fetch(tid)
        task.ended_at = now
        task.running_on = Worker.fetch(wid).description
        running_tasks.append(task)

    tasks = failed_tasks + finished_tasks + running_tasks
    tasks.sort(key=lambda t: t.started_at)

    by_func = defaultdict(list)
    for t in tasks:
        by_func[t.func_name].append(t)

    # reconstruct worker-mapping
    for group in by_func.values():
        workers = []
        for task in sorted(group, key=lambda t: t.started_at):
            workers = [
                None if not t or t.ended_at <= task.started_at else t
                for t in workers
            ]
            try:
                task.worker = workers.index(None)
                workers[task.worker] = task
            except ValueError:
                task.worker = len(workers)
                workers.append(task)

    worker_streams = defaultdict(lambda: defaultdict(list))
    short_tasks_stream = defaultdict(list)

    for task in tasks:
        if until_ts and task.ended_at < until_ts:
            continue
        duration = task.ended_at - task.started_at
        if duration.seconds > 1 :
            ws = worker_streams[task.worker]
        else:
            ws = short_tasks_stream
        ws['start'].append(task.started_at)
        ws['end'].append(task.ended_at)
        ws['duration'].append(str(duration))
        ws['task_func'].append(task.func_name)
        ws['status'].append(task.status)
        ws['key'].append(task.key)
        ws['description'].append(task.description)



    return worker_streams, short_tasks_stream
