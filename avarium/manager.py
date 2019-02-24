import collections
import importlib
import logging
import json
import multiprocessing
import os
import shutil
import signal
import sys
import time


class Manager:
    worker_info = collections.namedtuple(
        'worker',
        (
            'Worker',
            'count',
            'kwargs'
        )
    )

    def __init__(self, worker_module_name, poll_delay, death_folder,
                 log_path=None, log_level=logging.INFO):
        signal.signal(signal.SIGTERM, self.handle_sigterm)

        if log_path:
            logging.basicConfig(filename=log_path)
        else:
            logging.basicConfig()

        self.worker_module_name = worker_module_name
        self.log = logging.getLogger()
        self.log.setLevel(log_level)

        self.poll_delay = poll_delay / 1000.0
        self.death_folder = death_folder
        self.workers = {}
        self.running_workers = collections.defaultdict(dict)

    def handle_sigterm(self, signum, frame):
        self.log.info("Caught sigterm, exiting.")

        self.kill_everything_and_die()

    def kill_everything_and_die(self):
        self.kill_everything()
        self.die(0)

    def die(self, code=0):
        try:
            sys.exit(code)
        finally:
            os._exit(code)

    def start(self):
        # Regenerate death folder
        try:
            shutil.rmtree(self.death_folder)
        except OSError:
            pass

        os.makedirs(self.death_folder)

        # Spawn workers
        for name in self.workers:
            info = self.workers[name]

            for n in range(info.count):
                self.spawn_worker(name, info)

        # Start event loop
        self.event_loop(name)

        self.die(1)

    def add_worker(self, name, Worker, count, **kwargs):
        if count > 0:
            self.workers[name] = self.worker_info(Worker, count, kwargs)

    def add_worker_configs(self, defaults, worker_configs):
        module_template = '.'.join(worker_module_name, '{}')

        for worker_config in worker_configs:
            merged_config = defaults.copy()

            merged_config.update(worker_config)

            count = merged_config.pop('worker_count')

            if count <= 0:
                continue

            name = merged_config.pop('name')
            cls_name = merged_config.pop('class')
            module_name = module_template.format(name)
            module = importlib.import_module(module_name)
            Worker = getattr(module, cls_name)
            self.workers[name] = self.worker_info(Worker, count, merged_config)

    def spawn_worker(self, name, info):
        start = lambda **k: info.Worker()._initailize(**k).start()
        info.Worker.death_folder = self.death_folder

        worker_process = multiprocessing.Process(
            target=start,
            kwargs=info.kwargs,
            daemon=True
        )

        worker_process.start()

        self.running_workers[name][worker_process.pid] = worker_process

    def kill_everything(self):
        for name in self.running_workers:
            pid_map = self.running_workers[name]

            for pid in pid_map:
                process = pid_map[pid]
                self.kill_worker(process)

    def kill_worker(self, process):
        process.terminate()

        if process.is_alive():
            os.kill(process.pid, 9)

    def event_loop(self, name):
        while True:
            try:
                self.check_for_changes()
                time.sleep(self.poll_delay)
            except (KeyboardInterrupt, SystemExit):
                self.kill_everything_and_die()
            except Exception as e:
                self.log.exception(e)

    def check_for_changes(self):
        for name in self.running_workers:
            pid_map = self.running_workers[name]

            self.manage_pool(name, pid_map)

    def manage_pool(self, name, pid_map):
        for pid in pid_map:
            process = pid_map[pid]
            death_file_path = os.path.join(self.death_folder, str(pid))

            worker_is_dead = any([
                not process.is_alive(),
                os.path.exists(death_file_path)
            ])

            if worker_is_dead:
                info = self.workers[name]

                self.handle_death(name, info.Worker, process, death_file_path)
                self.spawn_worker(name, info)

    def handle_death(self, name, Worker, process, death_file_path):
        pid = process.pid

        if process.is_alive():
            self.log.warning("Force terminating pid".format(pid))
            process.terminate()
            os.kill(pid, 9)

        del self.running_workers[name][pid]

        if os.path.exists(death_file_path):
            with open(death_file_path, 'r') as f:
                message = json.loads(f.read())

            success = message.get(b'success', None)
            death_message = message.get(b'message', None)
            msg = "Worker {} termination Success: {}, Message: {}"

            self.log.info(msg.format(pid, success, death_message))

            os.remove(death_file_path)
