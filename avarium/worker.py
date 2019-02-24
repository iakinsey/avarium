import datetime
import logging
import json
import os
import random
import shutil
import signal
import sys
import time


class Worker:
    def _initailize(
        self,
        inbox,
        outbox,
        dlq,
        inbox_regex,
        outbox_regex,
        max_iterations,
        death_folder,
        max_sleep_time,
        outbox_max_size,
        worker_config,
        log_path,
        log_level
    ):
        """
        Initialization hook called by process manager. This function is kept
        separate from the constructor so that workers can be instantiated from
        the repl easily.
        """

        signal.signal(signal.SIGTERM, self.handle_sigterm)
        logging.basicConfig(path=log_path)

        self.name = self.__class__.__name__
        self.tasks = []
        self.sequence = 0
        self.inbox = inbox
        self.outbox = outbox
        self.dlq = dlq
        self.outbox_regex = outbox_regex
        self.outbox_max_size = outbox_max_size
        self.max_iterations = max_iterations
        self.pid = os.getpid()
        self.death_folder = death_folder
        self.inbox_regex = inbox_regex
        self.schedule = max_sleep_time
        self.sleep_time = random.randint(0, max_sleep_time) / 1000.0
        self.log = logging.getLogger()

        self.create_mailboxes()
        self.log.setLevel(log_level)

        self.init(**worker_config)

        return self

    def init(self, **kwargs):
        pass

    def handle_sigterm(self, signum, frame):
        self.kill_process()

    def kill_process(self, code=0):
        try:
            sys.exit(code)
        finally:
            os._exit(code)

    def create_mailboxes(self):
        paths = (self.inbox, self.outbox, self.dlq)

        for path in paths:
            self.create_mailbox(path)

    def create_mailbox(self, path):
        try:
            if path:
                os.makedirs(path)
        except FileExistsError:
            pass

    def on_message(self, message):
        raise NotImplementedError

    def event_loop(self):
        max_iterations = range(self.max_iterations)
        for n in max_iterations:
            self.run_iteration()

    def run_iteration(self):
        self.do_work()
        self.send_metrics()

    def do_work(self):
        self.find_and_process_work()

    def find_and_process_work(self):
        path = self.claim_file()

        if path:
            message = self.get_work_from_file(path)
            success = self.process_work(message)

            if success:
                os.remove(path)
            else:
                self.move_to_dlq(path)
        else:
            time.sleep(self.sleep_time)

    def process_work(self, message):
        result = None

        try:
            result = self.on_message(message)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.log.error("{} got error".format(self.name))
            self.log.exception(e)

            return False
        finally:
            if result is not None and self.outbox:
                self.write_result(result)

        return True

    @property
    def outbox_has_space(self):
        if self.outbox and self.outbox_max_size:
            return len(os.listdir(self.outbox)) < self.outbox_max_size
        else:
            return True

    @property
    def pending_outbox_count(self):
        all_files = os.listdir(self.outbox)
        paths = list(filter(self.outbox_regex.match, all_files))

        return len(paths)

    @property
    def outbox_space(self):
        if self.outbox and self.outbox_max_size:
            size = self.outbox_max_size - len(os.listdir(self.outbox))

            if size > 0:
                return size
            else:
                return 0

    def claim_specific_file(self, path):
        new_path = "{}.claimed".format(path)

        try:
            shutil.move(path, new_path)
        except (KeyboardInterrupt, SystemExit):
            self.kill_process()
        except:
            return None
        else:
            return new_path

    def claim_file(self):
        if self.outbox_has_space:
            all_files = os.listdir(self.inbox)
            self.metric("inbox_size",  len(all_files))
            paths = sorted(filter(self.inbox_regex.match, all_files))

            for file_name in paths:
                path = os.path.join(self.inbox, file_name)
                new_path = self.claim_specific_file(path)

                if new_path:
                    return new_path

    def generate_unique_name(self, prefix=''):
        self.sequence += 1

        return "{}{}_{}_{}_{}".format(
            prefix,
            self.name,
            self.pid,
            int(time.time()),
            self.sequence
        )

    def write_result(self, message, prefix='', path=None):
        if not path:
            name = self.generate_unique_name(prefix)
            path = os.path.join(self.outbox, name)

        with open(path, 'w') as f:
            f.write(json.dumps(message))

    def get_work_from_file(self, path):
        with open(path, 'r') as f:
            contents = json.loads(f.read())

        return contents

    def die(self, success, message=None):
        death_file_path = os.path.join(self.death_folder, str(self.pid))
        self.log.debug("{} about to die".format(self.name))
        self.send_metrics()

        death_message = {
            'success': success
        }

        if message:
            death_message['message'] = message

        with open(death_file_path, 'w') as f:
            f.write(json.dumps(death_message))

        self.kill_process()

    def start(self):
        try:
            self.event_loop()
        except KeyboardInterrupt:
            pass
        except:
            raise

        self.die(True)

    def move_to_dlq(self, path):
        if self.dlq:
            file_name = os.path.basename(path).replace(".claimed", "")
            dlq_path = os.path.join(self.dlq, file_name)

            self.log.info("Writing {} to {} dlq".format(file_name, self.name))
            shutil.move(path, dlq_path)


class ScheduledWorker(Worker):
    def do_work(self):
        current_time = int(time.time())
        result = None

        try:
            result = self.run(current_time)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            self.log.exception(e)
        finally:
            if result is not None and self.outbox:
                self.write_result(result)

        time.sleep(self.schedule)

    def run(self, current_time):
        name = self.__class__.__name__

        raise NotImplementedError("{}.{}".format(name, 'run'))
