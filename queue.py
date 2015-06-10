#!/usr/bin/python

import random
import time

from Queue import Empty, Full, Queue
from threading import Semaphore, Thread


DEBUG = True

# Configuration
# =============
q_size = 5

# Max number of machines that can process a task. Each machine is assumed to be
# able to process only 1 task at a time.
max_machines = 4

# Max task processing time in seconds (will be randomized)
max_task_time = 5

# Max number of tasks to dispatch per second (will be randomized)
num_tasks_per_sec = 5

# Interval period of how often tasks should be dispatched
dispatch_interval = 1


q = Queue(maxsize=q_size)
num_dropped = 0
num_processed = 0
total_dispatched = 0


def write_debug(msg):
    if DEBUG:
        print msg


def process_task(task, machines):
    global q
    # Set a random task processing time
    task_time = random.random() * max_task_time
    time.sleep(task_time)
    q.task_done()
    machines.release()


def print_stats():
    global q
    current_size = q.qsize()
    load = current_size / float(q.maxsize)
    print('\nQueue stats')
    print('================')
    print('Load: {load}%'.format(load=load * 100))
    print('Queue size: {qsize} of {maxsize}\n'.format(qsize=current_size,
                                                      maxsize=q.maxsize))


class System(object):
    """Global system definition."""

    def __init__(self):
        global max_machines, num_tasks_per_sec
        self.processor = Processor(max_machines)
        self.producer = Producer(num_tasks_per_sec)
        self.consumer = Consumer(self.processor)
        self.producer.daemon = True
        self.consumer.daemon = True

    def start(self):
        self.producer.start()
        self.consumer.start()

    def stop(self):
        self.consumer.should_stop = True
        self.producer.should_stop = True
        self.consumer.join()
        self.producer.join()


class Consumer(Thread):
    """Consumer thread that picks a task from the task queue and process it."""

    def __init__(self, processor, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)
        self.processor = processor
        self.should_stop = False

    def run(self):
        global num_processed

        while not self.should_stop:
            write_debug('-> Consumer: Waiting for a task to process...')
            try:
                # Set a block timeout so we don't need to wait for long when
                # stopping the process
                task = q.get(block=True, timeout=2)
            except Empty:
                pass
            else:
                self.processor.process(task)
                num_processed += 1


class Producer(Thread):
    """Producer thread that dispatches tasks to process."""

    def __init__(self, num_tasks_per_sec, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.num_tasks_per_sec = num_tasks_per_sec
        self.counter = 0
        self.should_stop = False

    def run(self):
        global num_dropped, total_dispatched

        while not self.should_stop:
            # Set a random num of tasks to dispatch
            num_tasks = random.randint(1, self.num_tasks_per_sec)
            total_dispatched += num_tasks
            write_debug('\nProducer: Dispatching {num} tasks\n'.format(num=num_tasks))

            for i in xrange(num_tasks):
                name = 'Batch {batch} (Task {id})'.format(batch=self.counter, id=i)
                task = Task(name)
                try:
                    q.put(task, block=False)
                except Full:
                    print 'Producer: Queue is full! Discarding task "{}"'.format(task)
                    num_dropped += 1

            self.counter += 1
            time.sleep(dispatch_interval)
            print_stats()


class Processor(object):
    """Processes tasks."""
    def __init__(self, max_machines):
        self.machines = Semaphore(max_machines)

    def process(self, task):
        write_debug('-> Consumer: Acquiring a machine to process task "{}"...'.format(task))
        self.machines.acquire(blocking=True)
        write_debug('-> Consumer: Processing: {}'.format(task))
        t = Thread(name=task.name, target=process_task, args=[task, self.machines])
        t.start()
        write_debug("-> Consumer: Done processing: {}".format(task))

    def get_load(self):
        global q
        current_size = q.qsize()
        return current_size / float(q.maxsize)


class Task(object):
    """Task definition."""
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


if __name__ == '__main__':
    try:
        system = System()
        system.start()
        while True:
            if not system.producer.isAlive() and not system.consumer.isAlive():
                break
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print "Stopping process..."
        system.stop()

    num_unprocessed = total_dispatched - (num_processed + num_dropped)
    percent_processed = num_processed / float(total_dispatched) * 100
    percent_dropped = num_dropped / float(total_dispatched) * 100
    percent_unprocessed = num_unprocessed / float(total_dispatched) * 100

    print
    print "Summary"
    print "======="
    print "Total tasks dispatched: {total}".format(total=total_dispatched)
    print "Num of tasks processed: {num} ({percent:.0f}% of all tasks)".format(
        num=num_processed, percent=percent_processed)
    print "Num of tasks dropped: {num} ({percent:.0f}% of all tasks)".format(
        num=num_dropped, percent=percent_dropped)
    print "Num of tasks unprocessed: {num} ({percent:.0f}% of all tasks)".format(
        num=num_unprocessed, percent=percent_unprocessed)
    print
