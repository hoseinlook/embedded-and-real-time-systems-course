#!/usr/bin/env python

import abc
import json
import sys
from copy import deepcopy
from typing import List, Iterator
import matplotlib.pyplot as plt

colors = [
    'darkgray',
    'darkorange',
    'mediumorchid',
    'greenyellow',
    "yellow"]

section_color = {i: color for i, color in enumerate(colors)}


class TaskSetJsonKeys(object):
    # Task set
    KEY_TASKSET = "taskset"

    # Task
    KEY_TASK_ID = "taskId"
    KEY_TASK_PERIOD = "period"
    KEY_TASK_WCET = "wcet"
    KEY_TASK_DEADLINE = "deadline"
    KEY_TASK_OFFSET = "offset"
    KEY_TASK_SECTIONS = "sections"

    # Schedule
    KEY_SCHEDULE_START = "startTime"
    KEY_SCHEDULE_END = "endTime"

    # Release times
    KEY_RELEASETIMES = "releaseTimes"
    KEY_RELEASETIMES_JOBRELEASE = "timeInstant"
    KEY_RELEASETIMES_TASKID = "taskId"


class TaskSet(object):
    def __init__(self, data):
        self._task_set_dict = {}
        self.parse_data_to_tasks(data)
        self.build_job_releases(data)

    def highest_priority_of_section(self, default_priority, section):
        tasks = list(self._task_set_dict.values())
        tasks = list(filter(lambda x: section in x.section_ids, tasks))
        related_priorities = [task.priority for task in tasks]
        return max(*related_priorities, default_priority)

    def parse_data_to_tasks(self, data):
        task_set = {}

        for taskData in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(taskData)

            if task.id in task_set:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.relativeDeadline < 0:
                print("Error: aperiodic task must have positive relative deadline")
                return

            task_set[task.id] = task

        self._task_set_dict = task_set

    def build_job_releases(self, data):
        jobs = []

        if TaskSetJsonKeys.KEY_RELEASETIMES in data:  # necessary for sporadic releases
            for jobRelease in data[TaskSetJsonKeys.KEY_RELEASETIMES]:
                releaseTime = float(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_JOBRELEASE])
                taskId = int(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_TASKID])

                job = self.get_task_by_id(taskId).spawn_job(releaseTime)
                jobs.append(job)
        else:
            schedule_start_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
            schedule_end_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
            for task in self:
                t = max(task.offset, schedule_start_time)
                while t < schedule_end_time:
                    job = task.spawn_job(t)
                    if job is not None:
                        jobs.append(job)

                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = schedule_end_time  # aperiodic

        self.jobs = jobs

    def __contains__(self, elt):
        return elt in self._task_set_dict

    def __iter__(self):
        for key, value in self._task_set_dict.items():
            yield value

    def __len__(self):
        return len(self._task_set_dict.keys())

    def get_task_by_id(self, taskId):
        return self._task_set_dict[taskId]

    def print_tasks(self):
        print("Task Set:")
        for task in self:
            print(task)

    def print_jobs(self):
        print("\nJobs:")
        for task in self:
            for job in task.get_jobs():
                print(job)

    def sum_of_utilization(self):
        return sum([task.get_utilization() for task in self])


class Task(object):
    def __init__(self, task_dict):
        self.id = int(task_dict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(task_dict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(task_dict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.relativeDeadline: float = float(
            task_dict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, task_dict[TaskSetJsonKeys.KEY_TASK_PERIOD]))
        self.offset: float = float(task_dict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections: List[list] = task_dict[TaskSetJsonKeys.KEY_TASK_SECTIONS]
        self.section_ids = {i[0] for i in self.sections}
        self.lastJobId = 0
        self.lastReleasedTime = 0.0
        self.priority = 1 / self.relativeDeadline
        self.jobs = []

    def get_all_resources(self):
        return [section[0] for section in self.sections]

    def spawn_job(self, releaseTime):
        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime:
            print("INVALID: release time of job is not monotonic")
            return None

        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime + self.period:
            print("INVDALID: release times are not separated by period")
            return None

        self.lastJobId += 1
        self.lastReleasedTime = releaseTime

        job = Job(self, self.lastJobId, releaseTime)

        self.jobs.append(job)
        return job

    def get_jobs(self):
        return self.jobs

    def get_job_by_id(self, job_id):
        if job_id > self.lastJobId:
            return None

        job = self.jobs[job_id - 1]
        if job.id == job_id:
            return job

        for job in self.jobs:
            if job.id == job_id:
                return job

        return None

    def get_utilization(self):
        return self.wcet / self.period

    def __str__(self):
        return "task {0}: (Φ,T,C,D,∆) = ({1}, {2}, {3}, {4}, {5})".format(self.id, self.offset, self.period, self.wcet,
                                                                          self.relativeDeadline, self.sections)


class Job(object):
    def __init__(self, task: Task, job_id: int, release_time: int):
        self.task = task
        self.id = job_id
        self.release_time = release_time
        self.deadline = task.relativeDeadline + release_time
        self.actual_priority = 1 / self.task.relativeDeadline
        self.current_wcet = self.task.wcet
        self.priority = self.actual_priority
        self.sections = deepcopy(self.task.sections)
        self.section_ids = {i[0] for i in self.sections}
        self.current_section = self.sections.pop(0)
        self.is_completed = False

    def is_section_in_this_job(self, section):
        return section in self.sections

    @property
    def current_time(self) -> float:
        return self.task.wcet - self.current_wcet

    def get_resource_held(self):
        if self.current_section[0] == 0: return None
        return self.current_section[0]

    def execute(self) -> Iterator:
        "iteratively execute job and yield section"
        while True:
            self.current_section[1] = self.current_section[1] - 1
            if self.current_section[1] == 0:
                if len(self.sections) == 0:
                    self.is_completed = True
                    yield self.current_section[0]
                self.current_section = self.sections.pop(0)
                self.priority = self.actual_priority

            yield self.current_section[0]

    def __str__(self):
        return "[{0}:{1}] released at {2} -> deadline at {3}".format(self.task.id, self.id, self.release_time,
                                                                     self.deadline)

    def __repr__(self):
        return "[{0}:{1}]".format(self.task.id, self.id)


class Queue:
    def __init__(self):
        self._queue = []

    def pop(self):
        return self._queue.pop(0)

    def push(self, item):
        self._queue.append(item)
        self.sort()

    def sort(self):
        self._queue.sort(key=lambda x: x.priority, reverse=True)

    @property
    def max_priority(self):
        return max(*[i.priority for i in self._queue], -10000, -10000)

    def is_empty(self):
        return len(self._queue) == 0

    # def highest_priority_of_section(self, default_priority, section):
    #     x = max(*[job.priority for job, j in self._queue if job.is_section_in_this_job(section)], default_priority,
    #             -1000)
    #     assert x != -1000
    #     return x

    def __repr__(self):
        # return '\n'.join([f"item{i[0]},priority{i[1]}" for i in self._queue])
        return str(self._queue)

    def __str__(self):
        return self.__repr__()


class Scheduler(abc.ABC):

    def __init__(self, task_set: TaskSet):
        self.task_set = task_set
        self.queue = Queue()
        self.time = 0
        self.future_jobs = []
        self._initial_jobs()
        self.success_jobs = []
        self.failed_jobs = []
        self.make_plt()

    def make_plt(self):

        fig, ax = plt.subplots()
        self.ax = ax
        task_size = len(self.task_set) + 1

        ax.set_yticks(range(1, len(self.task_set) + 1))
        ax.set_yticklabels([f'task_id={task.id}' for task in self.task_set])
        for job in self.future_jobs:
            y = job.task.id
            ax.vlines(x=job.release_time - 1, ymin=y - 0.6, ymax=y + 0.4, color="blue", linewidth=1,
                      linestyles='dashed')
            ax.vlines(x=job.deadline - 1, ymin=y - 0.4, ymax=y + 0.6, color="red", linewidth=1, linestyles='dashed')

    def _initial_jobs(self):
        for task in self.task_set:
            for job in task.get_jobs():
                self.future_jobs.append(job)

    def check_time_and_release_job(self):
        for i, job in enumerate(self.future_jobs):
            job: Job
            if job.release_time <= self.time:
                self.queue.push(job)
                self.future_jobs.pop(i)

    def is_job_finished(self, job: Job):
        if job.is_completed:
            if self.time <= job.deadline:
                self.success_jobs.append(job)
            else:
                self.failed_jobs.append(job)
            return True
        return False


class NPPScheduler(Scheduler):

    def run(self):
        print('Validating the schedule')
        while self.time <= 1000:
            self.check_time_and_release_job()
            if self.queue.is_empty():
                self.time += 1
                continue
            the_job: Job = self.queue.pop()
            for critical_section in the_job.execute():
                self.check_time_and_release_job()
                # preemption
                if critical_section == 0:
                    if the_job.priority < self.queue.max_priority:
                        self.queue.push(the_job)
                        break
                self.ax.barh(the_job.task.id, width=1, left=self.time - 1, color=section_color[critical_section])
                self.time += 1
                if self.is_job_finished(the_job):
                    break

        if len(self.failed_jobs) != 0:
            print("This scheduler is not feasible")
        else:
            print("""no WCETs are missed \nThis schedule is feasible""")


class HLPScheduler(Scheduler):
    def run(self):
        print('Validating the schedule')
        while self.time <= 10000:
            self.check_time_and_release_job()
            if self.queue.is_empty():
                self.time += 1
                continue
            the_job: Job = self.queue.pop()
            for critical_section in the_job.execute():

                self.check_time_and_release_job()
                if the_job.priority < self.queue.max_priority:
                    self.queue.push(the_job)
                    break
                self.ax.barh(the_job.task.id, width=1, left=self.time - 1, color=section_color[critical_section])
                self.time += 1
                if critical_section != 0:
                    the_job.priority = self.task_set.highest_priority_of_section(the_job.actual_priority,
                                                                                 critical_section) + 0.01

                if self.is_job_finished(the_job):
                    break

                # preemption

        if len(self.failed_jobs) != 0:
            print("This scheduler is not feasible")
        else:
            print("""no WCETs are missed \nThis schedule is feasible""")


class PIPScheduler(Scheduler):
    def __init__(self, task_set: TaskSet):
        super().__init__(task_set)
        self.preemption_stack = []

    def run(self):
        print('Validating the schedule')
        while self.time <= 10000:
            self.check_time_and_release_job()
            if self.queue.is_empty():
                self.time += 1
                continue
            the_job: Job = self.queue.pop()
            for critical_section in the_job.execute():
                self.check_time_and_release_job()
                if critical_section != 0:
                    if self.can_inherit_priority(critical_section, the_job):
                        self.queue.push(the_job)
                        break

                if the_job.priority < self.queue.max_priority:
                    self.queue.push(the_job)
                    if critical_section != 0:
                        self.preemption_stack.append((critical_section, the_job))
                    break
                self.ax.barh(the_job.task.id, width=1, left=self.time - 1, color=section_color[critical_section])
                self.time += 1

                if self.is_job_finished(the_job):
                    break

        if len(self.failed_jobs) != 0:
            print("This scheduler is not feasible")
        else:
            print("""no WCETs are missed \nThis schedule is feasible""")

    def can_inherit_priority(self, critical_section, the_job):
        for i, item in enumerate(self.preemption_stack[::-1]):
            preempted_section, preempted_job = item
            if preempted_section == critical_section:
                preempted_job: Job
                preempted_job.priority = max(the_job.priority, preempted_job.priority) + 0.01
                self.queue.sort()
                self.preemption_stack.pop(i)
                return True
        return False


def load_file_and_create_tasks(file_path):
    with open(file_path) as json_data:
        data = json.load(json_data)
    task_set = TaskSet(data)
    task_set.print_tasks()
    task_set.print_jobs()
    return task_set


if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "taskset3.json"

    task_set = load_file_and_create_tasks(file_path)

    if task_set.sum_of_utilization() > 1:
        print(f"task set is not feasible {task_set.sum_of_utilization()} >1")

    NPPScheduler(task_set=task_set, ).run()
    plt.show()
