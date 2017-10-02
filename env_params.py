import math, time, datetime, random, shutil
from pathlib import Path
from pprint import pprint

import task_manager

class Params:
    def __init__(self):
        self.JOB_DEADLINE = 0
        self.TASK_DURATION = 0
        #self.TASK_COUNT = 0
        self.INSTANCE_COUNT = 0
        self.MIN_INSTANCES_ALLOWED = 1
        self.MAX_INSTANCES_ALLOWED = 10
        self.SIMULTANEOUS_INSTANCES_NEEDED = 0
        self.task_manager = task_manager.Task_Manager

    def update(self, args):
        #self.init_directory()
        self.task_manager = task_manager.Task_Manager(args.tasks, args.tc)
        self.TASK_DURATION_SET(args.td)
        self.JOB_DEADLINE_SET(args.jdl)
        try:
            self.MIN_INSTANCES_ALLOWED_SET(args.min)
        except:
            pass
        try:
            self.MAX_INSTANCES_ALLOWED_SET(args.max)
        except:
            pass


    def __str__(self):
        text  = "JDL:" + str(self.JOB_DEADLINE) + " | "
        text += "TD :" + str(self.TASK_DURATION) + " | "
        text += "JDL " + str(self.JOB_DEADLINE) + " | "
        text += "IC  " + str(self.INSTANCE_COUNT) + " | "
        text += "SIN " + str(self.SIMULTANEOUS_INSTANCES_NEEDED) + " | "
        text += "MIN " + str(self.MIN_INSTANCES_ALLOWED) + " | "
        text += "MAX " + str(self.MAX_INSTANCES_ALLOWED) + " | "
        text += "ST  " + str(self.STARTING_TIME_GET()) + " | "
        text += "RT  " + str(self.REMAINING_TIME()) + " | "
        text += str(self.task_manager)
        return text
    '''
    def init_directory(self, dir = "./jobs"):
        path = Path(dir)
        if (path.exists()):
            print("Path = " + str(path))
        else:
            print("Path doesn't exist")
            path.mkdir()

        dir += "/" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(random.randrange(1000,9999)) 
        self.JOB_DIR = Path(dir)
        self.JOB_DIR.mkdir()
        input_dir = dir + "/" + "input" 
        self.INPUT_DIR = Path(input_dir)
        self.INPUT_DIR.mkdir()
        #self.INPUT_DIR_UNDISPATCHED = Path(input_dir + "/" + "undispatched")
        #self.INPUT_DIR_UNDISPATCHED.mkdir()
        #self.INPUT_DIR_DISPATCHED   = Path(input_dir + "/" + "dispatched")
        #self.INPUT_DIR_DISPATCHED.mkdir()
        #self.INPUT_DIR_DONE         = Path(input_dir + "/" + "done")
        #self.INPUT_DIR_DONE.mkdir()
        self.OUTPUT_DIR = Path(dir + "/" + "output")
        self.OUTPUT_DIR.mkdir()

    '''
    def JOB_DEADLINE_GET(self):
        return self.JOB_DEADLINE

    def JOB_DEADLINE_SET(self, jdl):
        assert isinstance(jdl, int)
        self.JOB_DEADLINE = jdl
        self.SIMULTANEOUS_INSTANCES_NEEDED = math.ceil(self.JOB_DUATION_NEEDED() / self.JOB_DEADLINE)

    def TASK_DURATION_GET(self):
        return self.TASK_DURATION

    def TASK_DURATION_SET(self, td):
        assert isinstance(td, int)
        self.TASK_DURATION = td

    #def TASK_COUNT_GET(self):
    #    return self.TASK_COUNT

    #def TASK_COUNT_SET(self, tc):
    #    assert isinstance(tc, int)
    #    self.TASK_COUNT = tc

    def INSTANCE_COUNT_GET(self):
        return self.INSTANCE_COUNT

    def INSTANCE_COUNT_INCREASE(self, count = 1):
        assert isinstance(count, int)
        self.INSTANCE_COUNT += count

    def INSTANCE_COUNT_DECREASE(self, count = 1):
        assert isinstance(count, int)
        self.INSTANCE_COUNT -= count

    def MAX_INSTANCES_ALLOWED_GET(self):
        return self.MAX_INSTANCES_ALLOWED

    def MAX_INSTANCES_ALLOWED_SET(self, max):
        assert isinstance(max, int)
        self.MAX_INSTANCES_ALLOWED = max

    def MIN_INSTANCES_ALLOWED_GET(self):
        return self.MAX_INSTANCES_ALLOWED

    def MIN_INSTANCES_ALLOWED_SET(self, min):
        assert isinstance(min, int)
        self.MIN_INSTANCES_ALLOWED = min

    def TIME_NOW_GET(self):
        return round(time.time())

    def STARTING_TIME_GET(self):
        return self.STARTING_TIME

    def STARTING_TIME_SET(self):
        self.STARTING_TIME = self.TIME_NOW_GET()

    def JOB_DUATION_NEEDED(self):
        return self.task_manager.get_undispatched_count() * self.TASK_DURATION

    def SIMULTANEOUS_INSTANCES_NEEDED_GET(self):
        return self.SIMULTANEOUS_INSTANCES_NEEDED

    def SIMULTANEOUS_INSTANCES_NEEDED_INCREASE(self, count = 1):
        if ((self.SIMULTANEOUS_INSTANCES_NEEDED + count) <= self.MAX_INSTANCES_ALLOWED_GET()):
            self.SIMULTANEOUS_INSTANCES_NEEDED += count
        else:
            self.SIMULTANEOUS_INSTANCES_NEEDED = self.MAX_INSTANCES_ALLOWED_GET()

    def SIMULTANEOUS_INSTANCES_NEEDED_DECREASE(self, count = 1):
        if ((self.SIMULTANEOUS_INSTANCES_NEEDED - count) > self.MIN_INSTANCES_ALLOWED_GET()):
            self.SIMULTANEOUS_INSTANCES_NEEDED -= count
        else:
            self.SIMULTANEOUS_INSTANCES_NEEDED = self.MIN_INSTANCES_ALLOWED_GET()

    def ELAPSED_TIME(self):
        return self.TIME_NOW_GET() - self.STARTING_TIME_GET()

    def REMAINING_TIME(self):
        return self.JOB_DEADLINE - self.ELAPSED_TIME()