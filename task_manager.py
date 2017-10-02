import math, time, datetime, random, shutil, json
from pathlib import Path

class Task_Manager:
    def __init__(self, tasks, task_count=0):
        self.TASKS = []
        print(tasks)
        print(task_count)
        self.init_directory()
        if (tasks == None):
            self.process_list(task_count)
        else:
            self.process_tasks(tasks)
        '''
        print("********************************************")
        #print(self.get_undispatched_task("instance2"))
        print("---------------------------------------------")
        print(self.get_undispatched_tasks())
        print("---------------------------------------------")
        print(self.get_dispatched_tasks())
        print("---------------------------------------------")
        print(self.get_done_tasks())
        print("---------------------------------------------")
        print(self.get_undispatched_count())
        print(self.get_dispatched_count())
        print(self.get_done_count())
        print("********************************************")
        
        #print(self.get_undispatched_task("instance1"))
        print("---------------------------------------------")
        print(self.get_undispatched_tasks())
        print("---------------------------------------------")
        print(self.get_dispatched_tasks())
        print("---------------------------------------------")
        print(self.get_done_tasks())
        print("---------------------------------------------")
        print(self.get_undispatched_count())
        print(self.get_dispatched_count())
        print(self.get_done_count())
        #self.TASK_COUNT_SET(args.tc)
        '''
    def __str__(self):
        text = "UTC :" + str(self.get_undispatched_count()) + " | "
        text += "DTC :" + str(self.get_dispatched_count()) + " | "
        text += "FTC :" + str(self.get_finished_count()) + " | "
        return text

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
        self.OUTPUT_DIR = Path(dir + "/" + "output")
        self.OUTPUT_DIR.mkdir()

    def process_tasks(self, tasks):
        path = Path(tasks)
        if (path.exists()):
            for x in sorted(list(path.glob('*.json'))):
                if not x.is_dir():
                    file_name = str(x.resolve().stem)
                    print("File_name : " + file_name)
                    task = {'id': file_name, \
                        'status': 'undispatched', \
                        'instance': None, \
                        'dispached_at':None, \
                        'finished_at':None, \
                    }
                    shutil.copy(str(x),str(self.INPUT_DIR))
                    task['json'] = str(self.INPUT_DIR) + "/" + file_name + ".json"
                    directory_name = str(x.parent) + "/" + file_name + "_files"
                    directory = Path(directory_name)
                    if (directory.exists()):
                        task['data'] = directory_name
                    self.TASKS.append(task)
            print(self.TASKS)
            print(len(self.TASKS))

        else:
            print("Tasks Directory doesn't exist")

    def process_list(self, task_count):
        for x in range(0,task_count):
            task = {"task_" + str(x) : { \
                'status': 'undispatched', \
                'instance': None, \
                'dispached_at':None, \
                'finished_at':None, \
                'json': None, \
                'data': None, \
            }}
            self.TASKS.append(task)
        print(self.TASKS)
        print(len(self.TASKS))

    def count(self, key, value):
        return len([i for i in self.TASKS if i[key] == value])

    def findAll(self, key, value):
        return [i for i in self.TASKS if i[key] == value]

    def findFirst(self, key, value):
        for i, dic in enumerate(self.TASKS):
            if dic[key] == value:
                return self.TASKS[i]
        return None
    
    def get_undispatched_task(self, instance):
        task = self.findFirst("status", "undispatched")
        if (task != None):
            task["status"] = "dispatched"
            task["instance"] = instance
            task["dispached_at"] = time.time()
        return task

    def get_undispatched_tasks(self):
        return self.findAll("status", "undispatched")

    def get_undispatched_count(self):
        return self.count("status", "undispatched")

    def get_dispatched_tasks(self):
        return self.findAll("status", "dispatched")

    def get_dispatched_count(self):
        return self.count("status", "dispatched")

    def get_finished_tasks(self):
        return self.findAll("status", "finished")

    def get_finished_count(self):
        return self.count("status", "finished")

    def save_result(self,task_id, instance_id, result_id, result):
        file_name = str(self.OUTPUT_DIR) + "/" + result_id 
        try:
            json_object = json.loads(result)
            file_name += ".json"

        except ValueError:
            file_name += ".result"
 
        file = open(file_name, 'a')
        file.write(result)
        file.close()

    def task_finished(self,task_id, instance_id, finished_at):
        print("666666666666666666666666666666666666")
        print(task_id)
        print("666666666666666666666666666666666666")
        task = self.findFirst("id",task_id)
        print(task)
        task["status"] = "finished"
        task['finished_at'] = finished_at
        return 