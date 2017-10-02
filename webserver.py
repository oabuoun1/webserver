#!/usr/bin/env python

import argparse, time
from pprint import pprint
from threading import Thread
from pathlib import Path
import env_params, dispatcher 

params = env_params.Params()

def scaleUp():
    print("Scaling Up")
    print("Start instance")
    #print("Dispatch Task")
    '''
    task = params.task_manager.get_undispatched_task("instance3")
    print("////////////////////////////////////////////")
    print(task)
    print("////////////////////////////////////////////")
    print(params.task_manager.get_undispatched_tasks())
    print("////////////////////////////////////////////")
    task["status"] = "done"
    print("////////////////////////////////////////////")
    print(params.task_manager.TASKS)
    print("////////////////////////////////////////////")
    '''
    #print(params.task_manager.get_undispatched_task("instance"))
    return 

def scaler():
    params.STARTING_TIME_SET()
    while True:
        print(params)
        if (params.task_manager.get_undispatched_count() > 0):
            print("TC > 0")
            if (params.INSTANCE_COUNT_GET() < params.SIMULTANEOUS_INSTANCES_NEEDED_GET()):
                print("IC < SIN")
                # Scaleup
                scaleUp()
                # IC++
                params.INSTANCE_COUNT_INCREASE()
                # TC--
                continue
            else:
                print("IC >= SIN")
                if (params.SIMULTANEOUS_INSTANCES_NEEDED_GET() > params.task_manager.get_dispatched_count()):
                    pass
                else:                    
                    if((params.task_manager.get_undispatched_count() * params.TASK_DURATION_GET()) > params.REMAINING_TIME()):
                        print("TC*TD > RT")
                        if (params.SIMULTANEOUS_INSTANCES_NEEDED_GET() < params.MAX_INSTANCES_ALLOWED_GET()):
                            print("SIN < MAX")
                            params.SIMULTANEOUS_INSTANCES_NEEDED_INCREASE()
                            continue
                        else:
                            print("SIN >= MAX")
                            pass
                    else:
                        print("TC*TD <= RT")
                        if (params.SIMULTANEOUS_INSTANCES_NEEDED_GET() >= params.MAX_INSTANCES_ALLOWED_GET()):
                            print("SIN >= MAX")
                            params.SIMULTANEOUS_INSTANCES_NEEDED_DECREASE()
                        else:
                            pass
        else:
            print("TC <= 0")
            if (params.INSTANCE_COUNT_GET() <= 0):
                print("IC <= 0")
                break 
            else:
                print("IC > 0")
                pass
        print(params)
        time.sleep(3)

    print("Loop is Over")

def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def check_directory(ivalue):
    path = Path(ivalue)
    if ((not path.exists()) | (not path.is_dir())):    
         raise argparse.ArgumentTypeError("%s doesn't exist or it isn't a directory" % ivalue)
    return ivalue

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min"   , help="Min Allowed Instances, default = 1" , type=check_positive)
    parser.add_argument("--max"   , help="Max Allowed Instances, default = 10", type=check_positive)
    parser.add_argument("--td"    , help="Single Task Duration (in seconds)", type=check_positive, required=True)
    parser.add_argument("--jdl"   , help="Job (all tasks) deadline (in seconds)", type=check_positive, required=True)
    parser.add_argument("--port"  , help="HTTP Server port, default = 8777", type=check_positive)
    parser.add_argument("--dir"   , help="Jobs directory, default = ./jobs")
    #parser.add_argument("--task"  , help="Task .json file")
    #parser.add_argument("--etask" , help="Extended Task JSON File and files directory, --etask x.json x_files")
    parser.add_argument("--tc"    , help="Task Count", type=int)
    parser.add_argument("--tasks" , help="Extended Tasks directory (JSON Files and Tasks' files), it overrides --tc", type=check_directory)
    return parser.parse_args()

if __name__ == "__main__":
    #from sys import argv
    args = getArgs()
    print(args)
    params.update(args)
    scaler_thread = Thread(target = scaler)
    scaler_thread.start()
    dispatcher_thread = Thread(target = dispatcher.start, args = (params, args.port,))
    dispatcher_thread.start()