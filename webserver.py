#!/usr/bin/env python

import argparse, time, sys
from pprint import pprint
from threading import Thread
from pathlib import Path
import env_params, dispatcher 

params = None

def replica_controller():
    params.STARTING_TIME_SET()
    while True:
        print(params)
        if (params.task_manager.get_undispatched_count() > 0):
            print("TC > 0")
            if (params.replica_manager.replica_count() < params.SIMULTANEOUS_REPLICAS_NEEDED_GET()):
                print("RC < SRN")
                print("Adding a replica")
                params.replica_manager.add_replicas(1)
                # RC++
                pass
            else:
                print("RC >= SRN")
                if (params.SIMULTANEOUS_REPLICAS_NEEDED_GET() > params.task_manager.get_dispatched_count()):
                    pass
                else:                    
                    if((params.task_manager.get_undispatched_count() * params.TASK_DURATION_GET()) > params.REMAINING_TIME()):
                        print("TC*TD > RT")
                        if (params.SIMULTANEOUS_REPLICAS_NEEDED_GET() < params.MAX_REPLICAS_ALLOWED_GET()):
                            print("SRN < MAX")
                            params.SIMULTANEOUS_REPLICAS_NEEDED_INCREASE()
                            continue
                        else:
                            print("SRN >= MAX")
                            pass
                    else:
                        print("TC*TD <= RT")
                        if (params.SIMULTANEOUS_REPLICAS_NEEDED_GET() >= params.MAX_REPLICAS_ALLOWED_GET()):
                            print("SRN >= MAX")
                            params.SIMULTANEOUS_REPLICAS_NEEDED_DECREASE()
                        else:
                            pass
        else:
            print("UTC <= 0")
            if (params.task_manager.get_finished_count() == params.task_manager.get_task_count()):
                print("TC == FTC")
                break 
            else:
                print("TC != FTC")
                pass
        print(params)
        time.sleep(3)

    print("Job Accomplished")
    params.replica_manager.stop_service()
    sys.exit()

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
    parser.add_argument("--server", help="HTTP Server Address (Accessible from all VMs)", required=True)
    parser.add_argument("--port"  , help="HTTP Server port, default = 8777", type=check_positive)
    parser.add_argument("--image" , help="Client Docker Image ", required=True)
    parser.add_argument("--min"   , help="Min Allowed Replicas, default = 1" , type=check_positive)
    parser.add_argument("--max"   , help="Max Allowed Replicas, default = 10", type=check_positive)
    parser.add_argument("--td"    , help="Single Task Duration (in seconds)", type=check_positive, required=True)
    parser.add_argument("--jdl"   , help="Job (all tasks) deadline (in seconds)", type=check_positive, required=True)
    parser.add_argument("--dir"   , help="Jobs directory, default = ./jobs")
    #parser.add_argument("--task"  , help="Task .json file")
    #parser.add_argument("--etask" , help="Extended Task JSON File and files directory, --etask x.json x_files")
    parser.add_argument("--tc"    , help="Task Count", type=int)
    parser.add_argument("--tasks" , help="Extended Tasks directory (JSON Files and Tasks' files), it overrides --tc", type=check_directory)
    return parser.parse_args()

if __name__ == "__main__":
    args = getArgs()
    print(args)
    params = env_params.Params(args)
    replica_controller_thread = Thread(target = replica_controller)
    replica_controller_thread.start()
    dispatcher_thread = Thread(target = dispatcher.start, args = (params, args.port,))
    dispatcher_thread.start()