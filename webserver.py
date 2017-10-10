#!/usr/bin/env python

import argparse, time, sys
from pprint import pprint
from threading import Thread
from pathlib import Path
import dispatcher 

import task_manager, replica_manager

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
    parser.add_argument("--tc"    , help="Task Count", type=int)
    parser.add_argument("--tasks" , help="Extended Tasks directory (JSON Files and Tasks' files), it overrides --tc", type=check_directory)
    return parser.parse_args()

if __name__ == "__main__":
    args = getArgs()
    print(args)
    task_manager = task_manager.Task_Manager(args.td, args.tasks, args.tc)
    replica_manager = replica_manager.Replica_Manager(args, task_manager)

    replica_controller_thread = Thread(target = replica_manager.start)
    replica_controller_thread.start()
    dispatcher_thread = Thread(target = dispatcher.start, args = (task_manager, replica_manager, args.port,))
    dispatcher_thread.start()