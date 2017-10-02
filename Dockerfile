FROM python:3
ADD webserver.py /
ADD env_params.py /
ADD dispatcher.py /
ADD task_manager.py /
ADD tasks /tasks
ENTRYPOINT ["python3", "./webserver.py"]
CMD []