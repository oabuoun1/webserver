FROM python:3
ADD webserver.py /
ADD env_params.py /
ADD dispatcher.py /
ADD task_manager.py /
ADD tasks /tasks
ENV PORT 8777
ENV TD 10
ENV JDL 10
ENV TASKS tasks
CMD [ "sh", "-c",  "python ./webserver.py --port ${PORT} --td ${TD} --jdl ${JDL} --tasks ${TASKS}" ]