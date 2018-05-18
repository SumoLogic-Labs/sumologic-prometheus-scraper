FROM python:3.6.3

RUN mkdir /opt/sumo

RUN pip install --upgrade pip requests pip prometheus_client

COPY extract-data.py /opt/sumo/

CMD ["python", "/opt/sumo/extract-data.py"]