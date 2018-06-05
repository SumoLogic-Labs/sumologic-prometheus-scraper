FROM python:3.6.6-alpine3.7

RUN mkdir /opt/sumo

RUN pip install --upgrade pip requests prometheus_client apscheduler

COPY sumologic_prometheus_scraper.py /opt/sumo/

CMD ["python", "/opt/sumo/sumologic_prometheus_scraper.py"]