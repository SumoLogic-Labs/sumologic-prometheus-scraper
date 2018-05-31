FROM python:3.6.3

RUN mkdir /opt/sumo

RUN pip install --upgrade pip requests pip prometheus_client

COPY sumologic_prometheus_scraper.py /opt/sumo/

CMD ["python", "/opt/sumo/sumologic_prometheus_scraper.py"]