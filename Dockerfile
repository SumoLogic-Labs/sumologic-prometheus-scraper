FROM python:3.6.5-alpine3.7

RUN pip install --upgrade pip pipenv

WORKDIR /opt/sumo

COPY . /opt/sumo/

RUN pipenv install --system

CMD ["python", "./sumologic_prometheus_scraper.py"]