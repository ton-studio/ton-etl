FROM python:3.9

COPY requirements.txt /opt/app/
RUN pip3 install -r /opt/app/requirements.txt

COPY gauges /opt/app/gauges
COPY main.py /opt/app/

WORKDIR /opt/app/
CMD ["python3", "main.py"]