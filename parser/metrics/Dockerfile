FROM ubuntu:22.04

COPY requirements.txt /opt/app/
RUN pip3 install -r /opt/app/requirements.txt

COPY metrics /opt/app/

WORKDIR /opt/app/
CMD ["python3", "main.py"]