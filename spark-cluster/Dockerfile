FROM apache/spark:3.3.3

USER root
RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y

COPY ./requirements.txt /opt/app/requirements.txt
COPY ./entrypoints /opt/app/entrypoints

WORKDIR /opt/app
RUN pip3 install -r requirements.txt

EXPOSE 7077 8080 4040
