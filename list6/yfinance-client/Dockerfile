FROM spark:3.5.1-scala

USER root
RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y

COPY ./requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip3 install -r requirements.txt