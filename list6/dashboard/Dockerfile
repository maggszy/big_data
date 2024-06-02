FROM python:3.9.0-alpine

RUN apk update 
# RUN apk add python3
# RUN apk add cmake
RUN apk add --update py-pip

COPY ./requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt


