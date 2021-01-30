FROM python:3.8-slim

RUN apt-get -qq update && \
    apt-get -qq install wget gzip

WORKDIR /app
COPY requirements.txt scripts/* /app/
COPY vogdb/ /app/vogdb/
RUN pip install -r requirements.txt

ENTRYPOINT [ "/bin/bash" ]
CMD [ "uvicorn" ]

