FROM python:3.8-slim

RUN apt-get -qq update && \
    apt-get -qq install wget gzip

WORKDIR /app
COPY dist/vogdb-0.1.0-py3-none-any.whl /app/dist/
COPY scripts/* /app/
RUN pip install /app/dist/vogdb-0.1.0-py3-none-any.whl uvicorn

ENTRYPOINT [ "/bin/bash" ]
CMD [ "uvicorn" ]

