FROM python:3.9-slim-bullseye 

RUN apt-get update && apt-get install -y build-essential python-dev nginx supervisor curl

# spams install error with 1.22.x
RUN pip --no-cache-dir install --upgrade pip && \
    pip --no-cache-dir install numpy==1.22.0 

RUN pip --no-cache-dir install flask pandas pyarrow fastparquet s3fs boto3 

RUN pip install uwsgi 

COPY app.py /app.py

HEALTHCHECK --interval=30s CMD curl --fail http://localhost:6077/healthcheck || exit 1

COPY uwsgi.ini /etc/uwsgi/
COPY nginx.conf /etc/nginx/ 
COPY supervisord.conf /etc/

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
