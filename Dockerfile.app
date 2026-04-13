FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/project

WORKDIR /opt/project

COPY requirements.app.txt /tmp/requirements.app.txt
RUN pip install --no-cache-dir -r /tmp/requirements.app.txt

COPY services /opt/project/services
COPY data /opt/project/data
COPY notebooks /opt/project/notebooks

