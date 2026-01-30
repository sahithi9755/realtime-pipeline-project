FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install required libs for Pandas UDF
RUN pip install --no-cache-dir pandas==2.2.2 pyarrow==15.0.2

WORKDIR /app
COPY app/ /app/

USER jovyan
