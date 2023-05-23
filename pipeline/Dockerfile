# Debian 11 is recommended.
FROM debian:11-slim

# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini libjemalloc2

# Enable jemalloc2 as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# (Optional) Add extra jars.
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
ENV SPARK_EXTRA_CLASSPATH='/opt/spark/jars/*'
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"
COPY deps/spark-bigquery-with-dependencies_2.13-0.30.0.jar "${SPARK_EXTRA_JARS_DIR}"
COPY deps/gcs-connector-hadoop2-2.2.13-shaded.jar "${SPARK_EXTRA_JARS_DIR}"
COPY deps/postgresql-42.5.4.jar "${SPARK_EXTRA_JARS_DIR}"

# Install and configure Miniconda3
# GCP recommends Conda for python
ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
COPY bin/Miniconda3-py39_23.3.1-0-Linux-x86_64.sh .
RUN bash Miniconda3-py39_23.3.1-0-Linux-x86_64.sh -b -p /opt/miniconda3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --prepend channels conda-forge \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict

# Not using Conda for packages insallation
# mamba (the faster version of Conda) takes 10 minutes to build image
# and image is bloated at 1.6GB vs 600MB for pip
# and pinning specific versions of packages is a complicated

COPY ./requirements.txt .
# Install pip
RUN python -m ensurepip --upgrade
RUN python -m pip install --no-cache-dir --upgrade -r requirements.txt

# (Required) Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark