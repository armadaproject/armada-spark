ARG armada_spark_image=spark:armada
FROM ${armada_spark_image}

USER 0

# Install Jupyter, PySpark, and Python dependencies
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir \
        jupyter \
        notebook \
        ipykernel \
        pyspark==3.5.5 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create Jupyter workspace and runtime directories
RUN mkdir -p /home/spark/workspace && \
    mkdir -p /home/spark/.local/share/jupyter && \
    mkdir -p /home/spark/.jupyter && \
    chown -R 185:185 /home/spark/workspace && \
    chown -R 185:185 /home/spark/.local && \
    chown -R 185:185 /home/spark/.jupyter

ARG spark_uid=185
USER ${spark_uid}
RUN HOME=/home/spark python3 -m ipykernel install --user --name python3 --display-name "Python 3"


ENV HOME=/home/spark
ENV SPARK_HOME=/opt/spark
ENV SPARK_DIST_CLASSPATH=/opt/spark/coreJars/*
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

WORKDIR /home/spark/workspace

# Expose Jupyter port
EXPOSE 8888

CMD ["jupyter", "notebook", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--NotebookApp.token=''", \
     "--NotebookApp.password=''", \
     "--NotebookApp.notebook_dir=/home/spark/workspace"]
