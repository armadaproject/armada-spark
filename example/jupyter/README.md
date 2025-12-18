# Jupyter with Armada Spark

## Prerequisites

- Docker installed
- Base Armada Spark image (`spark:armada`) built (see main project README)
- Armada server accessible from your host
- Base Armada Spark image (`spark:armada`) is already present in the armada cluster

## Building the Image

Build the Jupyter image from the `example/jupyter` directory:

```bash
cd example/jupyter
docker build -f docker/Dockerfile -t armada-spark-jupyter:latest .
```

The build context must be `example/jupyter` so that the `notebooks/` directory is accessible.

## Running the Container

Run the Jupyter container with the required ports and volume mounts:

```bash
docker run -d \
  --name armada-jupyter \
  -p 8888:8888 \
  -p 10061:10061 \
  -p 7078:7078 \
  -e SPARK_DRIVER_HOST=10.0.0.80 \
  -e SPARK_DRIVER_PORT=7078 \
  -e SPARK_BLOCK_MANAGER_PORT=10061 \
  -e ARMADA_MASTER=local://armada://host.docker.internal:30002 \
  -e ARMADA_QUEUE=test \
  -e IMAGE_NAME=spark:armada \
  -v $(pwd)/workspace:/home/spark/workspace \
  -v $(pwd)/../../conf:/opt/spark/conf \
  armada-spark-jupyter:latest
```

Copy the example notebooks to workspace.

```bash
cp notebooks/*.ipynb workspace/
```

**Ports:**
- `8888`: Jupyter notebook interface
- `10061`: Spark BlockManager port
- `7078`: Spark driver port

**Volumes:**
- `workspace/`: Your notebooks directory (persisted on host)
- `conf/`: Spark configuration files

## Accessing Jupyter

Once the container is running, access Jupyter at:

```
http://localhost:8888
```

No authentication is required (token and password are disabled).

## Configuration

The example notebook (`jupyter_armada_spark.ipynb`) uses the following environment variables (with defaults):

- `SPARK_DRIVER_HOST`: Driver hostname (default: `10.0.0.80`)
- `SPARK_DRIVER_PORT`: Driver port (default: `7078`)
- `SPARK_BLOCK_MANAGER_PORT`: Block manager port (default: `10061`)
- `ARMADA_MASTER`: Armada master URL (default: `local://armada://host.docker.internal:30002`)
- `ARMADA_QUEUE`: Armada queue name (default: `test`)
- `IMAGE_NAME`: Spark image name (default: `spark:armada`)

## Notes

- The workspace directory is mounted as a volume, so notebooks created in Jupyter are persisted on your host
- Make sure your host IP is accessible from the Kubernetes cluster where Armada executors run
- The driver runs in client mode, so it must be reachable by the executors
