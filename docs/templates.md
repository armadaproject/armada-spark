# Armada Spark Job Templates

## Introduction

Job templates were introduced to provide a reusable way to configure Spark jobs on Armada with common settings, while still allowing runtime customization.
This feature addresses the need for standardizing Armada job configurations across teams and environments.

## How It Works

The job template system uses a precedence hierarchy where CLI configuration flags override template values.
Templates define base configurations for driver and executor pods in YAML format, which are then merged with runtime settings.

Templates can be loaded from:
- Local files (absolute or relative paths, with or without `file://` prefix)
- HTTP/HTTPS URLs

## Configuration Options

### Spark Configuration Properties

```properties
# Job-level template (applies to both driver and executors)
spark.armada.jobTemplate=/path/to/job-template.yaml

# Driver-specific template
spark.armada.driver.jobItemTemplate=/path/to/driver-template.yaml

# Executor-specific template
spark.armada.executor.jobItemTemplate=/path/to/executor-template.yaml
```

### Using Templates

Templates are specified via `--conf` flags when submitting Spark jobs:

```bash
script/submitArmadaSpark.sh \
  --master armada://armada-server:50051 \
  --conf spark.armada.jobTemplate=https://config-server/templates/base-job.yaml \
  --conf spark.armada.driver.jobItemTemplate=/templates/driver.yaml \
  --conf spark.armada.executor.jobItemTemplate=/templates/executor.yaml \
  --conf spark.armada.scheduling.queue=gpu-queue \
  --conf spark.armada.scheduling.priority=100 \
  --class com.example.SparkApp \
  app.jar
```

## Template Examples

### Job Template (Armada JobSubmitRequest API)

```yaml
queue: default-queue
jobSetId: template-jobset
```

### Driver Template (Armada JobSubmitRequestItem API)

```yaml
priority: 100
namespace: spark-drivers
labels:
  component: spark-driver
  app: data-processing
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "4040"
podSpec:
  nodeSelector:
    node-type: compute
    zone: us-east-1a
  tolerations:
    - key: spark-workload
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - resources:
        limits:
          memory: 4Gi
          cpu: 2
          nvidia.com/gpu: 1
        requests:
          memory: 4Gi
          cpu: 2
          nvidia.com/gpu: 1
```

#### Expose Driver UI

To expose the Spark driver UI, you can add an ingress configuration in the driver template:

```yaml
priority: 100
namespace: spark-drivers
labels:
  component: spark-driver
  app: data-processing
ingress:
  annotations:
    kubernetes.io/rewrite-target: /
  tlsEnabled: true
  certName: spark-driver-tls
```

### Executor Template (Armada JobSubmitRequestItem API)

```yaml
priority: 50
namespace: spark-executors
labels:
  component: spark-executor
  app: data-processing
annotations:
  cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
podSpec:
  nodeSelector:
    node-type: compute
  tolerations:
    - key: spark-workload
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - name: spark-executor
      resources:
        limits:
          memory: 6Gi
          cpu: 3
          ephemeral-storage: 50Gi
        requests:
          memory: 6Gi
          cpu: 3
          ephemeral-storage: 50Gi
  volumes:
    - name: spark-local-dir
      emptyDir:
        sizeLimit: 100Gi
```

### Always Overridden Fields

The following fields are always set by Armada Spark to ensure correct behavior:
- Pod containers and init containers fields
- Services configuration
- `restartPolicy` (always "Never")
- `terminationGracePeriodSeconds` (always 0)
