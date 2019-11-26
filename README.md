# Content Evasion Moderation


### Requirements
- https://confluence.mpi-internal.com/display/YAPO/How+to+use+ArgoFlow+and+Kubernetes

### Compile docker image
```
make docker-build
```

### Run docker compose
```
make start
```

### How to run spark job 

```
spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.container.image=containers.mpi-internal.com/yapo/content-evasion-moderation:latest \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.pyspark.pythonVersion=3 \
    /app/src/test.py

```