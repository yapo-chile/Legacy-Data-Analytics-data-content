# Content Evasion Moderation
This readme is based in [Confluence documentation install argoflow](https://confluence.mpi-internal.com/display/YAPO/How+to+use+ArgoFlow+and+Kubernetes).

## Requirements
- [hyperkit](https://github.com/moby/hyperkit)
- [kubectl](https://matthewpalmer.net/kubernetes-app-developer/articles/guide-install-kubernetes-mac.html)
- [minikube](https://github.com/kubernetes/minikube/releases/tag/v1.6.0-beta.0)
- [spark](https://spark.apache.org/downloads.html)

### Run minikube
```
minikube start --vm-driver=hyperkit --memory 8192 --cpus 3 --disk-size=50g
minikube status
```

### Kubernetes environment
```
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

## Build docker image

```
eval $(minikube docker-env)
make docker-build
```

### How to run test spark job

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


```
spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --name content-evasion-moderation \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.container.image=containers.mpi-internal.com/yapo/content-evasion-moderation:feat_pyspark-integration \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.pyspark.pythonVersion=3 \
    /app/src/main.py -master=k8s://https://$(minikube ip):8443



    --conf spark.kubernetes.driver.pod.name=content-evasion-moderation \

```