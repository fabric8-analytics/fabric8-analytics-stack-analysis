# Stack Analysis

## List of models currently present in the analytcs platform


* [Gnosis](/analytics_platform/kronos/gnosis)
* [Pgm](/analytics_platform/kronos/pgm)
* [Softnet](/analytics_platform/kronos/softnet)

## Deploy to openshift cluster

- Create project

```bash
oc new-project fabric8-analytics-stack-analysis
```

- Build image and push to docker hub

```bash
docker build -t docker.io/surajd/kronos:latest -f Dockerfile.kronos .
docker push docker.io/surajd/kronos:latest
```

- Deploy secret

```bash
oc apply -f secret.yaml
```

- Deploy app using `oc`

```bash
oc apply -f kronos-os.yaml
```
