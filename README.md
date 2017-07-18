# Stack Analysis

## List of models currently present in the analytcs platform


* [Gnosis](/analytics_platform/gnosis)
* [Kronos](/analytics_platform/kronos)
* [Softnet](/analytics_platform/softnet)

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

- Deploy app using kedge

Make sure you have [kedge installed](https://github.com/kedgeproject/kedge#installation).

```bash
kedge apply -f app.yaml
```
