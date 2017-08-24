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

- Deploy secrets and [config map](https://github.com/fabric8-analytics/fabric8-analytics-common/blob/master/openshift/generate-config.sh)

```bash
oc apply -f secret.yaml
oc apply -f config.yaml
```

- Deploy app using `oc`

```bash
oc process -f openshift/template.yaml | oc apply -f -
```


## Sample Request

```
[
        {
            "ecosystem": "maven",
            "package_list": [
            	"org.mongodb:mongodb-driver-async",
                "io.vertx:vertx-core",
                "io.vertx:vertx-web"
				]
        }
    ]
```

## Latest Depolyment

* Maven
	* Redployed on: Aug 24, 2017 1:17:57 PM (GMT + 05:30)