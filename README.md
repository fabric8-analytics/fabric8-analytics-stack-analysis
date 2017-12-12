# Stack Analysis

## List of models currently present in the analytics platform


* [Gnosis](/analytics_platform/kronos/gnosis)
* [Pgm](/analytics_platform/kronos/pgm)
* [Softnet](/analytics_platform/kronos/softnet)
* [Apollo](/analytics_platform/kronos/apollo)
* [Uranus](/analytics_platform/kronos/uranus)

## To Test Locally

`python -m unittest discover tests  -v`


## To Run Evaluation Script Locally

```bash
PYTHONPATH=`pwd` python evaluation_platform/uranus/src/kronos_offline_evaluation.py
```

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


## Sample Evaluation Request Input
```
Request Type: POST
ENDPOINT: api/v1/schemas/kronos_evaluation
BODY: JSON data
{
    "training_data_url":"s3://dev-stack-analysis-clean-data/maven/github/"
}
```


## Sample Scoring Request Input
```
Request Type: POST 
ENDPOINT: /api/v1/schemas/kronos_scoring
BODY: JSON data
[
        {
            "ecosystem": "maven",
            "comp_package_count_threshold": 5,
            "alt_package_count_threshold": 2,
            "outlier_probability_threshold": 0.88,
            "unknown_packages_ratio_threshold": 0.3,
            "package_list": [         
            "io.vertx:vertx-core",
            "io.vertx:vertx-web"
    ]
        }
]
```

## Sample Response
```
[
    {
        "alternate_packages": {
            "io.vertx:vertx-health-check": [],
            "io.vertx:vertx-web": [
                {
                    "package_name": "io.vertx:vertx-web-templ-handlebars",
                    "similarity_score": 1,
                    "topic_list": [
                        "restful",
                        "web",
                        "mircoservices",
                        "real-time"
                    ]
                }
            ]
        },
        "companion_packages": [],
        "ecosystem": "maven",
        "missing_packages": [
            "io.vertx:test-random-package"
        ],
        "outlier_package_list": [
            {
                "outlier_prbability": 0.96774613950860655,
                "package_name": "io.vertx:vertx-health-check",
                "topic_list": [
                    "microservice",
                    "web-handler",
                    "metrics",
                    "health-check"
                ]
            },
            {
                "outlier_prbability": 0.9151901277396074,
                "package_name": "io.vertx:vertx-web",
                "topic_list": [
                    "http",
                    "restful",
                    "mircoservices",
                    "web"
                ]
            }
        ],
        "package_to_topic_dict": {
            "io.vertx:vertx-core": [
                "reactive",
                "concurrency",
                "non-blocking",
                "event-loop"
            ],
            "io.vertx:vertx-health-check": [
                "microservice",
                "web-handler",
                "metrics",
                "health-check"
            ],
            "io.vertx:vertx-web": [
                "http",
                "restful",
                "mircoservices",
                "web"
            ]
        },
        "user_persona": "1"
    }
]
```


## Latest Depolyment

* Maven
	* Retrained on: 2017-11-29 02:30 (UTC+5:30)
