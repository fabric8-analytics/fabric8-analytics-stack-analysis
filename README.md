# Stack Analysis

## List of models currently present in the analytics platform


* [Gnosis](/analytics_platform/kronos/gnosis)
* [Pgm](/analytics_platform/kronos/pgm)
* [Softnet](/analytics_platform/kronos/softnet)
* [Apollo](/analytics_platform/kronos/apollo)
* [Uranus](/analytics_platform/kronos/uranus)

## To Deploy Locally
Set up .env file with environment variables, i.e
```bash
cat > .env <<-EOF
# Amazon AWS s3 credentials
AWS_S3_ACCESS_KEY_ID=$CUSTOM_KEY
AWS_S3_SECRET_ACCESS_KEY=$CUSTOM_SECRET_KEY

# Kronos environment
KRONOS_SCORING_REGION=$CUSTOM_ECOSYSTEM  # <maven/pypi/npm>
DEPLOYMENT_PREFIX=$DEPL_PREFIX  # <dev/stage/prod>
GREMLIN_REST_URL=$GREMLIN_URL  # <url>:<port>
EOF
```

Deploy with docker-compose
```bash
docker-compose build
docker-compose up
```

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
```json
[
    {
        "alternate_packages": {
            "io.vertx:vertx-core": [
                {
                    "package_name": "io.netty:netty-codec-http",
                    "similarity_score": 1,
                    "topic_list": [
                        "http",
                        "network",
                        "netty",
                        "socket"
                    ]
                }
            ],
            "io.vertx:vertx-web": [
                {
                    "package_name": "org.jspare:jspare-core",
                    "similarity_score": 1,
                    "topic_list": [
                        "framework",
                        "webapp"
                    ]
                }
            ]
        },
        "companion_packages": [
            {
                "cooccurrence_count": 219,
                "cooccurrence_probability": 83.26996197718631,
                "package_name": "org.slf4j:slf4j-api",
                "topic_list": [
                    "logging",
                    "dependency-injection",
                    "api"
                ]
            },
            {
                "cooccurrence_count": 205,
                "cooccurrence_probability": 77.9467680608365,
                "package_name": "org.apache.logging.log4j:log4j-core",
                "topic_list": [
                    "logging",
                    "java"
                ]
            },
            {
                "cooccurrence_count": 208,
                "cooccurrence_probability": 79.08745247148289,
                "package_name": "io.vertx:vertx-web-client",
                "topic_list": [
                    "http",
                    "http-request",
                    "vertx-web-client",
                    "http-response"
                ]
            }
        ],
        "ecosystem": "maven",
        "missing_packages": [],
        "outlier_package_list": [
            {
                "outlier_prbability": 0.99789845842189628,
                "package_name": "io.vertx:vertx-core",
                "topic_list": [
                    "http",
                    "socket",
                    "tcp",
                    "reactive"
                ]
            },
            {
                "outlier_prbability": 0.99585300969280544,
                "package_name": "io.vertx:vertx-web",
                "topic_list": [
                    "vertx-web",
                    "webapp",
                    "auth",
                    "routing"
                ]
            }
        ],
        "package_to_topic_dict": {
            "io.vertx:vertx-core": [
                "http",
                "socket",
                "tcp",
                "reactive"
            ],
            "io.vertx:vertx-web": [
                "vertx-web",
                "webapp",
                "auth",
                "routing"
            ]
        },
        "user_persona": "1"
    }
]
```


## Latest Depolyment

* Maven
	* Retrained on: 2018-01-02 22:30 (UTC+5:30)
