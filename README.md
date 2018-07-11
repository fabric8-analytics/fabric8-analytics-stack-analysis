# Stack Analysis 

[![Build Status](https://ci.centos.org/buildStatus/icon?job=devtools-fabric8-analytics-stack-analysis-f8a-build-master)](https://ci.centos.org/job/devtools-fabric8-analytics-stack-analysis-f8a-build-master/)


## List of models currently present in the analytics platform


* [Gnosis](/analytics_platform/kronos/gnosis)
* [Pgm](/analytics_platform/kronos/pgm)
* [Softnet](/analytics_platform/kronos/softnet)
* [Apollo](/analytics_platform/kronos/apollo)
* [Uranus](/analytics_platform/kronos/uranus)

## To Deploy Locally
Set up .env file with environment variables, i.e (view docker-compose.yml for possible values)
```bash
cat > .env <<-EOF
# Amazon AWS s3 credentials
AWS_S3_ACCESS_KEY_ID=
AWS_S3_SECRET_ACCESS_KEY=

# Kronos environment
KRONOS_SCORING_REGION=
DEPLOYMENT_PREFIX=
GREMLIN_REST_URL=

#Set Post Filtering
USE_FILTERS=
EOF
```

[data-model]: https://github.com/fabric8-analytics/fabric8-analytics-data-model/tree/master/local-setup
**NOTES:**\
Do *not* use any `[#]` comments or `['"]` in the .env file.\
For the `GREMLIN_REST_URL`, you can take a look at out [data-model]
and use the local-setup services
```bash
git clone https://github.com/fabric8-analytics/fabric8-analytics-data-model.git
cp -r fabric8-analytics-data-model/local-setup/scripts .
cp fabric8-analytics-data-model/local-setup/docker-compose.yml docker-compose-data-model.yml

# and in .env file
GREMLIN_REST_URL="http://localhost:8182"  # Note that the port is a port accessed from within the container
```
Otherwise you can use custom gremlin service

Deploy with docker-compose:\

```bash
docker-compose build
docker-compose -f docker-compose.yml -f docker-compose-data-model.yml up
```

## To Test Locally

`python -m unittest discover tests  -v`


## To Run Evaluation Script Locally

```bash
PYTHONPATH=`pwd` python evaluation_platform/uranus/src/kronos_offline_evaluation.py
```

## To Run Training Locally

```bash
PYTHONPATH=`pwd` python analytics_platform/kronos/src/kronos_offline_training.py
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


## Latest Deployment

* Maven
	* Retrained on: `2018-04-11 5:43 PM(IST)` with hyper-parameters:
        * `fp_min_support_count = 300`
        * `fp_intent_topic_count_threshold = 2`
        * `FP_TAG_INTENT_LIMIT = 4`
    * Used pomegranate version: `0.7.3`

### Footnotes

#### Coding standards

- You can use scripts `run-linter.sh` and `check-docstyle.sh` to check if the code follows [PEP 8](https://www.python.org/dev/peps/pep-0008/) and [PEP 257](https://www.python.org/dev/peps/pep-0257/) coding standards. These scripts can be run w/o any arguments:

```
./run-linter.sh
./check-docstyle.sh
```

The first script checks the indentation, line lengths, variable names, white space around operators etc. The second
script checks all documentation strings - its presence and format. Please fix any warnings and errors reported by these
scripts.

#### Code complexity measurement

The scripts `measure-cyclomatic-complexity.sh` and `measure-maintainability-index.sh` are used to measure code complexity. These scripts can be run w/o any arguments:

```
./measure-cyclomatic-complexity.sh
./measure-maintainability-index.sh
```

The first script measures cyclomatic complexity of all Python sources found in the repository. Please see [this table](https://radon.readthedocs.io/en/latest/commandline.html#the-cc-command) for further explanation how to comprehend the results.

The second script measures maintainability index of all Python sources found in the repository. Please see [the following link](https://radon.readthedocs.io/en/latest/commandline.html#the-mi-command) with explanation of this measurement.

#### Check for scripts written in BASH

The script named `check-bashscripts.sh` can be used to check all BASH scripts (in fact: all files with the `.sh` extension) for various possible issues, incompatibilies, and caveats. This script can be run w/o any arguments:

```
./check-bashscripts.sh
```

Please see [the following link](https://github.com/koalaman/shellcheck) for further explanation, how the ShellCheck works and which issues can be detected.

