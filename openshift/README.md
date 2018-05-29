# Deployment

There are two OpenShift template files in this directory:

* `template.yaml` - template for development purposes. It instantiates Kronos as a Pod with `restartPolicy` set to `Never`. This is needed as the service can be unstable and restarts can be expensive. Developers can recreate the Pod, if needed.
`deploy.sh` script can be used to deploy the service.

* `template-prod.yaml` - template used by [saas-herder](https://github.com/openshiftio/saasherder) to deploy the service to staging and production.
