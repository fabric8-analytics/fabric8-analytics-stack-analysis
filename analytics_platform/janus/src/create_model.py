from __future__ import division
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline


def kmeans_estimator(raw_df=None, transformers=[], min_k=2, max_k=10):
    estimator_models = list()
    max_k = max_k + 1
    for k in range(min_k, max_k):
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features").setPredictionCol("prediction")
        net_tranform = transformers + [kmeans]
        net_pipeline = Pipeline(stages=net_tranform)
        model_k = net_pipeline.fit(raw_df)
        estimator_models.append(model_k)
    return estimator_models


def compute_kmeans_cost(clean_df=None, estimator_models=[]):
    cost = list()
    for e in estimator_models:
        c = e.stages[-1].computeCost(clean_df)
        cost.append(c)
    return cost


def get_best_model(cost=[], estimator_models=[], min_k=2):
    least_cost_k = cost.index(min(cost))
    optimal_k = least_cost_k + min_k
    user_persona_pipeline = estimator_models[least_cost_k]
    print("Optimal value of k = %s" % str(optimal_k))
    return user_persona_pipeline


def model_kmeans(raw_df=None, clean_df=None, transformers=[], min_k=2, max_k=10):
    estimator_models  = kmeans_estimator(raw_df, transformers, min_k, max_k)
    cost_metric = compute_kmeans_cost(clean_df, estimator_models)
    user_persona_pipeline = get_best_model(cost_metric, estimator_models, min_k)
    return user_persona_pipeline