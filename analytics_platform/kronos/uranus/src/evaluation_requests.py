from uuid import uuid1
import json


def submit_evaluation(training_data_url):
    """Evaluate the test data for kronos.

    :param training_data_url: Location where test data is loaded from.

    :return: The evalution id generated for this request."""
    evaluation_id = str(uuid1())
    with open("/tmp/queue.json", "r") as f:
        queue = json.load(f)
    queue.append(evaluation_id)
    with open("/tmp/queue.json", "w") as f:
        json.dump(queue, f)
    with open("/tmp/request_dict.json", "r") as f:
        request_dict = json.load(f)
    request_dict[evaluation_id] = {"input": training_data_url}
    with open("/tmp/request_dict.json", "w") as f:
        json.dump(request_dict, f)
    return {"Kronos Evaluation started with id": evaluation_id}


def get_evalution_result(evaluation_id):
    """Return the evaluation results if available for the given id.

    :param evaluation_id: The id against which the results are needed."""
    with open("/tmp/request_dict.json", "r") as f:
        request_dict = json.load(f)
    result = request_dict.get(evaluation_id)
    if result is None:
        return {"Error": "Invalid evalution id! Try again."}
    elif result.get("output") is None:
        return {"Wait": "Evalution still in progress"}
    else:
        return {"Evalutaion metric": result.get("output")}
