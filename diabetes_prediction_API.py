import diabetes_predicition_API_logic as d
from flask import Flask, request, make_response, jsonify
import json

logic = d.api()
app1 = Flask(__name__)


@app1.route('/patient-record', methods=["POST"])
def addRecord():
    ret = {}
    sample = request.get_json()
    patRec, hashedID = logic.getPrediction(sample)
    ret["patient_ID"] = hashedID
    return json.dumps(ret)


@app1.route('/patient-prediction/<patID>', methods=["GET"])
def predication(patID):
    print(patID)
    ret = {}
    pred = logic.predictions[patID]
    ret["diabetes_propability"] = pred
    return json.dumps(ret)


@app1.route('/model', methods=["PUT"])
def updateModel():
    ret = {}
    model_data = request.get_json()
    file_name = model_data["new_model_file_name"]
    result = logic.updateModel(file_name)
    ret["model_loading_result"] = result
    return json.dumps(ret)


if __name__ == '__main__':
    app1.run(port=9875)