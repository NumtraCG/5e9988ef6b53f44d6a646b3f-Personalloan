import json
import Connectors
import Transformations
import AutoML
try:
    Personalloan_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e9988ef6b53f44d6a646b40", spark, "{'url': '/Demo/PersonalLoanTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    Personalloan_AutoFE = Transformations.TransformationMain.run(["5e9988ef6b53f44d6a646b40"], {"5e9988ef6b53f44d6a646b40": Personalloan_DBFS}, "5e9988ef6b53f44d6a646b41", spark, json.dumps({"FE": [{"transformationsData": {}, "feature": "Age", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "45.38", "stddev": "11.35", "min": "23", "max": "67", "missing": "0"}}, {"transformationsData": {}, "feature": "Experience", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "20.15", "stddev": "11.39", "min": "-3", "max": "43", "missing": "0"}}, {"transformationsData": {}, "feature": "Income", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "73.59", "stddev": "45.63", "min": "8", "max": "205", "missing": "0"}}, {"transformationsData": {}, "feature": "ZIP_Code", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "93136.18", "stddev": "2442.32", "min": "9307", "max": "96651", "missing": "0"}}, {"transformationsData": {}, "feature": "Family", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "2.39", "stddev": "1.14", "min": "1", "max": "4", "missing": "0"}}, {"transformationsData": {}, "feature": "CCAvg", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2482", "mean": "1.92", "stddev": "1.73", "min": "0.0", "max": "8.8", "missing": "0"}, "transformation": ""}, {"transformationsData": {
    }, "feature": "Education", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "1.87", "stddev": "0.84", "min": "1", "max": "3", "missing": "0"}}, {"transformationsData": {}, "feature": "Mortgage", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "57.5", "stddev": "101.39", "min": "0", "max": "590", "missing": "0"}}, {"transformationsData": {}, "feature": "Securities_Account", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "0.1", "stddev": "0.3", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {}, "feature": "CD_Account", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "0.06", "stddev": "0.23", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {}, "feature": "Online", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "0.6", "stddev": "0.49", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {}, "feature": "CreditCard", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "0.29", "stddev": "0.45", "min": "0", "max": "1", "missing": "0"}}, {"transformationsData": {}, "feature": "Personal_Loan", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2482", "mean": "0.09", "stddev": "0.28", "min": "0", "max": "1", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionClassification(Personalloan_AutoFE, ["Age", "Experience", "Income", "ZIP_Code", "Family", "CCAvg",
                                                        "Education", "Mortgage", "Securities_Account", "CD_Account", "Online", "CreditCard"], "Personal_Loan")

except Exception as ex:
    print(ex)
