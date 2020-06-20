import pandas as pd
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from joblib import dump, load
import matplotlib.pyplot as plt
import json

diabetes = pd.read_csv('diabetes.csv')
X = diabetes.loc[:, diabetes.columns != 'Outcome']
y = diabetes['Outcome']
X_train, X_test, y_train, y_test = train_test_split(X, y)
X_test.to_csv("X_test.csv", index=False)
y_test.to_csv("y_test.csv", index=False)

mlp = MLPClassifier()

MLPClassifier_parameter_space = {
    'hidden_layer_sizes': [(50,50,50), (50,100,50), (100,)],
    'activation': ['tanh', 'relu'],
    'solver': ['sgd', 'adam'],
    'alpha': [0.0001, 0.05],
    'learning_rate': ['constant','adaptive'],
}

dev_MLPClassifier_parameter_space = {
    'hidden_layer_sizes': [(50)],
    'activation': ['tanh'],
    'solver': ['adam'],
    'alpha': [0.05],
    'learning_rate': ['adaptive'],
}

MLPClassifier_parameter_space = dev_MLPClassifier_parameter_space

clf = GridSearchCV(mlp, MLPClassifier_parameter_space, n_jobs=1, cv=3)
clf.fit(X_train, y_train)
best_params = clf.best_params_
clf = MLPClassifier(**best_params)
#clf.fit(X, y)
clf.fit(X_train, y_train)
model_name = "diabetes_clf"
dump(clf, model_name + '.joblib')
clf = load(model_name + '.joblib')
predictions = clf.predict_proba(X_test)[:,0]
fpr, tpr, thresholds = metrics.roc_curve(y_test, predictions, pos_label=0)
AuROC = metrics.auc(fpr, tpr)

plt.figure()
plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % AuROC)
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Offline Receiver Operating Characteristic')
plt.legend(loc="lower right")
plt.show()

example_of_JSON_for_Postman_tests = json.dumps(X.tail(1).to_dict(orient="records")[0])
print("example_of_JSON_for_Postman_tests: ", example_of_JSON_for_Postman_tests)