"""
A Python script to run an autoML model for the existing data.

To run the AutoML system, we rely on an *ad hoc* virtual environnement created with conda. It can be activated with:
```
conda activate predi-veli-ml-venv
```

"""

import datetime
from pathlib import Path
import pandas as pd

import autosklearn.regression
from autosklearn.metrics import median_absolute_error as mdae
import joblib

def main():
    save_path = (Path()/"model/basic_model").resolve()

    X_train = pd.read_csv(save_path/"features_train.csv", index_col="date")
    y_train = pd.read_csv(save_path/"target_train.csv", index_col="date")

    X_test = pd.read_csv(save_path/"features_test.csv", index_col="date")
    y_test = pd.read_csv(save_path/"target_test.csv", index_col="date")

    # in [s]
    TIME_BUDGET = 1800

    automl = autosklearn.regression.AutoSklearnRegressor(
        time_left_for_this_task=TIME_BUDGET,
        metric=mdae,
        n_jobs=-1,
        tmp_folder="/tmp/autosklearn_regression_example_tmp"
    )

    automl.fit(X_train, y_train)

    now = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    model_name = "current-classifier_{}".format(now)
    joblib.dump(automl, save_path/model_name)

    y_pred = automl.predict(X_test)

    score=mdae(y_test, y_pred)
    print(score)

    show_modes_str=automl.show_models()
    #sprint_statistics_str = automl.sprint_statistics()

    print(show_modes_str)
    print(automl.leaderboard())

# Needed for parallelization
if __name__ == "__main__":
    main()