''' The sklearn ElasticNet algorithm ran with forecast-forecast data. '''


import os
import time

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from sklearn.metrics import r2_score


repo = '/Users/mastacow/data/forecast-forecast'
directory = 'ETL/owm_direct'
data_file = 'instants.npy'
error_file = 'diffs.npy'
# Create the data objects from the saved numpy files.
data = os.path.join(repo, data_file)#directory, data_file)
target = os.path.join(repo, data_file)#directory, error_file)
X = np.load(data, allow_pickle=True)
y = np.load(target, allow_pickle=True)
X_df = pd.DataFrame(X)
y_df = pd.DataFrame(y)

# Remove any columns that can't be converted to float types
    # Try to convert DataFrame column by column and save the column names
    # for anyone that goes to except.
drops = []
for col in X_df.columns:
    try:
        X_df[col].astype(float)
    except:
        drops.append(col)
X_df = X_df.drop(columns=drops).astype(float)
y_df = y_df.drop(columns=drops).astype(float)
# Split data into training set and testing set.
X_train, X_test, y_train, y_test = train_test_split(X_df, y_df, test_size=.2)

# ElasticNet
alpha = 0.1
enet = ElasticNet(alpha=alpha, l1_ratio=0.7, tol=.1, max_iter=10000, normalize=True)
print(f"Running ElasticNet...this might take a while. Started at {time.ctime()}")
y_pred_enet = enet.fit(X_train, y_train).predict(X_test)
r2_score_enet = r2_score(y_test, y_pred_enet)
print(enet)
print("r^2 on test data : %f" % r2_score_enet)