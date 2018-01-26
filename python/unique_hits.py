import pandas as pd
import category_encoders as ce
from sklearn.cross_validation import train_test_split
from sklearn.tree import DecisionTreeRegressor
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error

dataframe = pd.read_csv('data/ml_50k.csv', header=None, names=['ip', 'hour', 'path', 'fields', 'session_so_far',
                                                   'unique_hits_so_far', 'session_min', 'session_sec', 'unique_hits'],
                        dtype={'ip': 'object', 'hour': 'object', 'path': 'object', 'fields': 'object', 'session_so_far': float,
                               'unique_hits_so_far': float, 'session_min': float, 'session_sec': float, 'unique_hits': float}
            )

# hashing trick
encoder = ce.HashingEncoder(cols=['ip', 'hour', 'path', 'fields'], n_components=12)

df_transformed = encoder.fit_transform(dataframe)

vals = df_transformed.values

# ip, hour, path, fields, session_so_far, unique_hits_so_far
X = vals[:, 0:-3]

# unique hits
y = vals[:, -1]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=100)

# chose 12. needs something like grid search cv to get a good number
dtr = DecisionTreeRegressor(max_depth=12)

dtr.fit(X_train, y_train)

y_pred = dtr.predict(X_test)

# simple metrics
mae = mean_absolute_error(y_test, y_pred)
print('mean_absolute error: %s' % mae)

plt.plot(y_pred[:100], label='prediction')
plt.plot(y_test[:100], label='reality')
plt.legend()
plt.show()