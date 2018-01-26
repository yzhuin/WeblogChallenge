import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM


def parse(date_string):
    return pd.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')


# for times series prediction, we need to use previous load info as input
def prepare_data(data, shift):
    df = pd.DataFrame(data)
    # input
    cols = [df.shift(i) for i in range(1, shift+1)]
    # output
    cols.append(df)
    # stack to the right
    df = pd.concat(cols, axis=1)
    df.fillna(0, inplace=True)
    return df.values


# scale
def scale(train, test):
    scaler = MinMaxScaler(feature_range=(-1, 1));
    # fit scaler
    scaler = scaler.fit(train)
    # transform
    train = train.reshape(train.shape[0], train.shape[1])
    train_scaled = scaler.transform(train)
    test = test.reshape(test.shape[0], test.shape[1])
    test_scaled = scaler.transform(test)
    return scaler, train_scaled, test_scaled


def fit(train, batch_size, epoch, num_neurons):
    X, y = train[:, 0:-1], train[:, -1]
    print(X.shape)
    # 3d array
    X = X.reshape(X.shape[0], 1, X.shape[1])
    print(X.shape)
    model = Sequential()
    model.add(LSTM(num_neurons, batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True))
    model.add(Dense(1))
    model.compile(loss="mean_squared_error", optimizer='adam')
    for i in range(epoch):
        model.fit(X, y, epochs=1, batch_size=batch_size, verbose=0, shuffle=False)
        model.reset_states()
    return model


def predict(model, batch_size, X):
    # model.predict requires
    X = X.reshape(1, 1, len(X))
    y_ = model.predict(X, batch_size=batch_size)
    return y_[0,0]


series = pd.read_csv('data/loads.txt', header=None, names=['minute', 'load'], parse_dates=[0], index_col=0, squeeze=True, date_parser=parse)
print(series.head())
size = series.size
raw = series.values

# use just one step
prepared = prepare_data(raw, 1)
train, test = prepared[0:-int(size/3)], prepared[-int(size/3):]

scaler, train_scaled, test_scaled = scale(train, test)

# build up state for prediction
lstm_model = fit(train_scaled, 1, 100, 1)

train_reshaped = train_scaled[:, 0].reshape(len(train_scaled), 1, 1)

lstm_model.predict(train_reshaped, batch_size=1)

predicted = []
for i in range(len(test_scaled) - 1):
    X, y = test_scaled[i, 0:-1], test_scaled[i, -1]
    y_ = predict(lstm_model, 1, X)
    predicted.append(y_)
    actual_y = raw[len(train) + i + 1]


plt.plot(raw[-int(size/3):])
plt.plot(predicted)
plt.show()


