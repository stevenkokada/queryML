from keras.models import Sequential
from keras.layers import Dense, Activation


import csv
import numpy as np
import sklearn
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn.neighbors import KNeighborsRegressor
from sklearn.decomposition import PCA

def csvToVector(csvPath):
	featureVectors = []
	targetVectors = []



	with open(csvPath) as f:
		reader = csv.DictReader(f)
		for row in reader:
			fVec = []
			for key in row.keys():
				if key != "durationMillis":
					if row[key] == "":
						fVec.append(0)

					else:
						fVec.append(int(row[key]))

				else:
					targetVectors.append(int(row[key]))



			featureVectors.append(fVec)

	return featureVectors, targetVectors

featureVectors, targetVectors = csvToVector("trainingData.csv")

# filter out non-trivial vectors
combined = zip(featureVectors, targetVectors)
featureVectors, targetVectors = zip(*filter(lambda x: sum(x[0]) > 0, combined))



scaler = sklearn.preprocessing.StandardScaler().fit(featureVectors)
featureVectors = scaler.transform(featureVectors)

# plot PCA queries
# pca = PCA(n_components = 2)
# pca.fit(featureVectors)
# featureVectors = pca.transform(featureVectors)

# fig = plt.figure()
# ax = Axes3D(fig)

# ax.scatter([featureVectors[i][0] for i in range(len(featureVectors))], [featureVectors[i][1] for i in range(len(featureVectors))], targetVectors)

# plt.show()


trainX = featureVectors[0:int(len(featureVectors)*0.8)]
testX = featureVectors[int(len(featureVectors)*0.8):]

trainY = targetVectors[0:int(len(targetVectors)*0.8)]
testY = targetVectors[int(len(targetVectors)*0.8):]

trainX = np.array(trainX)
testX = np.array(testX)
trainY = np.array(trainY)
testY = np.array(testY)



model = Sequential()

model.add(Dense(units = 10, input_dim=24))
model.add(Activation('relu'))
model.add(Dense(units=5))
model.add(Activation('relu'))
model.add(Dense(units=1))
model.add(Activation('linear'))

model.compile(loss="mean_absolute_percentage_error", optimizer = 'sgd')
# model.fit(trainX, trainY, epochs = 300, batch_size = 100)
hist = model.fit(trainX, trainY, validation_split = 0.2, epochs = 300, batch_size = 100)
losses = hist.history["loss"]

plt.scatter([x for x in range(len(losses))], losses)
plt.show()

loss_and_metrics = model.evaluate(testX, testY, batch_size=32)


print loss_and_metrics
