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
# transformVec = pca.transform(featureVectors)

# fig = plt.figure()
# ax = Axes3D(fig)

# ax.scatter([transformVec[i][0] for i in range(len(transformVec))], [transformVec[i][1] for i in range(len(transformVec))], targetVectors)

# plt.show()


neigh = KNeighborsRegressor(n_neighbors = 3)

trainX = featureVectors[0:int(len(featureVectors)*0.8)]
testX = featureVectors[int(len(featureVectors)*0.8):]

trainY = targetVectors[0:int(len(targetVectors)*0.8)]
testY = targetVectors[int(len(targetVectors)*0.8):]

neigh.fit(trainX, trainY)

pErrors = []
for i in range(len(testX)):
	pErrors.append(abs(neigh.predict(testX[i])[0]-testY[i])/testY[i])


print np.mean(pErrors)
