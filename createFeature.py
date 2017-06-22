import sys
import glob
import os
import csv
import re
import sklearn


csv.field_size_limit(sys.maxsize)

def getTrainingData():
	"""assumes impala profiles are in cwd/profiles"""
	# pathProf = os.getcwd() + '/profiles/*'
	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_5_51_58.csv"
	result = []

	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_6_33_25.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_6_45_51.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_5_44_33.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})


	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_6_27_55.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_7_49_40.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	pathProf = os.getcwd() + "/profiles/impala_profiles_2_24_2017_5_17_6.csv"
	for filename in glob.glob(pathProf):
		partialResult = extractFeature(filename)
		for lst in partialResult:
			result.append({lst[0]:lst[1]})

	



	appendRuntime(os.getcwd() + '/queries/impala_queries_2_24_2017.csv', result)


	return result



def extractFeature(impalaProf):
	"""impalaProf := file name of impala profile
		fields := vector of field names to extract; if None, assumes all
		returns list of dictionaries with attributes

	"""

	result = []

	with open(impalaProf, 'r') as f:
		reader = csv.DictReader(f)
		for row in reader:
			runtimeProf = row["compressedRuntimeProfile"]

			# tofindEnds = re.finditer(r"Query \(id=(.*?)\)", runtimeProf)
			# actualMatches = re.finditer(r"Query \(id=(.*?)\)", runtimeProf)

			# ends = []
			# for match in tofindEnds:
			# 	ends.append(match.end())


			# matchIndex = 0
			# for match in actualMatches:
			# 	queryCardinalDict = {}
			# 	currID = match.group(1)

			# 	if matchIndex < len(ends) -1:

			# 		toSearch = runtimeProf[match.end():ends[matchIndex+1]]

			# 	else:
			# 		toSearch = runtimeProf[match.end():]

			queryMatch = re.search(r"Query \(id=(.*?)\)", runtimeProf)
			# tableMatch = re.search(r"Operator", runtimeProf)

			queryCardinalDict = {}
			currID = queryMatch.group(1)
			toSearch = runtimeProf

			operations = re.finditer(r"(\d+):(AGGREGATE|SCAN HDFS|EXCHANGE|MERGING-EXCHANGE|TOP-N|UNION|SELECT|SORT|ANALYTIC|HASH JOIN|CROSS JOIN|NESTED LOOP JOIN).*?cardinality=(\d+)", toSearch,re.DOTALL)

			

			
			for op in operations:
				# print((op.group(1), op.group(2), op.group(3)))
				if op.group(2) in queryCardinalDict:
					queryCardinalDict[op.group(2)] += int(op.group(3))
					queryCardinalDict[op.group(2) + " COUNT"] += 1

				else:
					queryCardinalDict[op.group(2)] = int(op.group(3))
					queryCardinalDict[op.group(2) + " COUNT"] = 1

			result.append([currID, queryCardinalDict])

			# matchIndex += 1


	return result


def appendRuntime(impalaQueries, featureVecs):
	# print(featureVecs)
	with open(impalaQueries, 'r') as f:
		reader = csv.DictReader(f)
		for row in reader:

			for vec in featureVecs:
				#vec.keys()[0] just gets queryID name
				if row["queryId"] == vec.keys()[0]:
					duration = row["durationMillis"]
					vec[row["queryId"]]["durationMillis"] = int(duration)
									

def convertCSV(queryDict):

	#can we just learn on subset of unique queries
	#impala_profiles_2_24_2017_6_33_25.csv, impala_profiles_2_24_2017_5_51_58.csv has lots of operators
	possibleFeatures = ["AGGREGATE", "AGGREGATE COUNT", "SCAN HDFS", "SCAN HDFS COUNT", "EXCHANGE", "EXCHANGE COUNT", "MERGING-EXCHANGE", "MERGING-EXCHANGE COUNT", "TOP-N", "TOP-N COUNT", "UNION", "UNION COUNT", r"SELECT", r"SELECT COUNT", "SORT", "SORT COUNT", "ANALYTIC", "ANALYTIC COUNT", "HASH JOIN", "HASH JOIN COUNT", "CROSS JOIN", "CROSS JOIN COUNT", "NESTED LOOP JOIN", "NESTED LOOP JOIN COUNT", "durationMillis"]

	with open('trainingData.csv', 'wb') as f:
		w = csv.DictWriter(f, possibleFeatures)
		w.writeheader()
		for query in queryDict:
			toWrite = query.values()[0]

			w.writerow(toWrite)
			



# print(extractFeature("profiles/impala_profiles_2_24_2017_5_51_58.csv"))
td = getTrainingData()
print(td)
convertCSV(td)