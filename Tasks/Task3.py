import findspark, base64, os, shutil
findspark.init()
from operator import add

from pyspark import SparkContext, SparkConf

businessesPath = "./Data/yelp_businesses.csv"

RESULT_PATH = "results/task_3"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 3 a) What is the average rating for businesses in each city

def averageCityRating():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessesLinesRdd = textFile.map(lambda line: line.split('\t'))
    citiesAndRatings = businessesLinesRdd.map(lambda fields: (fields[3], float(fields[8])))
    avgRatings = citiesAndRatings.aggregateByKey((0, 0), lambda  a,b:(a[0] + b, a[1]+ 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: str(round(v[0]/v[1], 1)))
    return avgRatings

# Subtask b) What are the top 10 most frequent categories in the data?
def mostFrequentCategories():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFileWithoutHeaders = textFile.filter(lambda line: line != headers)
    arrayOfCategories = textFileWithoutHeaders.map(lambda line: line.split("\t")[10:])
    listOfCategoriesUnix = arrayOfCategories.flatMap(lambda list: list)
    arrayOfCategoriesAscii = listOfCategoriesUnix.map(lambda category: str(category)).map(lambda category: category.split(","))
    listOfCategoriesAscii = arrayOfCategoriesAscii.flatMap(lambda list: list)
    categoryFrequencies = listOfCategoriesAscii.map(lambda category: (category, 1)).reduceByKey(lambda a,b: a+b)
    topTenCategories = categoryFrequencies.takeOrdered(10, key = lambda x: -x[1])
    return topTenCategories


# Subtask c)
def postalCodeCentroids():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessLinesRdd = textFile.map(lambda line: line.split('\t'))
    codeLat = postalCodeLat(businessLinesRdd)
    codeLong = postalCodeLong(businessLinesRdd)
    avgLat = averageCoordinate(codeLat)
    avgLong = averageCoordinate(codeLong)
    centroids = avgLat.join(avgLong)
    return centroids

    
def postalCodeLat(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[6])))

def postalCodeLong(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[7])))

def averageCoordinate(postalCoordList):
    return postalCoordList.aggregateByKey((0,0), lambda a,b:(a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: round(v[0]/v[1], 3))

def toCSVLine(data):
    return '\t'.join(d.encode('utf-8') for d in data)

def tuplesToCSVLine(data):
    return '\t' .join(str(d) for d in data)


def main():
    print("-------- (3a) Average business-ratings for each city: --------")
    avgRatings = averageCityRating()
    print("done [x]")

    print("-------- (3b) Top 10 most frequent categories: --------")
    categories = mostFrequentCategories()
    print("done [x]")

    print("-------- (3c) Geographical centroids for businesses for each postal code: --------")
    centroids = postalCodeCentroids()
    print("done [x]")

    # Saving the results as csv
    if os.path.isdir(RESULT_PATH):
        shutil.rmtree(RESULT_PATH)
        
    avgRatings.map(toCSVLine).saveAsTextFile(RESULT_PATH + "/task_a/output.csv")
    sc.parallelize(categories).saveAsTextFile(RESULT_PATH + "/task_b/output.csv")
    centroids.map(tuplesToCSVLine).saveAsTextFile(RESULT_PATH + "/task_c/output.csv")


if __name__ == "__main__":
    main()
