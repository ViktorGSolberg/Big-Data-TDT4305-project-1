import findspark, base64, csv, os, shutil
from datetime import datetime

findspark.init()

from pyspark import SparkContext, SparkConf

reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"

RESULT_PATH = "results/task_2"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)


reviewersTextFile = sc.textFile(reviewersPath)

# Subtask a) How many distinct users are there in the dataset
def distinctUsers(reviewersTextFile):
    return reviewersTextFile.map(lambda line: line.split()[1]).distinct().count()

# Subtask b) What is the average number of the characters in a user review
def averageCharactersInReviews(reviewersTextFile):
    reviews = reviewersTextFile.map(lambda line: line.split()[3]).filter(lambda column: column != u'"review_text"')
    numberOfReviews = reviews.count()
    totalReviewsLength = reviews.map(lambda review: len(base64.b64decode(review))).reduce(lambda a, b: a + b)
    return totalReviewsLength / numberOfReviews

# Subtask c) What is the business_id of the top 10 businesses with the most number of reviews
def topTenBusinessesWithMostReviews(reviewersTextFile):
    reviewsPerBusiness = reviewersTextFile.map(lambda line: (line.split("\t")[2], 1)).reduceByKey(lambda a, b: a+b)
    topTenBusinesses = reviewsPerBusiness.takeOrdered(10, key = lambda x: -x[1])
    return topTenBusinesses

# Subtask d) Find the number of reviews per year
def timestampToYear(timestamp):
    return str(datetime.utcfromtimestamp(float(timestamp)))[0:4]

def numberofReviewsPerYear(reviewsTextFile):
    unixTimestampsWithHead = reviewsTextFile.map(lambda line: line.split()[4])
    head = unixTimestampsWithHead.first()
    unixTimestamps = unixTimestampsWithHead.filter(lambda timestamp: timestamp != head)
    reviewsPerYear = unixTimestamps.map(lambda stamp: (timestampToYear(stamp), 1)).reduceByKey(lambda a, b: a+b)
    return reviewsPerYear

# Subtask e) What is the time and date of the first and last review
def timeAndDateOfFirtAndLastReview(reviewsTextFile):
    headers = reviewsTextFile.first()
    textFile = reviewsTextFile.filter(lambda line: line != headers)
    reviewDates = textFile.map(lambda line: (line.split("\t")[4]))
    minTime = reviewDates.reduce(lambda time1, time2: time1 if time1<time2 else time2)
    maxTime = reviewDates.reduce(lambda time1, time2: time1 if time1>time2 else time2)
    return minTime, maxTime

# Subtask f) Calculate the PCC between the number of reviews by a user and the average number of
#            the characters in the users review.
def numberOfReviewsPerUser(reviewersTextFile):
    usersWithHead = reviewersTextFile.map(lambda user: user.split()[1])
    head = usersWithHead.first()
    users = usersWithHead.filter(lambda user: user != head)
    reviewsPerUser = users.map(lambda user: (user, 1)).reduceByKey(lambda a, b: a+b)
    return reviewsPerUser

def totalReviewsLengthPerUser(reviewersTextFile):
    reviewsAndUserWithHead = reviewersTextFile.map(lambda line: (line.split()[1], line.split()[3]))
    head = reviewsAndUserWithHead.first()
    reviewsAndUser = reviewsAndUserWithHead.filter(lambda reviewAndUser: reviewAndUser != head)
    reviewLengthPerUser = reviewsAndUser.map(lambda reviewAndUser: (reviewAndUser[0], len(base64.b64decode(reviewAndUser[1])))).reduceByKey(lambda a, b: a+b)
    return reviewLengthPerUser

def pearson_correlation_coefficient(reviewersTextFile):
    # 1) Find reviews per user 
    reviewsPerUser = numberOfReviewsPerUser(reviewersTextFile)

    # 2) Find the average number of reviews per user
    totalNumberOfUsers = reviewsPerUser.count()
    totalNumberOfReviews = reviewsPerUser.map(lambda x: x[1]).reduce(lambda a, b: a+b)
    avgNumberOfReviewsPerUser = totalNumberOfReviews / totalNumberOfUsers
    
    # 3) Find the average number of characters in all reviews 
    avgCharsInReviews = averageCharactersInReviews(reviewersTextFile)

    # 4) Find avg. chars in reviews per user and concatenate with no. of reviews per user
    totReviewsLengthPerUser = totalReviewsLengthPerUser(reviewersTextFile)
    newRdd = totReviewsLengthPerUser.join(reviewsPerUser)
    avgCharsInReviewsPerUser = newRdd.map(lambda userTotalReviewsAndChars: (
        (userTotalReviewsAndChars[1][0] / userTotalReviewsAndChars[1][1])-avgCharsInReviews, 
        userTotalReviewsAndChars[1][1]-avgNumberOfReviewsPerUser)
    )

    # 5) Calculate coefficient
    nominator = avgCharsInReviewsPerUser.map(lambda a: a[0]*a[1]).reduce(lambda a, b: a+b)
    denominator_sumX = avgCharsInReviewsPerUser.map(lambda a: a[1]**2).reduce(lambda a, b: a+b)
    denominator_sumY = avgCharsInReviewsPerUser.map(lambda a: a[0]**2).reduce(lambda a, b: a+b)
    denominator = (denominator_sumX**(.5)) * (denominator_sumY**(.5))
    return nominator/denominator

def toCSVLine(data):
    return '\t'.join(d.encode('utf-8') for d in data)

def tuplesToCSVLine(data):
    return '\t' .join(str(d) for d in data)

def main():

    print("-------- (2a) Number of distinct users: --------")
    numberOfDistinctUsers = distinctUsers(reviewersTextFile)
    #print(numberOfDistinctUsers) # 4522
    print("done[x]")

    print("-------- (2b) Average number of characters in a user review: --------")
    averageCharacters = averageCharactersInReviews(reviewersTextFile)
    #print(averageCharacters) # 857
    print("done[x]")

    print("-------- (2c) Business_id of top 10 businesses with most reviews: --------")
    topTenBusinesses = topTenBusinessesWithMostReviews(reviewersTextFile)
    #for business in topTenBusinesses:
    #    print(business[0], business[1])
    print("done[x]")

    print("-------- (2d) Number of reviews per year: --------")
    reviewsPerYear = numberofReviewsPerYear(reviewersTextFile)
    #print(reviewsPerYear.collect())
    print("done[x]")

    print("-------- (2e) Time and date of the first and last review: --------")
    firstReview, lastReview = timeAndDateOfFirtAndLastReview(reviewersTextFile)
    utc1 = str(datetime.utcfromtimestamp(float(firstReview)))
    utc2 = str(datetime.utcfromtimestamp(float(lastReview)))
    #print("First review date: ", utc1)
    #print("Last review date: ", utc2)
    print("done[x]")

    print("-------- (2f) Pearson correlation coefficient between no. of reviews and avg. no. of chars in reviews: --------")    
    corr = pearson_correlation_coefficient(reviewersTextFile)
    #print(corr) # 0.125036715364
    print("done[x]")  


    # Saving the results as csv
    with open(RESULT_PATH + "/task_a/output.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Number of distinct users: " + str(numberOfDistinctUsers)])
    
    with open(RESULT_PATH + "/task_b/output.csv","w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([str(averageCharacters)])

    if os.path.isdir(RESULT_PATH + "/task_c"):
        shutil.rmtree(RESULT_PATH + "/task_c")

    sc.parallelize(topTenBusinesses).saveAsTextFile(RESULT_PATH + "/task_c/output.csv")

    if os.path.isdir(RESULT_PATH + "/task_d"):
        shutil.rmtree(RESULT_PATH + "/task_d")
    
    sc.parallelize(reviewsPerYear.collect()).saveAsTextFile(RESULT_PATH + "/task_d/output.csv")

    with open(RESULT_PATH + "/task_e/output.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["First review: " + utc1])
        writer.writerow(["Last review: " + utc2])
    
    with open(RESULT_PATH + "/task_f/output.csv", "w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["PCC: " + str(corr)])



if __name__ == "__main__":
    main()
