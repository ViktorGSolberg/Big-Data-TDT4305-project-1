import findspark, os, shutil, csv

findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import desc

businessPath = "./Data/yelp_businesses.csv"
reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"
friendshipPath = "./Data/yelp_top_users_friendship_graph.csv"

RESULT_PATH_1 = "results/task_5"
RESULT_PATH_2 = "results/task_6"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# Task 5

print("-------- (5a) Load each file in the dataset into separate DataFrames: --------")

sqlContext = SQLContext(sc)
businessDF = sqlContext.read.csv(businessPath, header=True, sep="\t")
reviewersDF = sqlContext.read.csv(reviewersPath, header=True, sep="\t")
friendshipDF = sqlContext.read.csv(friendshipPath, header=True, sep=",")

# a) Load each file in the dataset into separate DataFrames.

businessDf = businessDF.toDF("business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "categories")
reviewersDF = reviewersDF.toDF("review_id", "user_id", "business_id", "review_text", "review_date")
friendshipDF = friendshipDF.toDF("src_user_id", "dst_user_id")

businessDF.createOrReplaceTempView("businesses")
reviewersDF.createOrReplaceTempView("reviewers")
friendshipDF.createOrReplaceTempView("friendshipGraph")

# Just to show that task 5 a) works:
b = sqlContext.sql("select count(*) from businesses")
r = sqlContext.sql("select count(*) from reviewers")
f = sqlContext.sql("select count(*) from friendshipGraph")
print("done [x]")


# Task 6

print("-------- Task (6) --------")

# a) Inner join review table and business table on business_id
# b) The result is saved in a temporary variable, "innerJoin"
innerJoin = businessDF.join(reviewersDF, "business_id")
# innerJoin.show()

# c) Number of reviews for each user in the review table for top 20 users with the most number of reviews
reviews = reviewersDF.groupBy("user_id").count().sort(desc("count")).limit(20)


# Saving the results as csv:

if os.path.isdir(RESULT_PATH_1):
    shutil.rmtree(RESULT_PATH_1)

if os.path.isdir(RESULT_PATH_2):
    shutil.rmtree(RESULT_PATH_2)

b.write.csv(RESULT_PATH_1 + "/task_a/output1.csv")
r.write.csv(RESULT_PATH_1 + "/task_a/output2.csv")
f.write.csv(RESULT_PATH_1 + "/task_a/output3.csv")

innerJoin.write.csv(RESULT_PATH_2 + "/task_a_and_b/output.csv")
reviews.write.csv(RESULT_PATH_2 + "/task_c/output.csv")