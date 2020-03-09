import findspark, os, shutil, csv 
findspark.init()

from pyspark import SparkContext, SparkConf

businessPath = "./Data/yelp_businesses.csv"
reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"
friendshipPath = "./Data/yelp_top_users_friendship_graph.csv"

RESULT_PATH = "results/task_1"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

businessTextFile = sc.textFile(businessPath)
reviewersTextFile = sc.textFile(reviewersPath)
friendshipTextFile = sc.textFile(friendshipPath)

# Subtask a)
businessLines = businessTextFile.map(lambda line: line.split("\t")).count()
reviewersLines = reviewersTextFile.map(lambda line: line.split("\t")).count()
friendshipLines = friendshipTextFile.map(lambda line: line.split(",")).count()
print("done[x]")

#print("Businesslines:", businessLines) #        192 610
#print("Reviewerslines:", reviewersLines) #      883 738
#print("Friendshiplines:", friendshipLines) #    1 938 473


# Saving the results as csv
with open(RESULT_PATH + "/task_a/output.csv", "w") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow([str(businessLines)])
    writer.writerow([str(reviewersLines)])
    writer.writerow([str(friendshipLines)])
