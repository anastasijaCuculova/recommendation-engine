import models.{Book, User, UserReview}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

object PredictionUseCases {


  def createTrainingAndTestSplits(reviews: RDD[UserReview], users: RDD[User], books: RDD[Book], sc: SparkContext) {

    val splits = reviews.randomSplit(Array[Double](0.8, 0.2))

    val trainingUserReviewsRDD = splits(0).cache
    val testUserReviewsRDD = splits(1).cache

    val numOfTrainingReviews = trainingUserReviewsRDD.count()
    val numOfTestingReviews = testUserReviewsRDD.count()

    System.out.println("Number of training reviews : " + numOfTrainingReviews)
    System.out.println("Number of testing reviews: " + numOfTestingReviews)

    val trainingRatingsRDD = createReviewModel(trainingUserReviewsRDD, users, books, sc)
    //val testReviewsRDD = createReviewModel(testUserReviewsRDD, users, books, sc)

    prepareALS(trainingRatingsRDD)
  }

  private def prepareALS(trainingReviewsRDD: RDD[Rating]): Unit = {
    val rank = 20
    val numIterations = 15
    val lambda = 0.10
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false

    /** References https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.ml.recommendation.ALS
      * Alternate Least Squares // Build the recommendation model using ALS on the training data **/

    val model = new ALS()
      .setIterations(numIterations)
      .setBlocks(block)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank).setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(trainingReviewsRDD)
  }

  private def createReviewModel(reviewsRDD: RDD[UserReview], users: RDD[User], books: RDD[Book], sc: SparkContext): RDD[Rating] = {
    var ratings: RDD[Rating] = sc.emptyRDD

    /** Assign unique Long id for each userId and bookId **/
    val userIdToInt = parseUserStringIds(sc, users)
    val bookIdToInt = parseBookStringIds(sc, books)


    val rdd1Broadcast = sc.broadcast(userIdToInt.collectAsMap())
    val rdd2Broadcast = sc.broadcast(bookIdToInt.collectAsMap())

    val joined = reviewsRDD.mapPartitions({ iter =>
      val m = rdd1Broadcast.value
      val m2 = rdd2Broadcast.value
      for {
        review <- iter
        if m.contains(review.reviewerId) && m2.contains(review.asin)
      } yield (review.overall, m2(review.asin), m(review.reviewerId))
    }, preservesPartitioning = true)

    ratings = joined.map(item => {
      Rating(item._2.toInt, item._3.toInt, item._1)
    })
    ratings
  }

  def parseUserStringIds(sc: SparkContext, reviewsRDD: RDD[User]): RDD[(String, Long)] = {
    reviewsRDD.map(_.reviewerID).distinct().zipWithUniqueId()
  }

  def parseBookStringIds(sc: SparkContext, reviewsRDD: RDD[Book]): RDD[(String, Long)] = {
    reviewsRDD.map(_.asin).distinct().zipWithUniqueId()
  }
}
