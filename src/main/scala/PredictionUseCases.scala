import models.UserReview
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

object PredictionUseCases {


  def createTrainingAndTestSplits(reviews: RDD[UserReview], sc: SparkContext) {

    val splits = reviews.randomSplit(Array[Double](0.8, 0.2))

    val trainingUserReviewsRDD = splits(0).cache
    val testUserReviewsRDD = splits(1).cache

    val numOfTrainingReviews = trainingUserReviewsRDD.count()
    val numOfTestingReviews = testUserReviewsRDD.count()

    System.out.println("Number of training reviews : " + numOfTrainingReviews)
    System.out.println("Number of testing reviews: " + numOfTestingReviews)

    val trainingReviewsRDD = createReviewModel(trainingUserReviewsRDD, sc)
    val testReviewsRDD = createReviewModel(testUserReviewsRDD, sc)

    prepareALS(trainingReviewsRDD)
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
      * Alternate Least Squares // Build the recommendation model using ALS on the training data
      * **/

    val model = new ALS()
      .setIterations(numIterations)
      .setBlocks(block)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank).setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(trainingReviewsRDD)
  }

  private def createReviewModel(reviewsRDD: RDD[UserReview], sc: SparkContext): RDD[Rating] = {
    //val reviews: RDD[Review] = sc.emptyRDD


    /* reviewsRDD.map(review => {
       val userId = userIdToInt.lookup(review.reviewerID).head.toInt

       /** Well, you cannot access another RDD from a transformation. That is not allowed. **/
       val bookId = bookIdToInt.lookup(review.asin).head.toInt
       val rating = review.overall
       Rating(userId, bookId, rating.toFloat)
     })*/
    null
  }

}
