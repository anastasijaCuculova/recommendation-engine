import models.UserReview
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.rdd.RDD

object PredictionUseCases {


  def createTrainingAndTestSplits(reviews: RDD[UserReview]) {

    val splits = reviews.randomSplit(Array[Double](0.8, 0.2))

    val trainingReviewsRDD = splits(0).cache
    val testReviewsRDD = splits(1).cache

    val numOfTrainingReviews = 0 //trainingRatingRDD.count
    val numOfTestingReviews = 0 //testRatingRDD.count

    System.out.println("Number of training reviews : " + numOfTrainingReviews)
    System.out.println("Number of testing reviews: " + numOfTestingReviews)

    // Build the recommendation model using ALS on the training data
    // References https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.ml.recommendation.ALS
    val als: ALS = new ALS()
    import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
    val model = als.setRank(20).setIterations(10).run(trainingReviewsRDD)
    null
  }


}
