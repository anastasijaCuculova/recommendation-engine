import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RecommendationEngine {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val booksDataSet = DataInitializer.loadBooks(session, sqlContext)
    val usersDataSet = DataInitializer.loadUsers(session, sqlContext)
    val userReviewsDataSet = DataInitializer.loadUserReviews(session, sqlContext)

    /**
      * Group reviews by product key
      */

    val reviewsGroupedByProduct = userReviewsDataSet.rdd.groupBy(userReview => userReview.asin)

    /**
      * Group reviews by user id
      */

    val reviewsGroupedByUser = userReviewsDataSet.rdd.groupBy(userReview => userReview.reviewerID)

    /** *
      * Get the max, min reviews along with the count of users who have
      * // rated a book.
      */

    //    val ratingsDF: Nothing = sqlContext.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu  "
    //      + " from(SELECT ratings.product, max(ratings.rating) as maxr, " + " min(ratings.rating) as minr,count(distinct user) as cntu  "
    //      + " FROM ratings group by ratings.product ) movierates " + " join movies on movierates.product=movies.movieId " + " order by movierates.cntu desc ")

    System.out.println("Total number of users who rated books   : " + reviewsGroupedByUser.count)
    System.out.println("Total number of books rated   : " + reviewsGroupedByProduct.count)

    System.out.println("=== Print out data sets ===")
    System.out.println("=== User reviews ===")
    //  userReviewsDataSet.show(20)

    System.out.println("=== Users ===")
    //  booksDataSet.show(20)

    System.out.println("=== Books ===")
    //  usersDataSet.show(20)



    PredictionUseCases.createTrainingAndTestSplits(userReviewsDataSet.rdd)
    Thread.sleep(30000)
  }
}
