import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RecommendationEngine {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    //Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //    val booksDataFrame = sqlContext.read.format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306/recommendation_engine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
    //      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "books")
    //      .option("user", "root")
    //      .option("password", "admin").load()
    //
    //    booksDataFrame.show()


    import session.implicits._

    val booksDataSet = DataInitializer.loadBooks(session, sqlContext)
    val usersDataSet = DataInitializer.loadUsers(session, sqlContext)
    val userReviewsDataSet = DataInitializer.loadUserReviews(session, sqlContext)
    /*

        // to group later
        userReviewsDataSet.filter(userReview => !userReview.reviewerID.isEmpty).groupBy("").count()

    */

    System.out.println("=== Print out data sets ===")
    System.out.println("=== User reviews ===")
    userReviewsDataSet.show(20)

    System.out.println("=== Users ===")
    booksDataSet.show(20)

    System.out.println("=== Books ===")
    usersDataSet.show(20)

    Thread.sleep(30000)
  }
}
