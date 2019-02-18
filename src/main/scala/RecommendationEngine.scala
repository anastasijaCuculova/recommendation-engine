import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RecommendationEngine {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val booksDataFrame = sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/recommendation_engine?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "books")
      .option("user", "root")
      .option("password", "admin").load()

    booksDataFrame.show()


    import session.implicits._

    val booksDataSet = DataInitializer.loadBooks(sc).toDS()
    val usersDataSet = DataInitializer.loadUsers(sc).toDS()
    val userReviewsDataSet = DataInitializer.loadUserReviews(sc).toDS()

    System.out.println("=== Print out schema ===")
    booksDataSet.printSchema()

    System.out.println("=== Print 20 records of books & users table ===")
    booksDataSet.show(20)
    usersDataSet.show(20)
    userReviewsDataSet.show(20)
  }
}