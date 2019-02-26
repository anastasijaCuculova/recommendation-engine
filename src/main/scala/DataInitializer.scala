import models.{Book, User, UserReview}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}


object DataInitializer {

  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
  val BOOKS_REVIEWS_PATH = "C:\\Users\\AnastasiaCuculova\\Downloads\\reviews_Books_5.json\\Books_5.json"
  val BOOKS_PATH = "C:\\Users\\AnastasiaCuculova\\Documents\\Books.txt"

  def loadUsers(session: SparkSession, sc: SQLContext): Dataset[User] = {
    //    val lines = sc.textFile(CSV_PATH)
    //    lines
    //      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
    //      .map(line => {
    //        val splits = line.split(COMMA_DELIMITER, -1)
    //        User(splits(1), splits(2))
    //      })
    val dataFrameReader = session.read
    val users = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .json(BOOKS_REVIEWS_PATH)


    var dataFrameFilterColumns = users.select("reviewerId", "reviewerName")

    import session.implicits._
    var usersDataSet = dataFrameFilterColumns.as[User]

    dataFrameFilterColumns = users.select("reviewerId", "reviewerName")
    usersDataSet = dataFrameFilterColumns.as[User]
    usersDataSet
  }

  def loadBooks(session: SparkSession, sc: SQLContext): Dataset[Book] = {
    //    val lines = sc.textFile(CSV_PATH)
    //    lines
    //      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
    //      .map(line => {
    //        val splits = line.split(COMMA_DELIMITER, -1)
    //        Book(splits(0))
    //      })
    val dataFrameReader = session.read
    val books = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .json(BOOKS_REVIEWS_PATH)


    val dataFrameFilterColumns = books.select("asin")
    import session.implicits._
    val booksDataSet = dataFrameFilterColumns.as[Book]
    booksDataSet
  }

  def loadUserReviews(session: SparkSession, sc: SQLContext): Dataset[UserReview] = {
    //    val lines = sc.textFile(CSV_PATH)
    //    lines
    //      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
    //      .map(line => {
    //        val splits = line.split(COMMA_DELIMITER, -1)
    //        UserReview(splits(0), splits(1), splits(3), Utils.toInt(splits(4)).get, splits(5), Utils.toInt(splits(6)).get, splits(7))
    //      })
    val dataFrameReader = session.read
    val userReviews = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value = true)
      .json(BOOKS_REVIEWS_PATH)
    val userReviewFilterColumns = userReviews.select("overall", "reviewerId", "asin", "reviewText", "reviewTime", "summary", "unixReviewTime")

    import session.implicits._
    val userReviewsDataSet = userReviewFilterColumns.as[UserReview]
    userReviewsDataSet
  }
}