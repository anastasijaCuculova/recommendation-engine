import models.{Book, User, UserReview}
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Utils

object DataInitializer {

  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  def loadBooks(sc: SparkContext): RDD[Book] = {
    val lines = sc.textFile("C:\\diplomska\\podatoci\\item_dedup.csv")
    lines
      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(COMMA_DELIMITER, -1)
        Book(splits(0))
      })
  }

  def loadUsers(sc: SparkContext): RDD[User] = {
    val lines = sc.textFile("C:\\diplomska\\podatoci\\item_dedup.csv")
    lines
      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(COMMA_DELIMITER, -1)
        User(splits(1), splits(2))
      })
  }

  def loadUserReviews(sc: SparkContext): RDD[UserReview] = {
    val lines = sc.textFile("C:\\diplomska\\podatoci\\item_dedup.csv")
    lines
      .filter(line => !line.split(COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(COMMA_DELIMITER, -1)
        UserReview(splits(0), splits(1), splits(3), Utils.toInt(splits(4)).get, splits(5), Utils.toInt(splits(6)).get, new TimeStamp(splits(7)))
      })
  }
}