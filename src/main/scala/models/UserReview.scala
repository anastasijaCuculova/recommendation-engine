package models

case class UserReview(overall: Double,
                      reviewerId: String,
                      asin: String,
                      reviewText: String,
                      reviewTime: String,
                      summary: String,
                      unixReviewTime: Double) {

  private var userId = -1
  private var bookId = -1

  def setUserId(userId: Int) {
    this.userId = userId
  }

  def getUserId: Int = {
    userId
  }

  def setBookId(bookId: Int) {
    this.bookId = bookId
  }

  def getBookId: Int = {
    bookId
  }
}