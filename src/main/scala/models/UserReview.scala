package models

import org.apache.commons.net.ntp.TimeStamp

case class UserReview(userId: String,
                      bookId: String,
                      reviewText: String,
                      overall: Double,
                      summary: String,
                      unixReviewTime: Double,
                      reviewTime: TimeStamp)