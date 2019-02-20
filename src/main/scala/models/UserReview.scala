package models

case class UserReview(asin: String,
                      overall: Double,
                      reviewText: String,
                      reviewTime: String,
                      reviewerID: String,
                      summary: String,
                      unixReviewTime: Double)