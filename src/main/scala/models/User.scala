package models

case class User(reviewerID: String, reviewerName: String) {
  private var id = -1

  def setId(id: Int) {
    this.id = id
  }

  def getId: Int = {
    id
  }
}

