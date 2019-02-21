package models

case class Book(asin: String) {
  private var id = -1

  def setId(id: Int) {
    this.id = id
  }

  def getId: Int = {
    id
  }
}