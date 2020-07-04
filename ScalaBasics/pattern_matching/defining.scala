package com.tekcrux.pattern_matching

object defining {
  def main(args: Array[String]) {
    case class Book(isbn: String)

    val frankenstein = Book("978-0486282114")
    println(frankenstein)
  }
}