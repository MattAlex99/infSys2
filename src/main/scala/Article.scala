import com.google.gson.Gson

import java.util

case class Article(id:Long,
                   authors:util.ArrayList[Author],
                   title:String,
                   year:Int, //
                   n_citation:Int, //
                   doc_type:String,
                   page_start:String,
                   page_end:String,
                   publisher:String,
                   volume:String,
                   issue:String,
                   doi:String,
                   references: Array[Long]
               )


object Article {
  val gson = new Gson

  def stringToArticle(articleString:String)={
    val rawLineString=articleString.replaceAll("[^\\x00-\\x7F]", "?")
    rawLineString match {
      case "]" =>
      case "[" =>
      case _ => val clearedLineString = removeLeadingComma(rawLineString)
        val currentArticle: Article = gson.fromJson(clearedLineString, classOf[Article])
    }
  }


  final private def removeLeadingComma(string: String): String = {
    string.take(1) match {
      case "," => removeLeadingComma(string.substring(1))
      case _ => string
    }
  }
}