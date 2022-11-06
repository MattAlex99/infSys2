import java.util

case class StripedArticle(id:Long,
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

object StripedArticle{
  def stripArticle(article:Article): StripedArticle ={
    return new StripedArticle(article.id, article.title, article.year, article.n_citation,article.doc_type,
      article.page_start,article.page_end,article.publisher,article.volume,article.issue,article.doi,
      article.references
    )
  }
}