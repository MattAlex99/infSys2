import com.google.gson.Gson
import redis.clients.jedis.Jedis

import java.util
import java.util.regex.Pattern
import java.util.stream.Collectors

class GetQuerryManager {
  val gson = new Gson()
  val jedis = new Jedis("127.0.0.1", 6379,100000)
  //Aufgabe 5
  def titleByID(articleID: Long): String ={
    jedis.get(nameManagement.getArticleIdToInformationKey(articleID))
  }
  def authors(articleID: Long): util.ArrayList[Author]={
    val authorsOfArticle = jedis.lrange("articleIdToAuthors"+articleID.toString,0,-1)
    val result = new util.ArrayList[Author]()
    authorsOfArticle.forEach(authorString =>result.add(gson.fromJson(authorString, classOf[AuthorCounting])) )
        return result
  }
  def articles(authorID: Long): util.ArrayList[Article] ={
    val result= new util.ArrayList[Article]()
    val articlesOfAuthor = jedis.lrange("authorToArticle:"+authorID.toString,0,-1)
    articlesOfAuthor.forEach(articleId => {
      val currentArticleString = jedis.get("articleIdToInformation"+articleId.toString)
      result.add(gson.fromJson(currentArticleString, classOf[Article]))
    })
    return result
  }
  def referencedBy(articleID: Long):util.ArrayList[Article]={
    val result = new util.ArrayList[Article]()
    val referencedArticles = jedis.lrange("ArticleReferencedBy:" + articleID.toString, 0, -1)
    referencedArticles.forEach(articleId => {
      val currentArticleString = jedis.get(nameManagement.getArticleIdToInformationKey(articleId))
      result.add(gson.fromJson(currentArticleString, classOf[Article]))
    })
    return result
  }

  def mostArticles():util.ArrayList[Author]={
    val result = new util.ArrayList[Author]()
    val authorStrings = jedis.lrange("authorsWithMostArticles", 0, -1)
    authorStrings.forEach(authorId => {
      val currentAuthorString = jedis.get((nameManagement.getAuthorKey(authorId)))
      val currentCountingAuthor=gson.fromJson(currentAuthorString, classOf[AuthorCounting])
      val currentAuthor = new Author(currentCountingAuthor.id,currentCountingAuthor.name,currentCountingAuthor.org)
      result.add(currentAuthor)
    })
    return result

  }


  def distinctAuthorsLogLog():Long={
    return jedis.pfcount("hyperLogLogDistinct").toLong
  }

  def distinctAuthorsPreSavedValue():Long ={
    return jedis.get("numberOfDistinctAuthors").toLong
    //jedis.pfcount("distinctAuthors")
  }

  def distinctAuthorsCalculatedValue(): Long = {
    val allKeys = jedis.keys("*")
    val pattern = Pattern.compile("^authorIdToInformation:")
    val matching = allKeys.stream.filter(pattern.asPredicate)
    return matching.count()
  }



}
