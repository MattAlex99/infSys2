import Article.{gson, removeLeadingComma}
import com.google.gson.Gson
import com.sun.tools.javac.parser.ReferenceParser.Reference
import redis.clients.jedis.{Jedis, Pipeline, Response}

import java.util
import java.util.Collections
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class SetQuerryManaget {
  val jedis = new Jedis("127.0.0.1", 6379,500000)
  //val pipeline = jedis.pipelined()
  val gson = new Gson()



  val authorsWithMostArticlesWritten = new util.ArrayList[Author]()

  def processBatch(currentBatch: util.ArrayList[String]): Unit = {
    var pipeline = jedis.pipelined()
    var allArticles = articleStringsToArticles(currentBatch)



    //add all article IDs to ArticleContent
    allArticles.forEach(article => insertArticleIdtoArticleContent(article, pipeline))

    //add articleIdToAuthors structure
    allArticles.forEach(article=> addArticleIdToAuthors(article, pipeline))

    //check which authors already did write an article
    val alreadyExistingAuthorsOfBatch = getAlreadyExistingAuthorsOfBatch(allArticles,pipeline)

    //add to "article referenced by" Lists
    allArticles.forEach(currentArticle =>adjustArticleReferencesOfArticle(currentArticle,pipeline))

    //add articles to Authors created article list
    allArticles.forEach(currentArticle =>adjustAuthorToArticleList(currentArticle,pipeline))

    //fetch highest Number of Articles Written of any author
    val previouslyHighestArticleCountResponse = getHighestNumberofArticlesWritten(pipeline)
    pipeline.sync()

    val secondPipeline = jedis.pipelined()
    val previouslyHighestArticleCount = previouslyHighestArticleCountResponse.get()
    //add authors, count how many articles they have written and update article written higscore List
    addAuthors(alreadyExistingAuthorsOfBatch,previouslyHighestArticleCount.toInt,secondPipeline)
    secondPipeline.sync()

  }
  def adjustArticleReferencesOfArticle(article:Article,pipeline: Pipeline)={
    if ( !(article.references == null))
        article.references.foreach(referencedArticle => pushIntoArticleReferences(referencedArticle,article.id,pipeline))
  }
  def pushIntoArticleReferences(referencedArticleID: Long, referencingArticleId: Long,pipeline: Pipeline): Unit = {
    pipeline.rpush("ArticleReferencedBy:" + referencedArticleID.toString, referencingArticleId.toString)
  }

  def adjustAuthorToArticleList(article: Article, pipeline: Pipeline) = {
    if (!(article.authors==null))
      article.authors.foreach(currentAuthor => pushIntoAuthorToArticleList(currentAuthor, article.id, pipeline))
  }
  def pushIntoAuthorToArticleList(author: Author, articleID: Long,pipeline: Pipeline): Unit = {
    //keeps track of whick articles have been writen by which author
    pipeline.rpush("authorToArticle:" + author.id, articleID.toString)

  }
  def addAuthors(allResponses: util.ArrayList[(Author, Response[String])], currentHighestArticleCount: Integer,pipeline:Pipeline)={
    val mapOfNumberOfArticlesWrittenByAuthorInBatch = getMapOfNumberOfArticlesWrittenByAuthorInBatch(allResponses)
    addAuthorsInner(allResponses,
                    currentHighestArticleCount,
                    new util.ArrayList[Author](),
                    new util.ArrayList[Author](),
                    mapOfNumberOfArticlesWrittenByAuthorInBatch,
                    pipeline)
  }


  def AuthorListContainsID(authorList:util.ArrayList[Author], authorid:Long):Boolean={
    authorList.forEach(currentAuthor => {
      if(currentAuthor.id==authorid)
        return true
    })
    return false
  }
  @tailrec
  final def addAuthorsInner(allAuthors:util.ArrayList[(Author,Response[String])],
                            currentHighestArticleCount:Integer,
                            authorsWithMostArticles:util.ArrayList[Author],
                            alreadyProcessedAuthors:util.ArrayList[Author],
                            mapOfNumberOfArticlesWrittenByAuthorInBatch:util.HashMap[Long,Integer],
                            pipeline:Pipeline):Unit={
    allAuthors.size() match {
      case 0 =>   //println("AddingMostAuthors")
                  //println(authorsWithMostArticles)
                  setNewHighestNumberOfArticlesWritten(currentHighestArticleCount,pipeline)
                  addAuthorsToHighestArticleCount(authorsWithMostArticles,pipeline)
      case _ =>
                  val currentAuthor=allAuthors.get(0)._1
                  val currentResponse = allAuthors.get(0)._2.get()
                  allAuthors.remove(0)
                  //add authors for hyperloglol distinct counting
                  pipeline.pfadd("hyperLogLogDistinct", currentAuthor.id.toString)

                  if (!AuthorListContainsID(alreadyProcessedAuthors,currentAuthor.id)) {
                    alreadyProcessedAuthors.add(currentAuthor)

                    val stringForNewAuthorCounting=Author.getAuthorStringFromResonse(currentAuthor,currentResponse)

                    val currentAuthorCounting = gson.fromJson(stringForNewAuthorCounting, classOf[AuthorCounting])
                    //check how many articles have been written by this author in this batch
                    val currentAuthorCountOfArticlesWrittenInThisBatch = mapOfNumberOfArticlesWrittenByAuthorInBatch.get(currentAuthor.id)
                    val totalNumberofArticlesWritten = currentAuthorCounting.numArticles + currentAuthorCountOfArticlesWrittenInThisBatch
                    //write current Author with new Article Count
                    val newAuthorString = Author.getAuthorJsonString(currentAuthor,totalNumberofArticlesWritten)
                    //println("setting:"+nameManagement.getAuthorKey(currentAuthor) )
                    pipeline.set(nameManagement.getAuthorKey(currentAuthor),newAuthorString)


                    if(totalNumberofArticlesWritten==currentHighestArticleCount){
                      authorsWithMostArticles.add(currentAuthor)
                    }

                    if (totalNumberofArticlesWritten > currentHighestArticleCount) {
                      //println("new highest Score " +currentAuthor.id)
                      //delete previous highscore List
                      resetListOfAuthorsWithMostArticles(pipeline)
                      //create a new List
                      val newMostArticlesList = new util.ArrayList[Author]()
                      newMostArticlesList.add(currentAuthor)
                      addAuthorsInner(allAuthors,
                        totalNumberofArticlesWritten,
                        newMostArticlesList,
                        alreadyProcessedAuthors,
                        mapOfNumberOfArticlesWrittenByAuthorInBatch,
                        pipeline)
                    } else {
                      addAuthorsInner(allAuthors,
                        currentHighestArticleCount,
                        authorsWithMostArticles,
                        alreadyProcessedAuthors,
                        mapOfNumberOfArticlesWrittenByAuthorInBatch,
                        pipeline)
                    }
                  }
                  else{
                    addAuthorsInner(allAuthors,
                      currentHighestArticleCount,
                      authorsWithMostArticles,
                      alreadyProcessedAuthors,
                      mapOfNumberOfArticlesWrittenByAuthorInBatch,
                      pipeline)
                        }
    }
}

  def getMapOfNumberOfArticlesWrittenByAuthorInBatch(allResponses: util.ArrayList[(Author, Response[String])]):util.HashMap[Long,Integer] ={
    //returns a mac maping author IDs to the number of articles this author wrote in this batch
    val listofIds = new util.ArrayList[Long]
    val result=new util.HashMap[Long,Integer]()
    allResponses.forEach(tup => listofIds.add(tup._1.id))
    listofIds.forEach(id => result.put(id,Collections.frequency(listofIds, id)))
    return result
  }
  def addAuthorsToHighestArticleCount(authors:util.ArrayList[Author], pipeline: Pipeline)={
    if (!(authors==null))
      authors.forEach(author=> addAuthorToHighestArticleCountList(author.id,pipeline))
  }
  def resetListOfAuthorsWithMostArticles(pipeline:Pipeline): Unit ={
    pipeline.del("authorsWithMostArticles")
  }

  def addAuthorToHighestArticleCountList(authorId: Long, pipeline: Pipeline): Unit = {
    //println("adding author:" +authorId)
    pipeline.rpush("authorsWithMostArticles", authorId.toString)
  }
  def getHighestNumberofArticlesWritten(pipeline: Pipeline):Response[String] ={
   pipeline.get("highestNumberOfArticlesWritten")
  }
  def setNewHighestNumberOfArticlesWritten(value:Integer,pipeline: Pipeline)={
      pipeline.set("highestNumberOfArticlesWritten",value.toString)
  }

  def addArticleIdToAuthors(article: Article,pipeline: Pipeline)={
    if (!(article.authors==null))
      article.authors.forEach(author => pipeline.rpush(nameManagement.getArticleIdToAuthorsKey(article),author.id.toString))
  }



  def getAlreadyExistingAuthorsOfBatch(articleBatch:util.ArrayList[Article],pipeline: Pipeline):util.ArrayList[(Author,Response[String])] = {
    val resultList = new util.ArrayList[(Author,Response[String])]
    articleBatch.forEach(article => addArticleOfAuthor(article.authors,resultList,pipeline))
    return resultList
  }
  def addArticleOfAuthor(authors:util.ArrayList[Author],currentResult:util.ArrayList[(Author,Response[String])], pipeline: Pipeline): Unit ={
    if(!(authors==null))
      authors.forEach(author=> currentResult.add((author,pipeline.get(nameManagement.getAuthorKey(author)))))
  }
  def insertArticleIdtoArticleContent(article: Article,pipeline:Pipeline): Unit ={
    val strippedArticle = StripedArticle.stripArticle(article)
    val articleString = gson.toJson(strippedArticle) //references stil need to be removed
    pipeline.set(nameManagement.getArticleIdToInformationKey(article), articleString)
  }



  def articleStringsToArticles(articleStringList: util.ArrayList[String]):util.ArrayList[Article]={
    val result= new util.ArrayList[Article]()
    articleStringList.forEach(articleString => articleStringToArticleInner(articleString,result))
    return result
  }
  def articleStringToArticleInner(articleString:String,articleList:util.ArrayList[Article]):Unit={
    val rawLineString = articleString.replaceAll("[^\\x00-\\x7F]", "?")
    rawLineString match {
      case "]" =>
      case "[" =>
      case _ => val clearedLineString = removeLeadingComma(rawLineString)
                articleList.add(gson.fromJson(clearedLineString, classOf[Article]))
    }
  }
  final private def removeLeadingComma(string: String): String = {
    string.take(1) match {
      case "," => removeLeadingComma(string.substring(1))
      case _ => string
    }
  }



  def getAuthorsStringInner(authors:util.ArrayList[Author],currentString:String, iteration:Integer):String= {
      iteration == authors.size match {
      case true => return currentString + "]"
      case _ => return getAuthorsStringInner(authors,
        currentString + "," + Author.getAuthorJsonString(authors.get(iteration)),iteration+1)
    }
  }



}
