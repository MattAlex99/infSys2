import Article.{gson, removeLeadingComma}
import com.google.gson.Gson
import com.sun.tools.javac.parser.ReferenceParser.Reference
import redis.clients.jedis.{Jedis, Pipeline, Response}

import java.util
import java.util.Collections
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class SetQuerryManaget {
  val jedis = new Jedis("127.0.0.1", 6379,5000)
  //val pipeline = jedis.pipelined()
  val gson = new Gson()



  val authorsWithMostArticlesWritten = new util.ArrayList[Author]()

  def processBatch(currentBatch: util.ArrayList[String]): Unit = {
    var pipeline = jedis.pipelined()
    var allArticles = articleStringsToArticles(currentBatch)

    //fetch highest Number of Articles Written of any author
    val previouslyHighestArticleCountResponse= getHighestNumberofArticlesWritten(pipeline)

    //add article ID to ArticleContent
    allArticles.forEach(article => insertArticleIdtoArticleContent(article, pipeline))

    //add articleIdToAuthors
    allArticles.forEach(article=> addArticleIdToAuthors(article, pipeline))

    //check which authors already did write an article
    val alreadyExistingAuthorsOfBatch = getAlreadyExistingAuthorsOfBatch(allArticles,pipeline)

    //add to referenced by Lists
    allArticles.forEach(currentArticle =>adjustArticleReferencesOfArticle(currentArticle,pipeline))

    //add articles to Authors create darticle list
    allArticles.forEach(currentArticle =>adjustAuthorToArticleList(currentArticle,pipeline))


    pipeline.sync()

    val secondPipeline = jedis.pipelined()
    val previouslyHighestArticleCount = previouslyHighestArticleCountResponse.get()
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

  @tailrec
  final def addAuthorsInner(allAuthors:util.ArrayList[(Author,Response[String])],
                            currentHighestArticleCount:Integer,
                            authorsWithMostArticles:util.ArrayList[Author],
                            alreadyProcessedAuthors:util.ArrayList[Author],
                            mapOfNumberOfArticlesWrittenByAuthorInBatch:util.HashMap[Long,Integer],
                            pipeline:Pipeline):Unit={
    allAuthors.size() match {
      case 0 =>
                  setNewHighestNumberOfArticlesWritten(currentHighestArticleCount,pipeline)
                  addAuthorsToHighestArticleCount(authorsWithMostArticles,pipeline)
      case _ =>
                  val currentAuthor=allAuthors.get(0)._1
                  val currentResponse = allAuthors.get(0)._2.get()
                  allAuthors.remove(0)
                  //add authors for hyperloglol distinct counting
                  pipeline.pfadd("hyperLogLogDistinct", currentAuthor.id.toString)

                  if (!alreadyProcessedAuthors.contains(currentAuthor)){
                    alreadyProcessedAuthors.add(currentAuthor)

                    val stringForNewAuthorCounting=Author.getAuthorStringFromResonse(currentAuthor,currentResponse)
                    try {
                     val currentAuthorCountingTest = gson.fromJson(stringForNewAuthorCounting, classOf[AuthorCounting])
                   } catch {
                     case e:Exception => println("crash")
                                          println(e)
                                         println(stringForNewAuthorCounting)
                                         println(currentAuthor)
                   }
                    val currentAuthorCounting = gson.fromJson(stringForNewAuthorCounting, classOf[AuthorCounting])
                    //check how many articles have been written by this author in this batch
                    val currentAuthorCountOfArticlesWrittenInThisBatch = mapOfNumberOfArticlesWrittenByAuthorInBatch.get(currentAuthor.id)
                    val totalNumberofArticlesWritten = currentAuthorCounting.numArticles + currentAuthorCountOfArticlesWrittenInThisBatch
                    //write current Author with new Article Count
                    val newAuthorString = Author.getAuthorJsonString(currentAuthor,totalNumberofArticlesWritten)
                    pipeline.set(getAuthorKey(currentAuthor),newAuthorString)

                    if(totalNumberofArticlesWritten==currentHighestArticleCount){
                      authorsWithMostArticles.add(currentAuthor)
                    }

                    if (totalNumberofArticlesWritten > currentHighestArticleCount) {
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

  def addAuthorToHighestArticleCountList(authorId: Long, pipeline: Pipeline): Unit = {
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
      article.authors.forEach(author => pipeline.rpush(getArticleIdToAuthorsKey(article),author.id.toString))
  }


  def getNumberOfArticlesWritenByEachAuthorInBatch(allArticles:util.ArrayList[Article]):util.HashMap[Long,Integer]={
    val countMap = new util.HashMap[Long,Integer]()
    allArticles.forEach(article=> countAuthorContributions(article,countMap))
    return countMap
  }

  def countAuthorContributions(article: Article,countMap:util.HashMap[Long,Integer]):Unit={
    article.authors.forEach(author => {
        countMap.containsKey(author) match{
          case false => countMap.put(author.id,1)
          case true => val authorsCurrentCount=countMap.get(author)
                        countMap.put(author.id,authorsCurrentCount+1)
                      print(author)
        }
    })
  }

  def getAlreadyExistingAuthorsOfBatch(articleBatch:util.ArrayList[Article],pipeline: Pipeline):util.ArrayList[(Author,Response[String])] = {
    val resultList = new util.ArrayList[(Author,Response[String])]
    articleBatch.forEach(article => addArticleOfAuthor(article.authors,resultList,pipeline))
    return resultList
  }
  def addArticleOfAuthor(authors:util.ArrayList[Author],currentResult:util.ArrayList[(Author,Response[String])], pipeline: Pipeline): Unit ={
    if(!(authors==null))
      authors.forEach(author=> currentResult.add((author,pipeline.get(getAuthorKey(author)))))
  }
  def insertArticleIdtoArticleContent(article: Article,pipeline:Pipeline): Unit ={
    val strippedArticle = StripedArticle.stripArticle(article)
    val articleString = gson.toJson(strippedArticle) //references stil need to be removed
    pipeline.set(getArticleIdToInformationKey(article), articleString)
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

  //def getAuthorSetString(author: Author,previousAuthorEntry:String): String = {
  //  //val previousAuthorEntry = jedis.get(getAuthorKey(author))
  //  previousAuthorEntry match {
  //    case null => Author.getAuthorJsonString(author)
  //    case _ => val authorCounting = gson.fromJson(previousAuthorEntry, classOf[AuthorCounting])
  //      //create a new string with incremented articles count
  //      val autorToBeWritten = new AuthorCounting(authorCounting.name, authorCounting.org, authorCounting.numArticles + 1)
  //      return gson.toJson(autorToBeWritten)
  //  }
  //}


 //def insertArticle(article:Article):Unit={
 //  //val t = jedis.multi()

 //  //stores articleID to articleInfo (articleInfo relpaced with articleID)
 //  //solves titleByID(articleID: Long): String
 //  val strippedArticle = StripedArticle.stripArticle(article)
 //  val articleString= gson.toJson(strippedArticle) //references stil need to be removed
 //  jedis.set(getArticleIdToInformationKey(article),articleString)

 //  //stores information authorID to authorInfo
 //  article.authors.foreach(author => writeAuthor(author))

 //  //solves articles(authorID: Long): List[Article]
 //  article.authors.foreach(author =>adjustAuthorsToArticleList(author,article.id.toString))

 //  //solves referencedBy(articleID: Long): List[Article]


 //  //t.exec();
 //  //t.hset("article:IdToJson", article.id.toString, article.title)

 //  //store the list of references from each article as string
 //  //für jeden article in references

 //  //article.references.foreach(ref => t.set())
 //}




  def getArticleIdToAuthorsKey(article:Article): String = {
    return "articleIdToAuthors:" + article.id.toString
  }
  def getAuthorKey(author:Author):String={
    return "authorIdToInformation:"+author.id.toString
  }
  def getArticleIdToInformationKey(article:Article):String={
    return "articleIdToInformation"+article.id.toString
  }
 //def writeAuthor(author:Author):Unit={
 //  //add author for authorID to Author Information DB
 //  val newAutorString =determineAuthorSetString(author)
 //  jedis.set(getAuthorKey(author),newAutorString)
 //}
 // def determineAuthorSetString(author:Author): String ={
 //   val authorEntry = jedis.get(getAuthorKey(author))
//
 //   authorEntry match {
 //     case null =>  Author.getAuthorJsonString(author)
//
 //     case _ => val authorCounting = gson.fromJson(authorEntry,classOf[AuthorCounting])
 //               //create a new string with incremented articles count
 //               val autorToBeWritten= new AuthorCounting(authorCounting.name,authorCounting.org,authorCounting.numArticles+1)
 //               return gson.toJson(autorToBeWritten)
 //   }
 // }

  def getAuthorsString(authors:util.ArrayList[Author]):String={
    authors.size() match {
      case 0 => return "[]"
      case 1 => return "["+Author.getAuthorJsonString(authors.get(0))+"]"
      case _ => return getAuthorsStringInner(authors,
                                    "["+Author.getAuthorJsonString(authors.get(0)),
                                        1)
    }

  }
  def getAuthorsStringInner(authors:util.ArrayList[Author],currentString:String, iteration:Integer):String= {
      iteration == authors.size match {
      case true => return currentString + "]"
      case _ => return getAuthorsStringInner(authors,
        currentString + "," + Author.getAuthorJsonString(authors.get(iteration)),iteration+1)
    }
  }
    def insertIntoAuthor(author: Author): Unit = {
      val t = jedis.multi()
      //track to dstinct authors
      t.pfadd("distinctAuthors", author.id.toString)
      t.exec();

    }

    def insertReference(reference: Reference): Unit = {
      val t = jedis.multi()
      t.exec();
    }


}
