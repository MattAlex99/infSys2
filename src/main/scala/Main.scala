import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction

import java.util.regex.Pattern

object Main {
  def main(args: Array[String]): Unit = {






   val getQuerryManager = new GetQuerryManager
   //println(getQuerryManager.distinctAuthorsCalculatedValue())
   val authorsWithMost=getQuerryManager.mostArticles()
    println("author with most articles information")
    println(authorsWithMost)
    val articles=getQuerryManager.articles(authorsWithMost.get(0).id)
    println(articles.size())
    println(authorsWithMost)
    println("referenced by:")
    println(getQuerryManager.referencedBy(2005687710)) //should contain 1091
   println("distinctAuthors: (HyperLogLog)")
   println(getQuerryManager.distinctAuthorsLogLog())
   println("destinct Authors: (presafed)")
    println(getQuerryManager.distinctAuthorsPreSavedValue())
    println("destinct Authors: (calucleted, may take a while)")
    println(getQuerryManager.distinctAuthorsCalculatedValue())



    //val jedis = new Jedis("127.0.0.1", 6379,50000)
    //println(jedis.get("authorIdToInformation:1983837166"))

    //val writingManager = new WritingManager
    //writingManager.processFileBatchWise("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json/dblp.v12.json")

    }
}