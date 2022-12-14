import com.google.gson.Gson
import redis.clients.jedis.Jedis

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import java.util.Scanner
import java.util.regex.Pattern
import scala.annotation.tailrec


class WritingManager {
    val batchSize=3000
    val jedis=new Jedis("127.0.0.1", 6379,5000000)

  def processFileBatchWise(path:String):Unit={
    jedis.set("highestNumberOfArticlesWritten","1")
    var currentIteration=0
    val is = new FileInputStream(path)
    val gson = new Gson
    val scanner = new Scanner(is, StandardCharsets.UTF_8.name())
    while (scanner.hasNextLine){
      val setManager = new SetQuerryManaget()
      val currentBatch = getBatches(0,batchSize,new util.ArrayList[String](),scanner)

      setManager.processBatch(currentBatch)
      println("batch "+currentIteration+" has been processed")
      currentIteration=currentIteration+1
      println(java.time.LocalDateTime.now().toString)
    }
    //just some finishing Operation
    val allKeys = jedis.keys("*")
    val pattern = Pattern.compile("^authorIdToInformation:")
    val matching = allKeys.stream.filter(pattern.asPredicate)
    jedis.set("numberOfDistinctAuthors", matching.count().toString)
  }

  def getBatches(currentCount:Integer, batchSize:Integer, currentArticles:util.ArrayList[String], scanner:Scanner):util.ArrayList[String]={
       if (scanner.hasNextLine) {
         if (currentCount == batchSize)
           return currentArticles
         else {
           currentArticles.add(scanner.nextLine())
           return getBatches(currentCount + 1,
             batchSize,
             currentArticles,
             scanner)}

       } else {
         return currentArticles
       }

  }


  @tailrec
  final def removeLeadingComma(string: String):String = {
    string.take (1) match {
    case "," => removeLeadingComma(string.substring (1))
    case _ => string
    }
  }


}
