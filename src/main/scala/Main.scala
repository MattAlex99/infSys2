import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction

object Main {
  def main(args: Array[String]): Unit = {


    val getQuerryManager = new GetQuerryManager
    println(getQuerryManager.distinctAuthorsCalculatedValue())
    val jedis = new Jedis("127.0.0.1", 6379,50000)
    //println(jedis.get("a"))
    //val jedis = new Jedis("127.0.0.1", 6379,5000)
    //val pipeline = jedis.pipelined()
    //val writingManager = new WritingManager
    //writingManager.processFileBatchWise("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json/dblp.v12.json")
//
    //var i=0
    //println(java.time.LocalDateTime.now().toString)
    //while (true) {
    //    i=i+1
    //    //jedis.set("key1"+i,"value")
    //    pipeline.set("key"+i,"value")
    //  if (i % 10000 == 0) {
    //    println("current line " + i)
    //    //println(java.time.LocalDateTime.now().toString)
    //  }
    //  if(i%100000==0){
    //      println("current line " + i)
    //      println(java.time.LocalDateTime.now().toString)
    //    }
    //  }

    //val writingManager = new WritingManager
    //writingManager.readFileByLine("D:\\Uni\\Master1\\Inf-Sys\\dblp.v12.json/dblp.v12.json")


    //val jedis = new Jedis("127.0.0.1", 6379)
    //var i=0
    //while (true){
    //  i=i+1
    //  jedis.set("keyid"+i,"afafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafap")
    //  if(i%10000==0){
    //    println("current line " + i)
    //    println(java.time.LocalDateTime.now().toString)
    //  }
    //}
    ////simple key value
    //jedis.set("hallstadt", "1234")
    //jedis.set("Gröningen","9023")
    //println(jedis.get("hallstadt"))
//
    ////hash Sets
    //jedis.hset("TierReiche", "Homo Sapiens", "Animalia")
    //jedis.hset("TierReiche", "Fliegenpilz", "Fungi")
    //println(jedis.hget("TierReiche","Homo Sapiens"))
    //println(jedis.hgetAll("TierReiche"))
//
//
    ////Transaction = menge von Befehlen, die alle auf einmal ausgeführt werden
    //val t = jedis.multi()
    //t.set("k1", "userTwoId")
    //t.set("k2", "userOneId")
    //t.exec();
    ////testen ob eine Transaktion erfolgreich war
    //println(jedis.watch("k3"))
    //println(jedis.get("k1"))
//
//
    }
}