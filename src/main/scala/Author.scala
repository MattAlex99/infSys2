case class Author(id:Long, name:String, org:String)

object Author{

  def getCleanOrgString(author:Author):String={

    author.org match {
      case null => return null
      //case _ => return author.org.replaceAll("#.*?#","").replaceAll("\\\\","").replaceAll("\"","?")
      case _ =>   return author.org.replaceAll("#.*?#","").replaceAll("\\\\","").replaceAll("\"","?")
      //case _ => return author.org.replaceAll("/[^a-zA-Z0-9 ]/g","?").replaceAll("#.*?#", "")
    }
    //return author.org.replaceAll("\"","?")
  }

  def getCleanNameString(author: Author): String = {
    // return author.name.replaceAll("/[^a-zA-Z0-9 ]/g", "?").replaceAll("#.*?#", "")
    return author.name.replaceAll("#.*?#", "").replaceAll("\\\\","").replaceAll("\"", "?")

    //return author.org.replaceAll("\"", "?")
  }
  def getAuthorJsonString(author:Author):String={
    return "{"+"\"name\":\""+ Author.getCleanNameString(author) +"\",\"org\":\""+
      Author.getCleanOrgString(author)+"\","+ "\"numArticles\":\""+"1"+"\"" +"}"
  }

  def getAuthorJsonString(author:Author,countArticles:Integer):String= {
    val AuthorString=Author.getCleanOrgString(author)
    val result= "{"+"\"name\":\""+Author.getCleanNameString(author)+"\",\"org\":\""+AuthorString+"\","+ "\"numArticles\":\""+countArticles+"\"," +"\"id\":\""+author.id.toString+"\"}"
    return result

  }

  def getAuthorStringFromResonse(author:Author,response:String):String={
    response match{
      case null =>val result= getAuthorJsonString(author,0)
                  return result
      case _ =>  return response
    }
  }
}