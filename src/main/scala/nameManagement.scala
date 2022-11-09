object nameManagement {


  def getArticleIdToAuthorsKey(article: Article): String = {
    return getArticleIdToAuthorsKey(article.id.toString)
  }

  def getArticleIdToAuthorsKey(articleId: Long): String = {
    return "articleIdToAuthors:" + articleId.toString
  }
  def getArticleIdToAuthorsKey(articleId: String): String = {
    return "articleIdToAuthors:" + articleId
  }

  def getAuthorKey(author: Author): String = {
    return getAuthorKey(author.id)
  }

  def getAuthorKey(authorId: String): String = {
    return getAuthorKey(authorId.toLong)
  }

  def getAuthorKey(authorId:Long ): String = {
    return "authorIdToInformation:" + authorId.toString
  }


  def getArticleIdToInformationKey(article: Article): String = {
    return getArticleIdToInformationKey(article.id.toString)
  }

  def getArticleIdToInformationKey(articleId:Long): String = {
    return getArticleIdToInformationKey(articleId.toString)
  }

  def getArticleIdToInformationKey(articleId: String): String = {
    return "articleIdToInformation" + articleId
  }

}
