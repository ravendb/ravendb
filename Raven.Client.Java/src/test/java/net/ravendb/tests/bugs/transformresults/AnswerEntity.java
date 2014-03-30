package net.ravendb.tests.bugs.transformresults;

public class AnswerEntity {
  private String id;
  private String userId;
  private Question question;
  private String content;
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getUserId() {
    return userId;
  }
  public void setUserId(String userId) {
    this.userId = userId;
  }
  public Question getQuestion() {
    return question;
  }
  public void setQuestion(Question question) {
    this.question = question;
  }
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }

}
