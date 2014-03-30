package net.ravendb.tests.bugs.transformresults;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Answer {
  private String id;
  private String userId;
  private String questionId;
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
  public String getQuestionId() {
    return questionId;
  }
  public void setQuestionId(String questionId) {
    this.questionId = questionId;
  }
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }

}
