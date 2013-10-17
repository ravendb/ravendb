package net.ravendb.tests.bugs.transformresults;

import java.util.List;
import java.util.UUID;

public class Answer2 {
  private UUID id;
  private String userId;
  private UUID questionId;
  private String content;
  private List<AnswerVote2> votes;
  public UUID getId() {
    return id;
  }
  public void setId(UUID id) {
    this.id = id;
  }
  public String getUserId() {
    return userId;
  }
  public void setUserId(String userId) {
    this.userId = userId;
  }
  public UUID getQuestionId() {
    return questionId;
  }
  public void setQuestionId(UUID questionId) {
    this.questionId = questionId;
  }
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }
  public List<AnswerVote2> getVotes() {
    return votes;
  }
  public void setVotes(List<AnswerVote2> votes) {
    this.votes = votes;
  }

}
