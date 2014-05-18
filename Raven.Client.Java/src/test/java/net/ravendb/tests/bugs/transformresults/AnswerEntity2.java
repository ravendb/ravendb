package net.ravendb.tests.bugs.transformresults;

import java.util.List;
import java.util.UUID;

public class AnswerEntity2 {
  private UUID id;
  private String userId;
  private Question2 question;
  private String content;
  private List<AnswerVoteEntity2> votes;
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
  public Question2 getQuestion() {
    return question;
  }
  public void setQuestion(Question2 question) {
    this.question = question;
  }
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }
  public List<AnswerVoteEntity2> getVotes() {
    return votes;
  }
  public void setVotes(List<AnswerVoteEntity2> votes) {
    this.votes = votes;
  }

}
