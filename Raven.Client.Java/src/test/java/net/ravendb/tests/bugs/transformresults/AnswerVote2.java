package net.ravendb.tests.bugs.transformresults;

import java.util.UUID;

public class AnswerVote2 {
  private UUID id;
  private UUID questionId;
  private UUID answerId;
  private int delta;
  public UUID getId() {
    return id;
  }
  public void setId(UUID id) {
    this.id = id;
  }
  public UUID getQuestionId() {
    return questionId;
  }
  public void setQuestionId(UUID questionId) {
    this.questionId = questionId;
  }
  public UUID getAnswerId() {
    return answerId;
  }
  public void setAnswerId(UUID answerId) {
    this.answerId = answerId;
  }
  public int getDelta() {
    return delta;
  }
  public void setDelta(int delta) {
    this.delta = delta;
  }

}
