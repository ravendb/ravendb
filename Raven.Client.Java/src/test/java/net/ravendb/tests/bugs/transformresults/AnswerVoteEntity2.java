package net.ravendb.tests.bugs.transformresults;

import java.util.UUID;

public class AnswerVoteEntity2 {
  private UUID id;
  private UUID questionId;
  private AnswerEntity2 answer;
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
  public AnswerEntity2 getAnswer() {
    return answer;
  }
  public void setAnswer(AnswerEntity2 answer) {
    this.answer = answer;
  }
  public int getDelta() {
    return delta;
  }
  public void setDelta(int delta) {
    this.delta = delta;
  }

}
