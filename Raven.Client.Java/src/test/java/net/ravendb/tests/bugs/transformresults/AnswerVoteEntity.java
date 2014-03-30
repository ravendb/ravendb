package net.ravendb.tests.bugs.transformresults;

public class AnswerVoteEntity {
  private String id;
  private String questionId;
  private AnswerEntity answer;
  private int delta;
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getQuestionId() {
    return questionId;
  }
  public void setQuestionId(String questionId) {
    this.questionId = questionId;
  }
  public AnswerEntity getAnswer() {
    return answer;
  }
  public void setAnswer(AnswerEntity answer) {
    this.answer = answer;
  }
  public int getDelta() {
    return delta;
  }
  public void setDelta(int delta) {
    this.delta = delta;
  }

}
