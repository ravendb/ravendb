package net.ravendb.tests.bugs.transformresults;

public class QuestionVote {
  private String questionId;
  private int delta;
  public String getQuestionId() {
    return questionId;
  }
  public void setQuestionId(String questionId) {
    this.questionId = questionId;
  }
  public int getDelta() {
    return delta;
  }
  public void setDelta(int delta) {
    this.delta = delta;
  }

}
