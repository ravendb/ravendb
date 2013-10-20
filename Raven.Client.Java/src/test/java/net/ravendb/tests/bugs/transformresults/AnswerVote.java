package net.ravendb.tests.bugs.transformresults;

public class AnswerVote {
  private String questionId;
  private String answerId;
  private int delta;
  private double decimalValue;
  public String getQuestionId() {
    return questionId;
  }
  public void setQuestionId(String questionId) {
    this.questionId = questionId;
  }
  public String getAnswerId() {
    return answerId;
  }
  public void setAnswerId(String answerId) {
    this.answerId = answerId;
  }
  public int getDelta() {
    return delta;
  }
  public void setDelta(int delta) {
    this.delta = delta;
  }
  public double getDecimalValue() {
    return decimalValue;
  }
  public void setDecimalValue(double decimalValue) {
    this.decimalValue = decimalValue;
  }

}
