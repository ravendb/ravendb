package net.ravendb.tests.bugs.transformresults;

public class AnswerViewItem {
  private String questionId;
  private String answerId;
  private String content;
  private String userId;
  private String userDisplayName;
  private int voteTotal;
  private double decimalTotal;

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
  public String getContent() {
    return content;
  }
  public void setContent(String content) {
    this.content = content;
  }
  public String getUserId() {
    return userId;
  }
  public void setUserId(String userId) {
    this.userId = userId;
  }
  public String getUserDisplayName() {
    return userDisplayName;
  }
  public void setUserDisplayName(String userDisplayName) {
    this.userDisplayName = userDisplayName;
  }
  public int getVoteTotal() {
    return voteTotal;
  }
  public void setVoteTotal(int voteTotal) {
    this.voteTotal = voteTotal;
  }
  public double getDecimalTotal() {
    return decimalTotal;
  }
  public void setDecimalTotal(double decimalTotal) {
    this.decimalTotal = decimalTotal;
  }

}
