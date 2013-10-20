package net.ravendb.tests.bugs.transformresults;

public class QuestionView {
  private String questionId;
  private String userDisplayName;
  private String questionTitle;
  private String questionContent;
  private int voteTotal;
  private User user;
  private Question question;
  public String getQuestionId() {
    return questionId;
  }
  public void setQuestionId(String questionId) {
    this.questionId = questionId;
  }
  public String getUserDisplayName() {
    return userDisplayName;
  }
  public void setUserDisplayName(String userDisplayName) {
    this.userDisplayName = userDisplayName;
  }
  public String getQuestionTitle() {
    return questionTitle;
  }
  public void setQuestionTitle(String questionTitle) {
    this.questionTitle = questionTitle;
  }
  public String getQuestionContent() {
    return questionContent;
  }
  public void setQuestionContent(String questionContent) {
    this.questionContent = questionContent;
  }
  public int getVoteTotal() {
    return voteTotal;
  }
  public void setVoteTotal(int voteTotal) {
    this.voteTotal = voteTotal;
  }
  public User getUser() {
    return user;
  }
  public void setUser(User user) {
    this.user = user;
  }
  public Question getQuestion() {
    return question;
  }
  public void setQuestion(Question question) {
    this.question = question;
  }

}
