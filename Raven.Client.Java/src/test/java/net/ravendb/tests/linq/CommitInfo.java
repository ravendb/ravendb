package net.ravendb.tests.linq;

import java.util.Date;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class CommitInfo {

  private String id;
  private String author;
  private String pathInRepo;
  private String repository;
  private int revision;
  private Date date;
  private String commitMessage;
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getAuthor() {
    return author;
  }
  public void setAuthor(String author) {
    this.author = author;
  }
  public String getPathInRepo() {
    return pathInRepo;
  }
  public void setPathInRepo(String pathInRepo) {
    this.pathInRepo = pathInRepo;
  }
  public String getRepository() {
    return repository;
  }
  public void setRepository(String repository) {
    this.repository = repository;
  }
  public int getRevision() {
    return revision;
  }
  public void setRevision(int revision) {
    this.revision = revision;
  }
  public Date getDate() {
    return date;
  }
  public void setDate(Date date) {
    this.date = date;
  }
  public String getCommitMessage() {
    return commitMessage;
  }
  public void setCommitMessage(String commitMessage) {
    this.commitMessage = commitMessage;
  }


}
