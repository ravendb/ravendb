package raven.abstractions.data;

import java.util.Date;

public class ServerError {
  private String index;
  private String error;
  private Date timestamp;
  private String document;
  private String action;

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public String getDocument() {
    return document;
  }

  public void setDocument(String document) {
    this.document = document;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  @Override
  public String toString() {
    return "ServerError [index=" + index + ", error=" + error + ", timestamp=" + timestamp + ", document=" + document + ", action=" + action + "]";
  }


}
