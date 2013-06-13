package raven.client.connection;

import java.util.Date;

import org.apache.commons.collections.MultiMap;

import raven.abstractions.json.linq.RavenJToken;

public class CachedRequest {
  private RavenJToken data;
  private Date time;
  private MultiMap headers;
  private String database;
  private boolean forceServerCheck;
  /**
   * @return the data
   */
  public RavenJToken getData() {
    return data;
  }
  /**
   * @param data the data to set
   */
  public void setData(RavenJToken data) {
    this.data = data;
  }
  /**
   * @return the time
   */
  public Date getTime() {
    return time;
  }
  /**
   * @param time the time to set
   */
  public void setTime(Date time) {
    this.time = time;
  }
  /**
   * @return the headers
   */
  public MultiMap getHeaders() {
    return headers;
  }
  /**
   * @param headers the headers to set
   */
  public void setHeaders(MultiMap headers) {
    this.headers = headers;
  }
  /**
   * @return the database
   */
  public String getDatabase() {
    return database;
  }
  /**
   * @param database the database to set
   */
  public void setDatabase(String database) {
    this.database = database;
  }
  /**
   * @return the forceServerCheck
   */
  public boolean isForceServerCheck() {
    return forceServerCheck;
  }
  /**
   * @param forceServerCheck the forceServerCheck to set
   */
  public void setForceServerCheck(boolean forceServerCheck) {
    this.forceServerCheck = forceServerCheck;
  }

}
