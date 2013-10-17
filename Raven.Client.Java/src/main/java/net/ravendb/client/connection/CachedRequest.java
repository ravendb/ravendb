package net.ravendb.client.connection;

import java.util.Date;
import java.util.Map;

import net.ravendb.abstractions.json.linq.RavenJToken;


public class CachedRequest {
  private RavenJToken data;
  private Date time;
  private Map<String, String> headers;
  private String database;
  private boolean forceServerCheck;


  public CachedRequest() {
    super();
  }
  public CachedRequest(RavenJToken data, Date time, Map<String, String> headers, String database, boolean forceServerCheck) {
    super();
    this.data = data;
    this.time = time;
    this.headers = headers;
    this.database = database;
    this.forceServerCheck = forceServerCheck;
  }
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
  public Map<String, String> getHeaders() {
    return headers;
  }
  /**
   * @param headers the headers to set
   */
  public void setHeaders(Map<String, String> headers) {
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
