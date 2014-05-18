package net.ravendb.abstractions.data;

import java.util.Map;
import java.util.TreeMap;

import net.ravendb.abstractions.json.linq.RavenJToken;


public class GetResponse {
  private RavenJToken result;

  private Map<String, String> headers;
  private int status;
  public GetResponse() {
    headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  }
  public Map<String, String> getHeaders() {
    return headers;
  }
  public RavenJToken getResult() {
    return result;
  }
  public int getStatus() {
    return status;
  }
  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }
  public void setResult(RavenJToken result) {
    this.result = result;
  }
  public void setStatus(int status) {
    this.status = status;
  }


  public boolean isRequestHasErrors() {
    switch (status) {
    case 0: // aggressively cached
    case  200: //known non error value
    case 201:
    case 203:
    case 204:
    case 304:
    case 404:
      return false;
    default:
      return true;
    }
  }


}
