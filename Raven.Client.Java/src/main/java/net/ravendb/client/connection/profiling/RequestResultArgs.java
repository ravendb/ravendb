package net.ravendb.client.connection.profiling;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.basic.EventArgs;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.client.connection.profiling.RequestStatus;

import org.apache.commons.lang.StringUtils;


public class RequestResultArgs extends EventArgs {

  private Map<String, String> additionalInformation;
  private Date at;
  private RequestStatus status;
  private String url;
  private double durationMilliseconds;
  private HttpMethods method;
  private String postedData;
  private int httpResult;
  private String result;

  public RequestResultArgs() {
    at = new Date();
    additionalInformation = new HashMap<>();
  }

  /**
   * Any additional information that might be required
   * @return the additionalInformation
   */
  public Map<String, String> getAdditionalInformation() {
    return additionalInformation;
  }
  /**
   * @param additionalInformation the additionalInformation to set
   */
  public void setAdditionalInformation(Map<String, String> additionalInformation) {
    this.additionalInformation = additionalInformation;
  }
  /**
   *  When the request completed
   * @return the at
   */
  public Date getAt() {
    return at;
  }
  /**
   * @param at the at to set
   */
  public void setAt(Date at) {
    this.at = at;
  }
  /**
   * The request status
   * @return the status
   */
  public RequestStatus getStatus() {
    return status;
  }
  /**
   * @param status the status to set
   */
  public void setStatus(RequestStatus status) {
    this.status = status;
  }
  /**
   * The request Url
   * @return the url
   */
  public String getUrl() {
    return url;
  }
  /**
   * @param url the url to set
   */
  public void setUrl(String url) {
    this.url = url;
  }
  /**
   * How long this request took
   * @return the durationMilliseconds
   */
  public double getDurationMilliseconds() {
    return durationMilliseconds;
  }
  /**
   * @param durationMilliseconds the durationMilliseconds to set
   */
  public void setDurationMilliseconds(double durationMilliseconds) {
    this.durationMilliseconds = durationMilliseconds;
  }
  /**
   *  The request method
   * @return the method
   */
  public HttpMethods getMethod() {
    return method;
  }
  /**
   * @param method the method to set
   */
  public void setMethod(HttpMethods method) {
    this.method = method;
  }
  /**
   * The data posted to the server
   * @return the postedData
   */
  public String getPostedData() {
    return postedData;
  }
  /**
   * @param postedData the postedData to set
   */
  public void setPostedData(String postedData) {
    this.postedData = postedData;
  }
  /**
   * The HTTP result for this request
   * @return the httpResult
   */
  public int getHttpResult() {
    return httpResult;
  }
  /**
   * @param httpResult the httpResult to set
   */
  public void setHttpResult(int httpResult) {
    this.httpResult = httpResult;
  }
  /**
   *  The result of this request
   * @return the result
   */
  public String getResult() {
    return result;
  }
  /**
   * @param result the result to set
   */
  public void setResult(String result) {
    this.result = result;
  }

  public int getTotalSize() {
    return StringUtils.length(result) + StringUtils.length(postedData) + StringUtils.length(url);
  }



}
