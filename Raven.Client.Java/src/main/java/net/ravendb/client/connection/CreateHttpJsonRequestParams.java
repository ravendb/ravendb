package net.ravendb.client.connection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.connection.profiling.IHoldProfilingInformation;
import net.ravendb.client.document.DocumentConvention;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;


public class CreateHttpJsonRequestParams implements Serializable {

  private int operationHeadersHash;
  private Map<String, List<String>> operationsHeadersCollection;
  private Map<String, String> operationsHeadersDictionary = new HashMap<>();
  private IHoldProfilingInformation owner;
  private String url;
  private String urlCached;
  private boolean avoidCachingRequest;
  private HttpMethods method;
  private RavenJObject metadata;
  private DocumentConvention convention;
  private boolean disableRequestCompression;

  public CreateHttpJsonRequestParams(IHoldProfilingInformation owner, String url, HttpMethods method, RavenJObject metadata, DocumentConvention convention) {
    super();

    this.method = method;
    this.url = url;
    this.owner = owner;
    this.metadata = metadata;
    this.convention = convention;
    this.operationsHeadersCollection = new HashMap<>();
  }

  /**
   * Adds the operation headers.
   * @param operationsHeaders
   * @return
   */
  public CreateHttpJsonRequestParams addOperationHeaders(Map<String, String> operationsHeaders) {
    urlCached = null;
    operationsHeadersDictionary = operationsHeaders;
    for (Entry<String, String> operationsHeader : operationsHeaders.entrySet()) {
      operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.getKey().hashCode();
      if (operationsHeader.getValue() != null) {
        operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.getKey().hashCode();
      }
    }
    return this;
  }

  /**
   * Adds the operation headers.
   * @param operationsHeaders
   * @return
   */
  public CreateHttpJsonRequestParams addOperationHeadersMultiMap(Map<String, List<String>> operationsHeaders) {
    urlCached = null;
    operationsHeadersCollection = operationsHeaders;
    for (String operationsHeader : operationsHeadersCollection.keySet()) {
      operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.hashCode();
      List<String> values = operationsHeaders.get(operationsHeader);
      if (values == null) {
        continue;
      }
      for (String header : values) {
        if (header != null) {
          operationHeadersHash = (operationHeadersHash * 397) ^ header.hashCode();
        }
      }
    }
    return this;
  }

  private String generateUrl() {
    if (operationHeadersHash == 0) {
      return url;
    }
    return url + (url.contains("?") ? "&" : "?") + "operationHeadersHash=" + operationHeadersHash;
  }

  /**
   * @return the convention
   */
  public DocumentConvention getConvention() {
    return convention;
  }

  /**
   * @return the metadata
   */
  public RavenJObject getMetadata() {
    return metadata;
  }

  /**
   * @return the method
   */
  public HttpMethods getMethod() {
    return method;
  }


  /**
   * @return the owner
   */
  public IHoldProfilingInformation getOwner() {
    return owner;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    if (urlCached != null) {
      return urlCached;
    }
    urlCached = generateUrl();
    return urlCached;
  }

  /**
   * @return the avoidCachingRequest
   */
  public boolean isAvoidCachingRequest() {
    return avoidCachingRequest;
  }

  /**
   * @return the disableRequestCompression
   */
  public boolean isDisableRequestCompression() {
    return disableRequestCompression;
  }

  /**
   * @param avoidCachingRequest the avoidCachingRequest to set
   */
  public CreateHttpJsonRequestParams setAvoidCachingRequest(boolean avoidCachingRequest) {
    this.avoidCachingRequest = avoidCachingRequest;
    return this;
  }

  /**
   * @param convention the convention to set
   */
  public void setConvention(DocumentConvention convention) {
    this.convention = convention;
  }

  /**
   * @param disableRequestCompression the disableRequestCompression to set
   */
  public void setDisableRequestCompression(boolean disableRequestCompression) {
    this.disableRequestCompression = disableRequestCompression;
  }

  /**
   * @param metadata the metadata to set
   */
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  /**
   * @param method the method to set
   */
  public void setMethod(HttpMethods method) {
    this.method = method;
  }

  /**
   * @param owner the owner to set
   */
  public void setOwner(IHoldProfilingInformation owner) {
    this.owner = owner;
  }

  public void updateHeaders(HttpRequest webRequest) {
    if (operationsHeadersDictionary != null) {
      for (Entry<String, String> kvp : operationsHeadersDictionary.entrySet()) {
        webRequest.addHeader(kvp.getKey(), kvp.getValue());
      }
    }
    if (operationsHeadersCollection != null) {
      for (Entry<String, List<String>> header : operationsHeadersCollection.entrySet()) {
        webRequest.addHeader(header.getKey(), StringUtils.join(header.getValue(), ","));
      }
    }
  }

}
