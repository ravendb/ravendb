package raven.client.connection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import raven.abstractions.closure.Action3;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.json.RavenJObject;
import raven.client.document.DocumentConvention;
import raven.client.document.FailoverBehavior;

//TODO: hash based cache
public class CreateHttpJsonRequestParams implements Serializable {
  private HttpMethods method;
  private String url;
  private ServerClient serverClient;
  private RavenJObject metadata;
  private Credentials credentials;
  private DocumentConvention convention;

  private Map<String, String> operationHeaders = new HashMap<>();

  /**
   * @return the serverClient
   */
  public ServerClient getServerClient() {
    return serverClient;
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
   * @return the credentials
   */
  public Credentials getCredentials() {
    return credentials;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @return the method
   */
  public HttpMethods getMethod() {
    return method;
  }



  public CreateHttpJsonRequestParams(ServerClient serverClient, String url, HttpMethods method, RavenJObject metadata, Credentials credentials, DocumentConvention convention) {
    super();
    this.method = method;
    this.url = url;
    this.serverClient = serverClient;
    this.metadata = metadata;
    this.credentials = credentials;
    this.convention = convention;
  }

  public CreateHttpJsonRequestParams addOperationHeaders(Map<String, String> operationsHeaders) {
    this.operationHeaders = operationsHeaders;//TODO: implement me!
    return this;
  }

  public CreateHttpJsonRequestParams addReplicationStatusHeaders(String thePrimaryUrl, String currentUrl,
      ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action3<Map<String, String>, String, String> handleReplicationStatusChanges) {
    //TODO: implement me!
    return this;
  }

}
