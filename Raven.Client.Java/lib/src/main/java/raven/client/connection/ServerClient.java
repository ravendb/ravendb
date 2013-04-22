package raven.client.connection;

import org.apache.commons.httpclient.HttpClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;

import raven.client.json.Guid;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;


public class ServerClient implements IDatabaseCommands {

  private String url;
  private String databaseName;
  private HttpClient httpClient;

  public ServerClient(String url, String databaseName) {
    super();
    this.url = url;
    this.databaseName = databaseName;
    httpClient = new HttpClient();
  }

  @Override
  public void delete(String key, Guid etag) {
  }

  @Override
  public JsonDocument get(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Perform a direct get for a document with the specified key on the specified server URL.
   * @param serverUrl
   * @param key
   * @return
   */
  private JsonDocument directGet(String serverUrl, String key) {
    //TODO:
    return null;
  }

  /**
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  @Override
  public PutResult put(String key, Guid guid, JsonNode document, JsonNode metadata) {
    // TODO Auto-generated method stub
    return null;
  }

}
