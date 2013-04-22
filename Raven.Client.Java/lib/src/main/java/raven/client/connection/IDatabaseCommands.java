package raven.client.connection;

import org.codehaus.jackson.JsonNode;

import raven.client.json.Guid;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;

public interface IDatabaseCommands {
  /**
   * Retrieves the document for the specified key
   * @param key The key
   * @return
   */
  public JsonDocument get(String key);

  /**
   * Puts the document in the database with the specified key
   * @param key The key.
   * @param guid The etag.
   * @param document The document.
   * @param metadata The metadata.
   * @return PutResult
   */
  public PutResult put(String key, Guid guid, JsonNode document, JsonNode metadata);

  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, Guid etag);
}
