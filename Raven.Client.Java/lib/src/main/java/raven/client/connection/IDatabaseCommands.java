package raven.client.connection;

import raven.client.common.data.Guid;
import raven.client.common.data.JsonDocument;
import raven.client.common.data.PutResult;
import raven.client.common.json.RavenJObject;

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
  public PutResult put(String key, Guid guid, RavenJObject document, RavenJObject metadata);

  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, Guid etag);
}
