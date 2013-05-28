package raven.client.connection;

import java.util.UUID;

import raven.client.json.JsonDocument;
import raven.client.json.PutResult;
import raven.client.json.RavenJObject;
import raven.client.json.lang.ServerClientException;

public interface IDatabaseCommands {
  /**
   * Retrieves the document for the specified key
   * @param key The key
   * @return
   */
  public JsonDocument get(String key) throws ServerClientException;

  /**
   * Puts the document in the database with the specified key
   * @param key The key.
   * @param guid The etag.
   * @param document The document.
   * @param metadata The metadata.
   * @return PutResult
   */
  public PutResult put(String key, UUID guid, RavenJObject document, RavenJObject metadata) throws ServerClientException;

  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, UUID etag) throws ServerClientException;

  /**
   * Create a new instance of {@link IDatabaseCommands} that will interacts with the specified database
   * @param database
   * @return
   */
  public IDatabaseCommands forDatabase(String database);

  /**
   * Creates a new instance of {@link IDatabaseCommands} that will interacts with the default database.
   */
  public IDatabaseCommands forSystemDatabase();
}
