package raven.client.connection;

import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import raven.abstractions.data.Attachment;
import raven.abstractions.data.MultiLoadResult;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;
import raven.client.json.RavenJObject;
import raven.client.json.lang.ServerClientException;

public interface IDatabaseCommands {
  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, UUID etag);
  /**
   * Generate the next identity value from the server
   * @param name
   * @return
   */
  public Long nextIdentityFor(String name);

  /**
   * Get the full URL for the given document key
   * @param documentKey
   * @return
   */
  public String urlFor(String documentKey);

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

  /**
   * Retrieves the document for the specified key
   * @param key The key
   * @return
   */
  public JsonDocument get(String key) throws ServerClientException;

  /**
   * Get documents from server
   * @param start Paging start
   * @param pageSize Size of the page.
   * @param metadataOnly Load just the document metadata
   * @return
   */
  public List<JsonDocument> getDocuments(int start, int pageSize, boolean metadataOnly);

  /**
   * Puts the document in the database with the specified key
   * @param key The key.
   * @param guid The etag.
   * @param document The document.
   * @param metadata The metadata.
   * @return PutResult
   */
  public PutResult put(String key, UUID guid, RavenJObject document, RavenJObject metadata);

  /**
   * Puts a byte array as attachment with the specified key
   * @param key The key.
   * @param etag The etag.
   * @param data The data.
   * @param metadata The metadata.
   */
  public void putAttachment(String key, UUID etag, InputStream data, RavenJObject metadata);

  /**
   * Retrieves documents for the specified key prefix
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @param metadataOnly
   * @return
   */
  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly);

  /**
   * Gets the results for the specified ids.
   * @param ids The ids.
   * @param includes The includes.
   * @param metadataOnly Load just the document metadata.
   * @return
   */
  public MultiLoadResult get(String[] ids, String[] includes, boolean metadataOnly);

  /**
   *  Updates just the attachment with the specified key's metadata
   * @param key The key.
   * @param etag The etag.
   * @param metadata The metadata.
   */
  public void updateAttachmentMetadata(String key, UUID etag, RavenJObject metadata);

  /**
   * Gets the attachments starting with the specified prefix
   * @param idPrefix
   * @param start
   * @param pageSize
   * @return
   */
  public List<Attachment> getAttachmentHeadersStartingWith(String idPrefix, int start, int pageSize);

  /**
   * Gets the attachment by the specified key
   * @param key
   * @return
   */
  public Attachment getAttachment(String key);

  /**
   * Retrieves the attachment metadata with the specified key, not the actual attachment
   * @param key
   * @return
   */
  public Attachment headAttachment(String key);

  /**
   * Deletes the attachment with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void deleteAttachment(String key, UUID etag);

  /**
   * Returns list of database names
   * @param pageSize
   * @param start
   * @return
   */
  public List<String> getDatabaseNames(int pageSize, int start);

}
