package raven.client.connection;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import raven.abstractions.data.Attachment;
import raven.abstractions.data.DatabaseStatistics;
import raven.abstractions.data.Etag;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.QueryResult;
import raven.abstractions.data.SuggestionQuery;
import raven.abstractions.data.SuggestionQueryResult;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.json.linq.RavenJObject;

//TODO: expose all methods
public interface IDatabaseCommands {
  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, Etag etag);


  /**
   * Returns server statistics
   * @return
   */
  public DatabaseStatistics getStatistics();

  /**
   * Performs index query
   * @param index
   * @param query
   * @param includes
   * @return
   */
  public QueryResult query(String index, IndexQuery query, String[] includes);
  /**
   * Deletes the attachment with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void deleteAttachment(String key, Etag etag);

  /**
   * Delete index
   * @param name
   */
  public void deleteIndex(final String name);

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
   * Gets the attachment by the specified key
   * @param key
   * @return
   */
  public Attachment getAttachment(String key);

  /**
   * Gets the attachments starting with the specified prefix
   * @param idPrefix
   * @param start
   * @param pageSize
   * @return
   */
  public List<Attachment> getAttachmentHeadersStartingWith(String idPrefix, int start, int pageSize);

  /**
   * Returns list of database names
   * @param pageSize
   * @return
   */
  List<String> getDatabaseNames(int pageSize);

  /**
   * Returns list of database names
   * @param pageSize
   * @param start
   * @return
   */
  public List<String> getDatabaseNames(int pageSize, int start);

  /**
   * Get documents from server
   * @param start
   * @param pageSize
   * @return
   */
  public List<JsonDocument> getDocuments(int start, int pageSize);

  /**
   * Get documents from server
   * @param start Paging start
   * @param pageSize Size of the page.
   * @param metadataOnly Load just the document metadata
   * @return
   */
  public List<JsonDocument> getDocuments(int start, int pageSize, boolean metadataOnly);

  /**
   * Checks if the document exists for the specified key
   * @param key The key.
   * @return
   */
  public JsonDocumentMetadata head(String key);

  /**
   * Retrieves the attachment metadata with the specified key, not the actual attachment
   * @param key
   * @return
   */
  public Attachment headAttachment(String key);

  /**
   * Generate the next identity value from the server
   * @param name
   * @return
   */
  public Long nextIdentityFor(String name);

  /**
   * Puts the document in the database with the specified key
   * @param key The key.
   * @param guid The etag.
   * @param document The document.
   * @param metadata The metadata.
   * @return PutResult
   */
  public PutResult put(String key, Etag guid, RavenJObject document, RavenJObject metadata);
  /**
   * Returns {@link IndexDefinition}s
   * @param start
   * @param pageSize
   * @return
   */
  public Collection<IndexDefinition> getIndexes(final int start, final int pageSize);

  /**
   * Gets the index definition for the specified name
   * @param name
   * @return
   */
  public IndexDefinition getIndex(String name);

  /**
   * Gets the index names from the server
   * @param start
   * @param pageSize
   * @return
   */
  public Collection<String> getIndexNames(final int start, final int pageSize) ;

  /**
   * Puts a byte array as attachment with the specified key
   * @param key The key.
   * @param etag The etag.
   * @param data The data.
   * @param metadata The metadata.
   */
  public void putAttachment(String key, Etag etag, InputStream data, RavenJObject metadata);

  /**
   * Puts index with given definition
   * @param name
   * @param definition
   * @return
   */
  public String putIndex(String name, IndexDefinition definition);

  /**
   * Puts the index.
   * @param name
   * @param definition
   * @param overwrite
   * @return
   */
  public String putIndex(final String name, final IndexDefinition definition, final boolean overwrite);

  /**
   * Retrieves documents for the specified key prefix
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @param metadataOnly
   * @return
   */
  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize) throws ServerClientException;

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
   *  Updates just the attachment with the specified key's metadata
   * @param key The key.
   * @param etag The etag.
   * @param metadata The metadata.
   */
  public void updateAttachmentMetadata(String key, Etag etag, RavenJObject metadata);

  /**
   * Get the full URL for the given document key
   * @param documentKey
   * @return
   */
  public String urlFor(String documentKey);

  /**
   * Resets the specified index
   * @param name
   */
  void resetIndex(String name);

  /**
   * Returns a list of suggestions based on the specified suggestion query
   * @param index
   * @param suggestionQuery
   * @return
   */
  public SuggestionQueryResult suggest(final String index, final SuggestionQuery suggestionQuery);

}
