package net.ravendb.client.connection;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.commands.ICommandData;
import net.ravendb.abstractions.data.Attachment;
import net.ravendb.abstractions.data.AttachmentInformation;
import net.ravendb.abstractions.data.BatchResult;
import net.ravendb.abstractions.data.BulkInsertOptions;
import net.ravendb.abstractions.data.DatabaseStatistics;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetQuery;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.JsonDocumentMetadata;
import net.ravendb.abstractions.data.MoreLikeThisQuery;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.data.PutResult;
import net.ravendb.abstractions.data.QueryHeaderInformation;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.data.ScriptedPatchRequest;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.TransformerDefinition;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.RavenPagingInformation;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.connection.profiling.IHoldProfilingInformation;
import net.ravendb.client.document.ILowLevelBulkInsertOperation;
import net.ravendb.client.indexes.IndexDefinitionBuilder;


public interface IDatabaseCommands extends IHoldProfilingInformation {

  /**
   * Gets the operations headers
   * @return
   */
  public Map<String, String> getOperationsHeaders();

  /**
   * Admin operations for current database
   * @return
   */
  public IAdminDatabaseCommands getAdmin();

  /**
   * Admin operations performed against system database, like create/delete database
   */
  public IGlobalAdminDatabaseCommands getGlobalAdmin();

  /**
   * Sets the operations headers
   * @param operationsHeaders
   */
  public void setOperationsHeaders(Map<String, String> operationsHeaders);

  /**
   * Retrieves documents for the specified key prefix
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
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
   * Retrieves documents for the specified key prefix
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @param metadataOnly
   * @return
   */
  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly, String exclude);

  /**
   * Retrieves documents for the specified key prefix
   * @param keyPrefix
   * @param matches
   * @param start
   * @param pageSize
   * @param metadataOnly
   * @return
   */
  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly, String exclude, RavenPagingInformation pagingInformation);

  public List<JsonDocument> startsWith(String keyPrefix, String matches, int start, int pageSize, boolean metadataOnly,
    String exclude, RavenPagingInformation pagingInformation, String transformer, Map<String, RavenJToken> queryInputs);

  /**
   * Retrieves the document for the specified key
   * @param key The key
   * @return
   */
  public JsonDocument get(String key) throws ServerClientException;

  /**
   * Gets the results for the specified ids.
   * @param ids
   * @param includes
   * @return
   */
  public MultiLoadResult get(final String[] ids, final String[] includes);

  /**
   * Gets the results for the specified ids.
   * @param ids
   * @param includes
   * @param transformer
   * @return
   */
  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer);

  /**
   * Gets the results for the specified ids.
   * @param ids
   * @param includes
   * @param transformer
   * @param queryInputs
   * @return
   */
  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer, final Map<String, RavenJToken> queryInputs);

  /**
   * @param ids
   * @param includes
   * @param transformer
   * @param queryInputs
   * @param metadataOnly
   * @return
   */
  public MultiLoadResult get(final String[] ids, final String[] includes, final String transformer, final Map<String, RavenJToken> queryInputs, final boolean metadataOnly);


  /**
   * Seeds the next identity value on the server
   * @param name
   * @param value
   * @return
   */
  public long seedIdentityFor(final String name, final long value) ;

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
   * Puts the document in the database with the specified key
   * @param key The key.
   * @param guid The etag.
   * @param document The document.
   * @param metadata The metadata.
   * @return PutResult
   */
  public PutResult put(String key, Etag guid, RavenJObject document, RavenJObject metadata);

  /**
   * Deletes the document with the specified key
   * @param key The key.
   * @param etag The etag.
   */
  public void delete(String key, Etag etag);

  /**
   * Puts a byte array as attachment with the specified key
   * @param key The key.
   * @param etag The etag.
   * @param data The data.
   * @param metadata The metadata.
   */
  public void putAttachment(String key, Etag etag, InputStream data, RavenJObject metadata);

  /**
   *  Updates just the attachment with the specified key's metadata
   * @param key The key.
   * @param etag The etag.
   * @param metadata The metadata.
   */
  public void updateAttachmentMetadata(String key, Etag etag, RavenJObject metadata);

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
  public void deleteAttachment(String key, Etag etag);

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
   * Gets the index names from the server
   * @param start
   * @param pageSize
   * @return
   */
  public Collection<String> getIndexNames(final int start, final int pageSize) ;

  /**
   * Returns {@link IndexDefinition}s
   * @param start
   * @param pageSize
   * @return
   */
  public Collection<IndexDefinition> getIndexes(final int start, final int pageSize);

  /**
   * Resets the specified index
   * @param name
   */
  public void resetIndex(String name);

  /**
   * Gets the index definition for the specified name
   * @param name
   * @return
   */
  public IndexDefinition getIndex(String name);

  /**
   * Puts index with given definition
   * @param name
   * @param definition
   * @return
   */
  public String putIndex(String name, IndexDefinition definition);

  /**
   * Creates a transformer with the specified name, based on an transformer definition
   * @param name
   * @param indexDef
   * @return
   */
  public String putTransformer(String name, TransformerDefinition transformerDef);

  /**
   * Puts the index.
   * @param name
   * @param definition
   * @param overwrite
   * @return
   */
  public String putIndex(final String name, final IndexDefinition definition, final boolean overwrite);

  /**
   * Creates an index with the specified name, based on an index definition
   * @param name
   * @param indexDef
   * @return
   */
  public String putIndex(String name, IndexDefinitionBuilder indexDef);

  /**
   * Creates an index with the specified name, based on an index definition
   * @param name
   * @param indexDef
   * @param overwrite
   * @return
   */
  public String putIndex(String name, IndexDefinitionBuilder indexDef, boolean overwrite);

  /**
   * Performs index query
   * @param index
   * @param query
   * @param includes
   * @return
   */
  public QueryResult query(String index, IndexQuery query, String[] includes);

  /**
   * Performs index query
   * @param index
   * @param query
   * @param includes
   * @return
   */
  public QueryResult query(String index, IndexQuery query, String[] includes, boolean metadataOnly);

  /**
   * Performs index query
   * @param index
   * @param query
   * @param includes
   * @return
   */
  public QueryResult query(String index, IndexQuery query, String[] includes, boolean metadataOnly, boolean indexEntriesOnly);

  /**
   *  Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
   *  of the number of items that might be returned.
   * @param index
   * @param query
   * @param queryHeaderInfo
   * @return
   */
  public RavenJObjectIterator streamQuery(String index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo) ;

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @return
   */
  public RavenJObjectIterator streamDocs();

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith, String matches);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith, String matches, int start);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith, String matches, int start, int pageSize);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @param pageSize
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith, String matches, int start, int pageSize, String exclude);

  /**
   * Streams the documents by etag OR starts with the prefix and match the matches
   * Will return *all* results, regardless of the number of items that might be returned.
   * @param fromEtag
   * @param startsWith
   * @param matches
   * @param start
   * @param pageSize
   * @param pagingInformation
   * @return
   */
  public RavenJObjectIterator streamDocs(Etag fromEtag, String startsWith, String matches, int start, int pageSize, String exclude, RavenPagingInformation pagingInformation);

  /**
   * Delete index
   * @param name
   */
  public void deleteIndex(final String name);

  /**
   *  Executed the specified commands as a single batch
   * @param commandDatas
   * @return
   */
  public BatchResult[] batch(final List<ICommandData> commandDatas);

  /**
   * Perform a set based deletes using the specified index.
   * @param indexName
   * @param queryToDelete
   * @param allowStale
   * @return
   */
  public Operation deleteByIndex(final String indexName, final IndexQuery queryToDelete, final boolean allowStale);

  /**
   * Perform a set based deletes using the specified index, not allowing the operation
   *  if the index is stale
   * @param indexName
   * @param queryToDelete
   * @return
   */
  public Operation deleteByIndex(String indexName, IndexQuery queryToDelete);

  /**
   * Perform a set based update using the specified index, not allowing the operation
   * if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patchRequests
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests);

  /**
   * Perform a set based update using the specified index, not allowing the operation
   * if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patch
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch);

  /**
   * Perform a set based update using the specified index, not allowing the operation
   * if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patchRequests
   * @param allowStale
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, boolean allowStale);

  /**
   * Perform a set based update using the specified index, not allowing the operation
   * if the index is stale
   * @param indexName
   * @param queryToUpdate
   * @param patch
   * @param allowStale
   * @return
   */
  public Operation updateByIndex(String indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, boolean allowStale);

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
   * Returns a list of suggestions based on the specified suggestion query
   * @param index
   * @param suggestionQuery
   * @return
   */
  public SuggestionQueryResult suggest(final String index, final SuggestionQuery suggestionQuery);

  /**
   * Return a list of documents that based on the MoreLikeThisQuery.
   * @param query
   * @return
   */
  public MultiLoadResult moreLikeThis(MoreLikeThisQuery query);

  /**
   * Get the all terms stored in the index for the specified field
   * You can page through the results by use fromValue parameter as the
   * starting point for the next query
   * @param index
   * @param field
   * @param fromValue
   * @param pageSize
   * @return
   */
  public List<String> getTerms(String index, String field, String fromValue, int pageSize);

  /**
   * Sends a multiple faceted queries in a single request and calculates the facet results for each of them
   * @param facetedQueries
   * @return
   */
  public FacetResults[] getMultiFacets(FacetQuery[] facetedQueries);

  /**
   *  Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facetSetupDoc
   * @param start
   * @return
   */
  public FacetResults getFacets(String index, IndexQuery query, String facetSetupDoc);

  /**
   *  Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facetSetupDoc
   * @param start
   * @return
   */
  public FacetResults getFacets(String index, IndexQuery query, String facetSetupDoc, int start);

  /**
   *  Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facetSetupDoc
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(String index, IndexQuery query, String facetSetupDoc, int start, Integer pageSize);

  /**
   * Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
   * @param index
   * @param query
   * @param facets
   * @param start
   * @param pageSize
   * @return
   */
  public FacetResults getFacets(final String index, final IndexQuery query, final List<Facet> facets, final int start, final Integer pageSize) ;

  /**
   *  Sends a patch request for a specific document, ignoring the document's Etag and if the document is missing
   * @param key
   * @param patches
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches);

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patches
   * @param ignoreMissing
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches, boolean ignoreMissing);

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag and  if the document is missing
   * @param key
   * @param patch
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch);

  /**
   * Sends a patch request for a specific document, ignoring the document's Etag
   * @param key
   * @param patch
   * @param ignoreMissing
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch, boolean ignoreMissing);

  /**
   * Sends a patch request for a specific document
   * @param key
   * @param patches
   * @param etag
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patches, Etag etag);

  /**
   * Sends a patch request for a specific document which may or may not currently exist
   * @param key
   * @param patchesToExisting
   * @param patchesToDefault
   * @param defaultMetadata
   * @return
   */
  public RavenJObject patch(String key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata);

  /**
   *  Sends a patch request for a specific document
   * @param key
   * @param patch
   * @param etag
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patch, Etag etag);

  /**
   * Sends a patch request for a specific document which may or may not currently exist
   * @param key
   * @param patchExisting
   * @param patchDefault
   * @param defaultMetadata
   * @return
   */
  public RavenJObject patch(String key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata);

  /**
   * Disable all caching within the given scope
   * @return
   */
  public AutoCloseable disableAllCaching();

  /**
   * Perform a single POST request containing multiple nested GET requests
   * @param requests
   * @return
   */
  public GetResponse[] multiGet(GetRequest[] requests);

  /**
   * Returns server statistics
   * @return
   */
  public DatabaseStatistics getStatistics();

  /**
   * Checks if the document exists for the specified key
   * @param key The key.
   * @return
   */
  public JsonDocumentMetadata head(String key);

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
   * Force the database commands to read directly from the master, unless there has been a failover.
   */
  public AutoCloseable forceReadFromMaster();

  public ILowLevelBulkInsertOperation getBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes);

  /**
   * Gets the transformers from the server
   * @param start
   * @param pageSize
   * @return
   */
  public Collection<TransformerDefinition> getTransformers(int start, int pageSize);

  /**
   *  Gets the transformer definition for the specified name
   * @param name
   * @return
   */
  public TransformerDefinition getTransformer(String name);

  /**
   * Deletes the specified transformer
   * @param name
   */
  public void deleteTransformer(String name);

  public AttachmentInformation[] getAttachments(Etag startEtag, int batchSize);

  /**
   * Tries to resolve conflict using registered listeners
   * @param key
   * @param etag
   * @param conflictedIds
   * @param opUrl
   * @return
   */
  public Boolean tryResolveConflictByUsingRegisteredListeners(String key, Etag etag, String[] conflictedIds,
    OperationMetadata opUrl);


}
