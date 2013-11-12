//-----------------------------------------------------------------------
// <copyright file="IAsyncDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Profiling;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
	/// <summary>
	/// An async database command operations
	/// </summary>
	public interface IAsyncDatabaseCommands : IDisposable, IHoldProfilingInformation
	{
		/// <summary>
		/// Gets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		IDictionary<string, string> OperationsHeaders { get; }

		/// <summary>
		/// Admin operations performed against system database, like create/delete database
		/// </summary>
		IAsyncGlobalAdminDatabaseCommands GlobalAdmin { get; }

		/// <summary>
		/// Admin operations for current database
		/// </summary>
		IAsyncAdminDatabaseCommands Admin { get; }

		IAsyncInfoDatabaseCommands Info { get; }

		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		Task<JsonDocument> GetAsync(string key);

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, string transformer = null, Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false);

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false);

        /// <summary>
        /// Begins an async get operation for documents
        /// </summary>
        /// <param name="fromEtag">The ETag of the first document to start with</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <param name="metadataOnly">Load just the document metadata</param>
        /// <remarks>
        /// This is primarily useful for administration of a database
        /// </remarks>
        Task<JsonDocument[]> GetDocumentsAsync(Etag fromEtag, int pageSize, bool metadataOnly = false);

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes, bool metadataOnly = false);

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas);

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery);

		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		Task<string[]> GetIndexNamesAsync(int start, int pageSize);

		/// <summary>
		/// Gets the indexes from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize);

		/// <summary>
		/// Gets the transformers from the server asynchronously
		/// </summary>
		Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize);

		/// <summary>
		/// Resets the specified index asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task ResetIndexAsync(string name);

		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task<IndexDefinition> GetIndexAsync(string name);

		/// <summary>
		/// Gets the transformer definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task<TransformerDefinition> GetTransformerAsync(string name);

		/// <summary>
		/// Puts the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite);

		/// <summary>
		/// Puts the transformer definition for the specified name asynchronously
		/// </summary>
		Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition);

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task DeleteIndexAsync(string name);

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete);

		/// <summary>
		/// Perform a set based deletes using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale);

		/// <summary>
		/// Deletes the transformer definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task DeleteTransformerAsync(string name);

		/// <summary>
		/// Deletes the document for the specified id asynchronously
		/// </summary>
		/// <param name="id">The id.</param>
		Task DeleteDocumentAsync(string id);

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		Task<PutResult> PutAsync(string key, Etag etag, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, Etag etag);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, bool ignoreMissing);

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		Task<RavenJObject> PatchAsync(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, bool ignoreMissing);

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, Etag etag);

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata);

		/// <summary>
		/// Create a http request to the specified relative url on the current database
		/// </summary>
		HttpJsonRequest CreateRequest(string relativeUrl, string method, bool disableRequestCompression = false);

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IAsyncDatabaseCommands ForDatabase(string database);

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the default database
		/// </summary>
		IAsyncDatabaseCommands ForSystemDatabase();

		/// <summary>
		/// Returns a new <see cref="IAsyncDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		IAsyncDatabaseCommands With(ICredentials credentialsForSession);

		/// <summary>
		/// Retrieve the statistics for the database asynchronously
		/// </summary>
		Task<DatabaseStatistics> GetStatisticsAsync();

		/// <summary>
		/// Gets the list of databases from the server asynchronously
		/// </summary>
		Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0);

        /// <summary>
        /// Gets the attachments asynchronously
        /// </summary>
        /// <returns></returns>
        Task<AttachmentInformation[]> GetAttachmentsAsync(Etag startEtag, int batchSize);

		/// <summary>
		/// Puts the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="stream">The data stream.</param>
		/// <param name="metadata">The metadata.</param>
		Task PutAttachmentAsync(string key, Etag etag, Stream stream, RavenJObject metadata);

		/// <summary>
		/// Gets the attachment by the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		Task<Attachment> GetAttachmentAsync(string key);

		/// <summary>
		/// Deletes the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		Task DeleteAttachmentAsync(string key, Etag etag);

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize);

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		IDisposable DisableAllCaching();

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		Task<GetResponse[]> MultiGetAsync(GetRequest[] requests);

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch);

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale);

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null);

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facets">List of facets</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start, int? pageSize);

		/// <summary>
		/// Gets the Logs
		/// </summary>
		Task<LogItem[]> GetLogsAsync(bool errorsOnly);

		/// <summary>
		/// Gets the license Status
		/// </summary>
		Task<LicensingStatus> GetLicenseStatusAsync();

		/// <summary>
		/// Gets the build number
		/// </summary>
		Task<BuildNumber> GetBuildNumberAsync();

		/// <summary>
		/// Get documents with id of a specific prefix
		/// </summary>
		Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize, bool metadataOnly = false, string exclude = null);

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		void ForceReadFromMaster();

		/// <summary>
		/// Retrieves the document metadata for the specified document key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns>The document metadata for the specified document, or null if the document does not exist</returns>
		Task<JsonDocumentMetadata> HeadAsync(string key);

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo);

		/// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
		Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue);

#if SILVERLIGHT
		/// <summary>
		/// Get the low level  bulk insert operation
		/// </summary>
		ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes);
#endif

	}

	public interface IAsyncGlobalAdminDatabaseCommands
	{
		/// <summary>
		/// Get admin statistics
		/// </summary>
		/// <returns></returns>
		Task<AdminStatistics> GetStatisticsAsync();

		/// <summary>
		/// Sends an async command to create a database
		/// </summary>
		Task CreateDatabaseAsync(DatabaseDocument databaseDocument);

		/// <summary>
		/// Sends an async command to delete a database
		/// </summary>
		Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false);

		/// <summary>
		/// Sends an async command to compact a database. During the compaction the specified database will be offline.
		/// </summary>
		Task CompactDatabaseAsync(string databaseName);
	}

	public interface IAsyncAdminDatabaseCommands
	{
		/// <summary>
		/// Sends an async command that disables all indexing
		/// </summary>
		Task StopIndexingAsync();

		/// <summary>
		/// Sends an async command that enables indexing
		/// </summary>
		Task StartIndexingAsync();

		/// <summary>
		/// Begins an async backup operation
		/// </summary>
		Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument);

		/// <summary>
		/// Begins an async restore operation
		/// </summary>
		Task StartRestoreAsync(string restoreLocation, string databaseLocation, string databaseName = null, bool defrag = false);

		/// <summary>
		/// Get the indexing status
		/// </summary>
		Task<string> GetIndexingStatusAsync();
	}

	public interface IAsyncInfoDatabaseCommands
	{
		/// <summary>
		/// Get replication info
		/// </summary>
		/// <returns></returns>
		Task<ReplicationStatistics> GetReplicationInfoAsync();
	}
}
