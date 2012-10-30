//-----------------------------------------------------------------------
// <copyright file="IAsyncDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
	/// <summary>
	/// An async database command operations
	/// </summary>
	public interface IAsyncDatabaseCommands : IDisposable, IHoldProfilingInformation
	{
		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		IDictionary<string, string> OperationsHeaders { get; }

		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		Task<JsonDocument> GetAsync(string key);

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, bool metadataOnly = false);

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
		/// Puts the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite);

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		Task DeleteIndexAsync(string name);

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale);

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
		Task<PutResult> PutAsync(string key, Guid? etag, RavenJObject document, RavenJObject metadata);

#if SILVERLIGHT
		/// <summary>
		/// Create a http request to the specified relative url on the current database
		/// </summary>
		Silverlight.Connection.HttpJsonRequest CreateRequest(string relativeUrl, string method);
#endif

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IAsyncDatabaseCommands ForDatabase(string database);

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the default database
		/// </summary>
		IAsyncDatabaseCommands ForDefaultDatabase();

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
		/// Puts the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		Task PutAttachmentAsync(string key, Guid? etag, byte[] data, RavenJObject metadata);

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
		Task DeleteAttachmentAsync(string key, Guid? etag);

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize);

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		Task EnsureSilverlightStartUpAsync();

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
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale);

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc);

		Task<LogItem[]> GetLogsAsync(bool errorsOnly);

		Task<LicensingStatus> GetLicenseStatusAsync();

		Task<BuildNumber> GetBuildNumberAsync();

		Task StartBackupAsync(string backupLocation);

		Task StartRestoreAsync(string restoreLocation, string databaseLocation);

		Task StartIndexingAsync();

		Task StopIndexingAsync();

		Task<string> GetIndexingStatusAsync();

		Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize, bool metadataOnly = false);

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		void ForceReadFromMaster();
	}
}
#endif
