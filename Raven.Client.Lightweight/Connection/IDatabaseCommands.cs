using Raven.Abstractions.Connection;
using Raven.Database.Data;
//-----------------------------------------------------------------------
// <copyright file="IDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Changes;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	///<summary>
	/// Expose the set of operations by the RavenDB server
	///</summary>
	public interface IDatabaseCommands : IHoldProfilingInformation
	{
		/// <summary>
		/// Gets or sets the operations headers
		/// </summary>
		/// <value>The operations headers.</value>
		NameValueCollection OperationsHeaders { get; set; }

		/// <summary>
		/// Admin operations for current database
		/// </summary>
		IAdminDatabaseCommands Admin { get; }

		/// <summary>
		/// Admin operations performed against system database, like create/delete database
		/// </summary>
		IGlobalAdminDatabaseCommands GlobalAdmin { get; }

		/// <summary>
		/// Primary credentials for access. Will be used also in replication context - for failovers
		/// </summary>
		OperationCredentials PrimaryCredentials { get; }

		/// <summary>
		/// Retrieves documents for the specified key prefix.
		/// </summary>
		/// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
		/// <param name="matches">pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?' any single character, '*' any characters)</param>
		/// <param name="start">number of documents that should be skipped</param>
		/// <param name="pageSize">maximum number of documents that will be retrieved</param>
		/// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
		/// <param name="metadataOnly">specifies if only document metadata should be returned</param>
		/// <param name="exclude">pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched ('?' any single character, '*' any characters)</param>
		/// <param name="transformer">name of a transformer that should be used to transform the results</param>
		/// <param name="transformerParameters">parameters that will be passed to transformer</param>
		/// <param name="skipAfter">skip document fetching until given key is found and return documents after that key (default: null)</param>
		JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize,
		                          RavenPagingInformation pagingInformation = null, bool metadataOnly = false,
		                          string exclude = null, string transformer = null,
		                          Dictionary<string, RavenJToken> transformerParameters = null,
								  string skipAfter = null);

		/// <summary>
		/// Retrieve a single document for a specified key.
		/// </summary>
		/// <param name="key">key of the document you want to retrieve</param>
		JsonDocument Get(string key);

	    /// <summary>
	    /// Retrieves documents with the specified ids, optionally specifying includes to fetch along and also optionally the transformer.
		/// <para>Returns MultiLoadResult where:</para>
		/// <para>- Results - list of documents in exact same order as in keys parameter</para>
		/// <para>- Includes - list of documents that were found in specified paths that were passed in includes parameter</para>
	    /// </summary>
		/// <param name="ids">array of keys of the documents you want to retrieve</param>
		/// <param name="includes">array of paths in documents in which server should look for a 'referenced' document</param>
		/// <param name="transformer">name of a transformer that should be used to transform the results</param>
		/// <param name="transformerParameters">parameters that will be passed to transformer</param>
		/// <param name="metadataOnly">specifies if only document metadata should be returned</param>
	    MultiLoadResult Get(string[] ids, string[] includes, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false);

		/// <summary>
		/// Retrieves multiple documents.
		/// </summary>
		/// <param name="start">number of documents that should be skipped</param>
		/// <param name="pageSize">maximum number of documents that will be retrieved</param>
		/// <param name="metadataOnly">specifies if only document metadata should be returned</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false);

		/// <summary>
		/// Puts the document in the database with the specified key.
		/// <para>Returns PutResult where:</para>
		/// <para>- Key - unique key under which document was stored,</para>
		/// <para>- Etag - stored document etag</para>
		/// </summary>
		/// <param name="key">unique key under which document will be stored</param>
		/// <param name="etag">current document etag, used for concurrency checks (null to skip check)</param>
		/// <param name="document">document data</param>
		/// <param name="metadata">document metadata</param>
        PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Deletes the document with the specified key
		/// </summary>
		/// <param name="key">key of a document to be deleted</param>
		/// <param name="etag">current document etag, used for concurrency checks (null to skip check)</param>
        void Delete(string key, Etag etag);

		/// <summary>
		/// Puts a byte array as attachment with the specified key
		/// </summary>
		/// <param name="key">unique key under which attachment will be stored</param>
		/// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
		/// <param name="data">attachment data</param>
		/// <param name="metadata">attachment metadata</param>
        [Obsolete("Use RavenFS instead.")]
        void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata);

		/// <summary>
		/// Updates attachments metadata only.
		/// </summary>
		/// <param name="key">key under which attachment is stored</param>
		/// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
		/// <param name="metadata">attachment metadata</param>
        [Obsolete("Use RavenFS instead.")]
        void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata);

		/// <summary>
		/// Downloads a single attachment.
		/// </summary>
		/// <param name="key">key of the attachment you want to download</param>
        [Obsolete("Use RavenFS instead.")]
        Attachment GetAttachment(string key);

		/// <summary>
		/// Downloads attachment metadata for a multiple attachments.
		/// </summary>
		/// <param name="idPrefix">prefix for which attachments should be returned</param>
		/// <param name="start">number of attachments that should be skipped</param>
		/// <param name="pageSize">maximum number of attachments that will be returned</param>
        [Obsolete("Use RavenFS instead.")]
        IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize);

		/// <summary>
		/// Download attachment metadata for a single attachment.
		/// </summary>
		/// <param name="key">key of the attachment you want to download metadata for</param>
        [Obsolete("Use RavenFS instead.")]
        Attachment HeadAttachment(string key);

		/// <summary>
		/// Removes an attachment from a database.
		/// </summary>
		/// <param name="key">key of an attachment to delete</param>
		/// <param name="etag">current attachment etag, used for concurrency checks (null to skip check)</param>
        [Obsolete("Use RavenFS instead.")]
        void DeleteAttachment(string key, Etag etag);

		/// <summary>
		/// Retrieves multiple index names from a database.
		/// </summary>
		/// <param name="start">number of index names that should be skipped</param>
		/// <param name="pageSize">maximum number of index names that will be retrieved</param>
		/// <returns></returns>
		string[] GetIndexNames(int start, int pageSize);

		/// <summary>
		/// Retrieves multiple index definitions from a database
		/// </summary>
		/// <param name="start">number of indexes that should be skipped</param>
		/// <param name="pageSize">maximum number of indexes that will be retrieved</param>
		IndexDefinition[] GetIndexes(int start, int pageSize);

		/// <summary>
		/// Removes all indexing data from a server for a given index so the indexation can start from scratch for that index.
		/// </summary>
		/// <param name="name">name of an index to reset</param>
		void ResetIndex(string name);

		/// <summary>
		/// Retrieves an index definition from a database.
		/// </summary>
		/// <param name="name">name of an index</param>
		IndexDefinition GetIndex(string name);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition
		/// </summary>
		/// <param name="name">name of an index</param>
		/// <param name="indexDef">definition of an index</param>
		string PutIndex(string name, IndexDefinition indexDef);

        /// <summary>
		/// Lets you check if the given index definition differs from the one on a server.
		/// <para>This might be useful when you want to check the prior index deployment, if index will be overwritten, and if indexing data will be lost.</para>
		/// <para>Returns:</para>
		/// <para>- <c>true</c> - if an index does not exist on a server</para>
		/// <para>- <c>true</c> - if an index definition does not match the one from the indexDef parameter,</para>
		/// <para>- <c>false</c> - if there are no differences between an index definition on server and the one from the indexDef parameter</para>
        /// If index does not exist this method returns true.
        /// </summary>
		/// <param name="name">name of an index to check</param>
		/// <param name="indexDef">index definition</param>
        bool IndexHasChanged(string name, IndexDefinition indexDef);

		/// <summary>
		/// Creates a transformer with the specified name, based on an transformer definition
		/// </summary>
		string PutTransformer(string name, TransformerDefinition transformerDef);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition
		/// </summary>
		/// <param name="name">name of an index</param>
		/// <param name="indexDef">definition of an index</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		string PutIndex(string name, IndexDefinition indexDef, bool overwrite);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition that is created by the supplied
		/// IndexDefinitionBuilder
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <returns></returns>
		string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition that is created by the supplied
		/// IndexDefinitionBuilder
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite);

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes.</param>
		QueryResult Query(string index, IndexQuery query, string[] includes = null, bool metadataOnly = false, bool indexEntriesOnly = false);
		
		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo);

		/// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
		IEnumerator<RavenJObject> StreamDocs(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null);

		/// <summary>
		/// Deletes the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		void DeleteIndex(string name);

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param> 
		BatchResult[] Batch(IEnumerable<ICommandData> commandDatas);

	    /// <summary>
	    /// Commits the specified tx id
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    void Commit(string txId);

	    /// <summary>
	    /// Rollbacks the specified tx id
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    void Rollback(string txId);

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		IDatabaseCommands With(ICredentials credentialsForSession);
	
		/// <summary>
		/// Perform a set based deletes using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
        /// <param name="options">Holds configuration options for base operation.</param>
        Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null);

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
        /// <param name="options">Holds configuration options for base operation.</param>
        Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, BulkOperationOptions options = null);

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
        /// <param name="options">Holds configuration options for base operation.</param>
        Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, BulkOperationOptions options = null);

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IDatabaseCommands ForDatabase(string database);

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the default database
		/// </summary>
		IDatabaseCommands ForSystemDatabase();

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery);

		/// <summary>
		/// Return a list of documents that based on the MoreLikeThisQuery.
		/// </summary>
		/// <param name="query">The more like this query parameters</param>
		/// <returns></returns>
		MultiLoadResult MoreLikeThis(MoreLikeThisQuery query);

		///<summary>
		/// Get the all terms stored in the index for the specified field
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize);

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		FacetResults GetFacets( string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null );

		/// <summary>
		/// Sends a multiple faceted queries in a single request and calculates the facet results for each of them
		/// </summary>
		/// <param name="facetedQueries">List of queries</param>
		FacetResults[] GetMultiFacets(FacetQuery[] facetedQueries);

        /// <summary>
        /// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
        /// </summary>
        /// <param name="index">Name of the index</param>
        /// <param name="query">Query to build facet results</param>
        /// <param name="facets">List of Facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start = 0, int? pageSize = null);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag and if the document is missing
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		RavenJObject Patch(string key, PatchRequest[] patches);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag and  if the document is missing
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		RavenJObject Patch(string key, ScriptedPatchRequest patch);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing);

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		RavenJObject Patch(string key, PatchRequest[] patches, Etag etag);

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata);

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag);

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata);

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		IDisposable DisableAllCaching();

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		GetResponse[] MultiGet(GetRequest[] requests);

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		DatabaseStatistics GetStatistics();

		/// <summary>
		/// Retrieves the document metadata for the specified document key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns>The document metadata for the specified document, or null if the document does not exist</returns>
		JsonDocumentMetadata Head(string key);

		/// <summary>
		/// Generate the next identity value from the server
		/// </summary>
		long NextIdentityFor(string name);

		/// <summary>
		/// Seeds the next identity value on the server
		/// </summary>
		long SeedIdentityFor(string name, long value);

		/// <summary>
		/// Get the full URL for the given document key
		/// </summary>
		string UrlFor(string documentKey);

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		IDisposable ForceReadFromMaster();

		/// <summary>
		/// Gets the transformers from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		TransformerDefinition[] GetTransformers(int start, int pageSize);

		/// <summary>
		/// Gets the transformer definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		TransformerDefinition GetTransformer(string name);

		/// <summary>
		/// Deletes the specified transformer
		/// </summary>
		/// <param name="name">The name.</param>
		void DeleteTransformer(string name);

	    /// <summary>
	    /// Prepares the transaction on the server.
	    /// </summary>
	    void PrepareTransaction(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null);

        [Obsolete("Use RavenFS instead.")]
        AttachmentInformation[] GetAttachments(int start, Etag startEtag, int pageSize);

        IndexMergeResults GetIndexMergeSuggestions();
	}

	public interface IGlobalAdminDatabaseCommands
	{
		/// <summary>
		/// Gets the build number
		/// </summary>
		BuildNumber GetBuildNumber();

		/// <summary>
		/// Returns the names of all tenant databases on the RavenDB server
		/// </summary>
		/// <returns>List of tenant database names</returns>
		string[] GetDatabaseNames(int pageSize, int start = 0);

		/// <summary>
		/// Get admin statistics
		/// </summary>
		AdminStatistics GetStatistics();

		/// <summary>
		/// Creates a database
		/// </summary>
		void CreateDatabase(DatabaseDocument databaseDocument);

		/// <summary>
		/// Deteles a database with the specified name
		/// </summary>
		void DeleteDatabase(string dbName, bool hardDelete = false);

		/// <summary>
		/// Sends an async command to compact a database. During the compaction the specified database will be offline.
		/// </summary>
		Operation CompactDatabase(string databaseName);

        /// <summary>
        /// Begins a restore operation
        /// </summary>
        Operation StartRestore(DatabaseRestoreRequest restoreRequest);

        /// <summary>
        /// Begins a backup operation
        /// </summary>
        void StartBackup(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName);

        IDatabaseCommands Commands { get; }
	}

	public interface IAdminDatabaseCommands
	{
		/// <summary>
		/// Disables all indexing
		/// </summary>
		void StopIndexing();

		/// <summary>
		/// Enables indexing
		/// </summary>
        void StartIndexing(int? maxNumberOfParallelIndexTasks = null);

		/// <summary>
		/// Get the indexing status
		/// </summary>
		string GetIndexingStatus();

		RavenJObject GetDatabaseConfiguration();
	}
}
