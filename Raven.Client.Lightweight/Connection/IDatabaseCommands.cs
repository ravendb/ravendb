#if !SILVERLIGHT
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
using Raven.Client.Connection.Profiling;
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
		/// Retrieves documents for the specified key prefix
		/// </summary>
		JsonDocument[] StartsWith(string keyPrefix, int start, int pageSize);

		/// <summary>
		/// Retrieves the document for the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		JsonDocument Get(string key);

		/// <summary>
		/// Retrieves documents with the specified ids, optionally specifying includes to fetch along
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		MultiLoadResult Get(string[] ids, string[] includes);

		/// <summary>
		/// Puts the document in the database with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		PutResult Put(string key, Guid? etag, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Deletes the document with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		void Delete(string key, Guid? etag);

		/// <summary>
		/// Puts a byte array as attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		void PutAttachment(string key, Guid? etag, Stream data, RavenJObject metadata);

		/// <summary>
		/// Retrieves the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		Attachment GetAttachment(string key);


		/// <summary>
		/// Retrieves the attachment metadata with the specified key, not the actual attachmet
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		Attachment HeadAttachment(string key);


		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		void DeleteAttachment(string key, Guid? etag);

		/// <summary>
		/// Returns the names of all tenant databases on the RavenDB server
		/// </summary>
		/// <returns>List of tenant database names</returns>
		string[] GetDatabaseNames(int pageSize);

		/// <summary>
		/// Returns the names of all indexes that exist on the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
		string[] GetIndexNames(int start, int pageSize);

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		void ResetIndex(string name);

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		IndexDefinition GetIndex(string name);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		string PutIndex(string name, IndexDefinition indexDef);

		/// <summary>
		/// Creates an index with the specified name, based on an index definition
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
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
		/// Queries the specified index in the Raven flavoured Lucene query syntax
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes.</param>
		QueryResult Query(string index, IndexQuery query, string[] includes);

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
		void Commit(Guid txId);

		/// <summary>
		/// Rollbacks the specified tx id
		/// </summary>
		/// <param name="txId">The tx id.</param>
		void Rollback(Guid txId);

		/// <summary>
		/// Promotes the transaction
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		byte[] PromoteTransaction(Guid fromTxId);

		/// <summary>
		/// Stores the recovery information
		/// </summary>
		/// <param name="resourceManagerId">The resource manager Id for this transaction</param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation);

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		IDatabaseCommands With(ICredentials credentialsForSession);

		/// <summary>
		/// Gets a value indicating whether [supports promotable transactions].
		/// </summary>
		/// <value>
		/// 	<c>true</c> if [supports promotable transactions]; otherwise, <c>false</c>.
		/// </value>
		bool SupportsPromotableTransactions { get; }

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		void DeleteByIndex(string indexName, IndexQuery queryToDelete);

		/// <summary>
		/// Perform a set based deletes using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale);

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests);

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, AdvancedPatchRequest patch);

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale);

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, AdvancedPatchRequest patch, bool allowStale);

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		IDatabaseCommands ForDatabase(string database);

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the default database
		/// </summary>
		IDatabaseCommands ForDefaultDatabase();

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery);

		///<summary>
		/// Get the all terms stored in the index for the specified field
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize);

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		IDictionary<string, IEnumerable<FacetValue>> GetFacets(string index, IndexQuery query, string facetSetupDoc);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		void Patch(string key, PatchRequest[] patches);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		void Patch(string key, AdvancedPatchRequest patch);

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		void Patch(string key, PatchRequest[] patches, Guid? etag);

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
        /// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		void Patch(string key, AdvancedPatchRequest patch, Guid? etag);

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
		/// Get the full URL for the given document key
		/// </summary>
		string UrlFor(string documentKey);

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		void ForceReadFromMaster();
	}
}
#endif
