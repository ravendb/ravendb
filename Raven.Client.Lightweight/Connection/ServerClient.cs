using Raven.Abstractions.Connection;
using Raven.Client.Exceptions;
using Raven.Database.Data;
#if !NETFX_CORE
//-----------------------------------------------------------------------
// <copyright file="ServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Changes;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	using Raven.Client.Extensions;

	/// <summary>
	/// Access the RavenDB operations using HTTP
	/// </summary>
	public class ServerClient : IDatabaseCommands
	{
		private readonly AsyncServerClient asyncServerClient;

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { asyncServerClient.ReplicationInformer.FailoverStatusChanged += value; }
			remove { asyncServerClient.ReplicationInformer.FailoverStatusChanged -= value; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ServerClient"/> class.
		/// </summary>
		public ServerClient(AsyncServerClient asyncServerClient)
		{
			this.asyncServerClient = asyncServerClient;
		}

		public OperationCredentials PrimaryCredentials
		{
			get { return asyncServerClient.PrimaryCredentials; }
		}

		public DocumentConvention Convention
		{
			get { return asyncServerClient.convention; }
		}

		/// <summary>
		/// Allow access to the replication informer used to determine how we replicate requests
		/// </summary>
		public IDocumentStoreReplicationInformer ReplicationInformer
		{
			get { return asyncServerClient.ReplicationInformer; }
		}

		#region IDatabaseCommands Members

		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		public NameValueCollection OperationsHeaders
		{
			get { return asyncServerClient.OperationsHeaders; }
			set { asyncServerClient.OperationsHeaders = value; }
		}

		/// <summary>
		/// Gets the document for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument Get(string key)
		{
			return asyncServerClient.GetAsync(key).ResultUnwrap();
		}

		public IGlobalAdminDatabaseCommands GlobalAdmin
		{
			get { return new AdminServerClient(asyncServerClient, new AsyncAdminServerClient(asyncServerClient)); }
		}

		/// <summary>
		/// Gets documents for the specified key prefix
		/// </summary>
		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize,
		                                 RavenPagingInformation pagingInformation = null, bool metadataOnly = false,
		                                 string exclude = null, string transformer = null,
		                                 Dictionary<string, RavenJToken> queryInputs = null)
		{
			return
				asyncServerClient.StartsWithAsync(keyPrefix, matches, start, pageSize, pagingInformation, metadataOnly, exclude,
				                                  transformer, queryInputs)
				                 .ResultUnwrap();
		}

		/// <summary>
		/// Execute a GET request against the provided url
		/// and return the result as a json object
		/// </summary>
		/// <param name="requestUrl">The relative url to the server</param>
		/// <remarks>
		/// This method respects the replication semantics against the database.
		/// </remarks>
		public RavenJToken ExecuteGetRequest(string requestUrl)
		{
			return asyncServerClient.ExecuteGetRequest(requestUrl).ResultUnwrap();
		}

		public HttpJsonRequest CreateRequest(string requestUrl, string method, bool disableRequestCompression = false)
		{
			return asyncServerClient.CreateRequest(requestUrl, method, disableRequestCompression);
		}

		public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, string method,
			bool disableRequestCompression = false)
		{
			return asyncServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method,
				disableRequestCompression);
		}

		internal void ExecuteWithReplication(string method, Action<string> operation)
		{
			asyncServerClient.ExecuteWithReplication(method, operationMetadata =>
			{
				operation(operationMetadata.Url);
				return null;
			}).WaitUnwrap();
		}

		internal T ExecuteWithReplication<T>(string method, Func<string, T> operation)
		{
			return
				asyncServerClient.ExecuteWithReplication(method,
					operationMetadata => Task.FromResult(operation(operationMetadata.Url))).ResultUnwrap();
		}

		/// <summary>
		/// Perform a direct get for a document with the specified key on the specified server URL.
		/// </summary>
		/// <param name="operationMetadata">The metadata that contains URL and credentials to perform operation</param>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument DirectGet(OperationMetadata operationMetadata, string key, string transformer = null)
		{
			//TODO: add transformer
			return asyncServerClient.DirectGetAsync(operationMetadata, key).ResultUnwrap();
		}

		public JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false)
		{
			return asyncServerClient.GetDocumentsAsync(start, pageSize, metadataOnly).ResultUnwrap();
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			return asyncServerClient.PutAsync(key, etag, document, metadata).ResultUnwrap();
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Etag etag)
		{
			asyncServerClient.DeleteAsync(key, etag).WaitUnwrap();
		}

		/// <summary>
		/// Puts the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata)
		{
			asyncServerClient.PutAttachmentAsync(key, etag, data, metadata).WaitUnwrap();
		}

		/// <summary>
		/// Updates just the attachment with the specified key's metadata
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="metadata">The metadata.</param>
		public void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata)
		{
			asyncServerClient.UpdateAttachmentMetadataAsync(key, etag, metadata).WaitUnwrap();
		}

		/// <summary>
		/// Gets the attachments starting with the specified prefix
		/// </summary>
		public IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize)
		{
			return new AsycnEnumerableWrapper<Attachment>(asyncServerClient.GetAttachmentHeadersStartingWithAsync(idPrefix,
				start, pageSize).Result);

		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
			return asyncServerClient.GetAttachmentAsync(key).ResultUnwrap();
		}

		/// <summary>
		/// Retrieves the attachment metadata with the specified key, not the actual attachmet
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment HeadAttachment(string key)
		{
			return asyncServerClient.HeadAttachmentAsync(key).ResultUnwrap();
		}

		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void DeleteAttachment(string key, Etag etag)
		{
			asyncServerClient.DeleteAttachmentAsync(key, etag).WaitUnwrap();
		}

		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
			return asyncServerClient.GetDatabaseNamesAsync(pageSize, start).ResultUnwrap();
		}

		/// <summary>
		/// Gets the index names from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
		public string[] GetIndexNames(int start, int pageSize)
		{
			return asyncServerClient.GetIndexNamesAsync(start, pageSize).ResultUnwrap();
		}

		public IndexDefinition[] GetIndexes(int start, int pageSize)
		{
			return asyncServerClient.GetIndexesAsync(start, pageSize).ResultUnwrap();
		}

		public TransformerDefinition[] GetTransformers(int start, int pageSize)
		{
			return asyncServerClient.GetTransformersAsync(start, pageSize).ResultUnwrap();
		}

		public TransformerDefinition GetTransformer(string name)
		{
			return asyncServerClient.GetTransformerAsync(name).ResultUnwrap();
		}

		public void DeleteTransformer(string name)
		{
			asyncServerClient.DeleteTransformerAsync(name).WaitUnwrap();
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
			asyncServerClient.ResetIndexAsync(name).WaitUnwrap();
		}

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public IndexDefinition GetIndex(string name)
		{
			return asyncServerClient.GetIndexAsync(name).ResultUnwrap();
		}

		/// <summary>
		/// Puts the index.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The definition.</param>
		/// <returns></returns>
		public string PutIndex(string name, IndexDefinition definition)
		{
			return asyncServerClient.PutIndexAsync(name, definition, false).ResultUnwrap();
		}

		public string PutTransformer(string name, TransformerDefinition transformerDef)
		{
			return asyncServerClient.PutTransformerAsync(name, transformerDef).ResultUnwrap();
		}

		/// <summary>
		/// Puts the index.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The definition.</param>
		/// <param name="overwrite">if set to <c>true</c> overwrite the index.</param>
		/// <returns></returns>
		public string PutIndex(string name, IndexDefinition definition, bool overwrite)
		{
			return asyncServerClient.PutIndexAsync(name, definition, overwrite).ResultUnwrap();
		}

		/// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <returns></returns>
		public string PutIndex<TDocument, TReduceResult>(string name,
			IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
		{
			return asyncServerClient.PutIndexAsync(name, indexDef).ResultUnwrap();
		}

		/// <summary>
		/// Puts the index for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		/// <returns></returns>
		public string PutIndex<TDocument, TReduceResult>(string name,
			IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
		{
			return asyncServerClient.PutIndexAsync(name, indexDef, overwrite).ResultUnwrap();
		}

		public string DirectPutIndex(string name, OperationMetadata operationMetadata, bool overwrite,
			IndexDefinition definition)
		{
			return asyncServerClient.DirectPutIndexAsync(name, definition, overwrite, operationMetadata).Result;
		}

		/// <summary>
		/// Queries the specified index.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false,
			bool indexEntriesOnly = false)
		{
			try
			{
				return asyncServerClient.QueryAsync(index, query, includes, metadataOnly, indexEntriesOnly).ResultUnwrap();
			}
			catch (Exception e)
			{
				if (e is ConflictException)
					throw;

				throw new InvalidOperationException("Query failed. See inner exception for details.", e);
			}
		}

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query,
			out QueryHeaderInformation queryHeaderInfo)
		{
			var reference = new Reference<QueryHeaderInformation>();
			var streamQueryAsync = asyncServerClient.StreamQueryAsync(index, query, reference).ResultUnwrap();
			queryHeaderInfo = reference.Value;
			return new AsycnEnumerableWrapper<RavenJObject>(streamQueryAsync);
		}

		/// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag, string startsWith, string matches, int start, int pageSize,
			string exclude, RavenPagingInformation pagingInformation = null)
		{
			return new AsycnEnumerableWrapper<RavenJObject>(
				asyncServerClient.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, exclude, pagingInformation).Result);
		}

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
			asyncServerClient.DeleteIndexAsync(name).WaitUnwrap();
		}

		/// <summary>
		/// Gets the results for the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="includes">The includes.</param>
		/// <param name="transformer"></param>
		/// <param name="queryInputs"></param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <returns></returns>
		public MultiLoadResult Get(string[] ids, string[] includes, string transformer = null,
			Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false)
		{
			return asyncServerClient.GetAsync(ids, includes, transformer, queryInputs, metadataOnly).ResultUnwrap();
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			return asyncServerClient.BatchAsync(commandDatas.ToArray()).ResultUnwrap();
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Commit(string txId)
		{
			asyncServerClient.CommitAsync(txId).WaitUnwrap();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Rollback(string txId)
		{
			asyncServerClient.RollbackAsync(txId).WaitUnwrap();
		}

		/// <summary>
		/// Prepares the transaction on the server.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void PrepareTransaction(string txId)
		{
			asyncServerClient.PrepareTransactionAsync(txId).WaitUnwrap();
		}

		public BuildNumber GetBuildNumber()
		{
			return asyncServerClient.GetBuildNumberAsync().ResultUnwrap();
		}
        public IndexMergeResults GetIndexMergeSuggestions()
        {
            return asyncServerClient.GetIndexMergeSuggestionsAsync().ResultUnwrap();
        }

		public AttachmentInformation[] GetAttachments(Etag startEtag, int pageSize)
		{
			return asyncServerClient.GetAttachmentsAsync(startEtag, pageSize).ResultUnwrap();
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient((AsyncServerClient) asyncServerClient.With(credentialsForSession)); //TODO This cast is bad
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public IDisposable ForceReadFromMaster()
		{
			return asyncServerClient.ForceReadFromMaster();
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IDatabaseCommands ForDatabase(string database)
		{
			return new ServerClient((AsyncServerClient) asyncServerClient.ForDatabase(database)); //TODO This cast is bad
		}

		public IDatabaseCommands ForSystemDatabase()
		{
			return new ServerClient((AsyncServerClient) asyncServerClient.ForSystemDatabase()); //TODO This cast is bad
		}

		/// <summary>
		/// Gets the URL.
		/// </summary>
		/// <value>The URL.</value>
		public string Url
		{
			get { return asyncServerClient.Url; }
		}


		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale = false)
		{
			return asyncServerClient.DeleteByIndexAsync(indexName, queryToDelete, allowStale).ResultUnwrap();
		}

		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests,
			bool allowStale = false)
		{
			return asyncServerClient.UpdateByIndexAsync(indexName, queryToUpdate, patchRequests, allowStale).ResultUnwrap();
		}

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch,
			bool allowStale = false)
		{
			return asyncServerClient.UpdateByIndexAsync(indexName, queryToUpdate, patch, allowStale).ResultUnwrap();
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		/// <returns></returns>
		public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
		{
			return asyncServerClient.SuggestAsync(index, suggestionQuery).ResultUnwrap();
		}

		/// <summary>
		/// Return a list of documents that based on the MoreLikeThisQuery.
		/// </summary>
		/// <param name="query">The more like this query parameters</param>
		/// <returns></returns>
		public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
		{
			return asyncServerClient.MoreLikeThisAsync(query).ResultUnwrap();
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
			return asyncServerClient.GetStatisticsAsync().ResultUnwrap();
		}

		/// <summary>
		/// Generate the next identity value from the server
		/// </summary>
		public long NextIdentityFor(string name)
		{
			return asyncServerClient.NextIdentityForAsync(name).ResultUnwrap();
		}

		/// <summary>
		/// Get the full URL for the given document key
		/// </summary>
		public string UrlFor(string documentKey)
		{
			return asyncServerClient.UrlFor(documentKey);
		}

		/// <summary>
		/// Check if the document exists for the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocumentMetadata Head(string key)
		{
			return asyncServerClient.HeadAsync(key).ResultUnwrap();
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public GetResponse[] MultiGet(GetRequest[] requests)
		{
			return asyncServerClient.MultiGetAsync(requests).ResultUnwrap();
		}

		///<summary>
		/// Get the possible terms for the specified field in the index 
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
			return asyncServerClient.GetTermsAsync(index, field, fromValue, pageSize).ResultUnwrap();
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc, int start, int? pageSize)
		{
			return asyncServerClient.GetFacetsAsync(index, query, facetSetupDoc, start, pageSize).ResultUnwrap();
		}

		/// <summary>
		/// Sends a multiple faceted queries in a single request and calculates the facet results for each of them
		/// </summary>
		/// <param name="facetedQueries">List of queries</param>
		public FacetResults[] GetMultiFacets(FacetQuery[] facetedQueries)
		{
			return asyncServerClient.GetMultiFacetsAsync(facetedQueries).ResultUnwrap();
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facets">List of facets</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start, int? pageSize)
		{
			return asyncServerClient.GetFacetsAsync(index, query, facets, start, pageSize).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public RavenJObject Patch(string key, PatchRequest[] patches)
		{
			return asyncServerClient.PatchAsync(key, patches, null).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			return asyncServerClient.PatchAsync(key, patches, ignoreMissing).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch)
		{
			return asyncServerClient.PatchAsync(key, patch, null).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			return asyncServerClient.PatchAsync(key, patch, ignoreMissing).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)
		{
			return asyncServerClient.PatchAsync(key, patches, etag).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault,
			RavenJObject defaultMetadata)
		{
			return asyncServerClient.PatchAsync(key, patchesToExisting, patchesToDefault, defaultMetadata).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)
		{
			return asyncServerClient.PatchAsync(key, patch, etag).ResultUnwrap();
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault,
			RavenJObject defaultMetadata)
		{
			return asyncServerClient.PatchAsync(key, patchExisting, patchDefault, defaultMetadata).ResultUnwrap();
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return asyncServerClient.DisableAllCaching();
		}

		#endregion

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return asyncServerClient.ProfilingInformation; }
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			GC.SuppressFinalize(this);
			asyncServerClient.Dispose();
		}

		/// <summary>
		/// Allows an <see cref="T:System.Object"/> to attempt to free resources and perform other cleanup operations before the <see cref="T:System.Object"/> is reclaimed by garbage collection.
		/// </summary>
		~ServerClient()
		{
			Dispose();
		}

		/// <summary>
		/// Admin operations, like create/delete database.
		/// </summary>
		public IAdminDatabaseCommands Admin
		{
			get { return new AdminServerClient(asyncServerClient, new AsyncAdminServerClient(asyncServerClient)); }
		}

		private class AsycnEnumerableWrapper<T> : IEnumerator<T>, IEnumerable<T>
		{
			private readonly IAsyncEnumerator<T> asyncEnumerator;

			public AsycnEnumerableWrapper(IAsyncEnumerator<T> asyncEnumerator)
			{
				this.asyncEnumerator = asyncEnumerator;
			}

			public void Dispose()
			{
				asyncEnumerator.Dispose();
			}

			public bool MoveNext()
			{
				return asyncEnumerator.MoveNextAsync().ResultUnwrap();
			}

			public void Reset()
			{
				throw new NotSupportedException();
			}

			public T Current
			{
				get { return asyncEnumerator.Current; }
			}

			object IEnumerator.Current
			{
				get { return Current; }
			}

			public IEnumerator<T> GetEnumerator()
			{
				return this;
			}

			IEnumerator IEnumerable.GetEnumerator()
			{
				return GetEnumerator();
			}
		}

	}
}
#endif
