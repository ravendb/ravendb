#if !SILVERLIGHT && !NETFX_CORE
//-----------------------------------------------------------------------
// <copyright file="ServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using Raven.Abstractions.Json;
using Raven.Client.Changes;
using Raven.Client.Listeners;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
    using System.Collections;
    using System.Threading.Tasks;
    using Raven.Abstractions.Util;
    using Raven.Client.Connection.Async;

    /// <summary>
	/// Access the RavenDB operations using HTTP
	/// </summary>
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;
		internal readonly DocumentConvention convention;
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
        private readonly ICredentials credentials;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? currentSessionId;
		private readonly IDocumentConflictListener[] conflictListeners;
		private int readStripingBase;

		private bool resolvingConflict;
		private bool resolvingConflictRetries;

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { replicationInformer.FailoverStatusChanged += value; }
			remove { replicationInformer.FailoverStatusChanged -= value; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ServerClient"/> class.
		/// </summary>
		public ServerClient(
            IAsyncDatabaseCommands asyncDatabaseCommands,
            string url,
            DocumentConvention convention,
            ICredentials credentials,
            Func<string, ReplicationInformer> replicationInformerGetter,
            string databaseName,
            HttpJsonRequestFactory jsonRequestFactory,
            Guid? currentSessionId,
            IDocumentConflictListener[] conflictListeners)
		{
		    this.asyncDatabaseCommands = asyncDatabaseCommands;
		    this.credentials = credentials;
			this.replicationInformerGetter = replicationInformerGetter;
			this.databaseName = databaseName;
			replicationInformer = replicationInformerGetter(databaseName);
			this.jsonRequestFactory = jsonRequestFactory;
			this.currentSessionId = currentSessionId;
			this.conflictListeners = conflictListeners;
			this.url = url;

			if (url.EndsWith("/"))
				this.url = url.Substring(0, url.Length - 1);

			this.convention = convention;
			replicationInformer.UpdateReplicationInformationIfNeeded(this);
			readStripingBase = replicationInformer.GetReadStripingBase();
		}

		/// <summary>
		/// Allow access to the replication informer used to determine how we replicate requests
		/// </summary>
		public ReplicationInformer ReplicationInformer
		{
			get { return replicationInformer; }
		}

		#region IDatabaseCommands Members

		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
        public NameValueCollection OperationsHeaders
		{
			get { return asyncDatabaseCommands.OperationsHeaders; }
            set { asyncDatabaseCommands.OperationsHeaders = value; }
		}

		/// <summary>
		/// Gets the document for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument Get(string key)
		{
		    return asyncDatabaseCommands.GetAsync(key).Result;
		}

	    public IGlobalAdminDatabaseCommands GlobalAdmin
	    {
	        get { return new AdminServerClient(this); }
	    }

	    /// <summary>
		/// Gets documents for the specified key prefix
		/// </summary>
		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize, bool metadataOnly = false, string exclude = null)
	    {
            return asyncDatabaseCommands.StartsWithAsync(keyPrefix, matches, start, pageSize, metadataOnly, exclude).Result;
		}

		public HttpJsonRequest CreateRequest(string requestUrl, string method, bool disableRequestCompression = false)
		{
		    return asyncDatabaseCommands.CreateRequest(requestUrl, method, disableRequestCompression);
		}

		public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, string method, bool disableRequestCompression = false)
		{
            return asyncDatabaseCommands.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method, disableRequestCompression);
		}

		internal void ExecuteWithReplication(string method, Action<string> operation)
		{
			ExecuteWithReplication<object>(method, operationUrl =>
			{
				operation(operationUrl);
				return null;
			});
		}

		internal T ExecuteWithReplication<T>(string method, Func<string, T> operation)
		{
			int currentRequest = convention.IncrementRequestCount();
			return replicationInformer.ExecuteWithReplication(method, url, currentRequest, readStripingBase, operation);
		}

		/// <summary>
		/// Perform a direct get for a document with the specified key on the specified server URL.
		/// </summary>
		/// <param name="serverUrl">The server URL.</param>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument DirectGet(string serverUrl, string key, string transformer = null)
		{
			if (key.Length > 127 || string.IsNullOrEmpty(transformer) == false)
			{
				// avoid hitting UrlSegmentMaxLength limits in Http.sys
				var multiLoadResult = DirectGet(new[] {key}, serverUrl, new string[0], transformer, new Dictionary<string, RavenJToken>(), false);
				var result = multiLoadResult.Results.FirstOrDefault();
				if (result == null)
					return null;
				return SerializationHelper.RavenJObjectToJsonDocument(result);
			}

			var metadata = new RavenJObject();
		    var actualUrl = serverUrl + "/docs/" + Uri.EscapeDataString(key);

			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(url, serverUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			try
			{
				var responseJson = request.ReadResponseJson();
				var docKey = request.ResponseHeaders[Constants.DocumentIdFieldName] ?? key;

				docKey = Uri.UnescapeDataString(docKey);
				request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
				return SerializationHelper.DeserializeJsonDocument(docKey, responseJson, request.ResponseHeaders, request.ResponseStatusCode);
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
				{
					var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
					var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));
					var etag = httpWebResponse.GetEtagHeader();

					var concurrencyException = TryResolveConflictOrCreateConcurrencyException(key, conflictsDoc, etag);
					if (concurrencyException == null)
					{
						if (resolvingConflictRetries)
							throw new InvalidOperationException("Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");

						resolvingConflictRetries = true;
						try
						{
							return DirectGet(serverUrl, key);
						}
						finally
						{
							resolvingConflictRetries = false;
						}
					}
					throw concurrencyException;
				}
				throw;
			}
		}

		private void HandleReplicationStatusChanges(string forceCheck, string primaryUrl, string currentUrl)
		{
			if (primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase)) 
				return;

			bool shouldForceCheck;
			if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
			{
				replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
			}
		}

		private ConflictException TryResolveConflictOrCreateConcurrencyException(string key, RavenJObject conflictsDoc, Etag etag)
		{
			var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
			if (ravenJArray == null)
				throw new InvalidOperationException("Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

			var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();



			if (conflictListeners.Length > 0 && resolvingConflict == false)
			{
				resolvingConflict = true;
				try
				{
					var multiLoadResult = Get(conflictIds, null);

					var results = multiLoadResult.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

					foreach (var conflictListener in conflictListeners)
					{
						JsonDocument resolvedDocument;
						if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
						{
							Put(key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata);

							return null;
						}
					}
				}
				finally
				{
					resolvingConflict = false;
				}
			}

			return new ConflictException("Conflict detected on " + key +
										", conflict must be resolved before the document will be accessible", true)
			{
				ConflictedVersionIds = conflictIds,
				Etag = etag
			};
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false)
		{
		    return asyncDatabaseCommands.GetDocumentsAsync(start, pageSize, metadataOnly).Result;
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
		    return asyncDatabaseCommands.PutAsync(key, etag, document, metadata).Result;
		}

		private void AddTransactionInformation(RavenJObject metadata)
		{
			if (convention.EnlistInDistributedTransactions == false)
				return;

			var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			if (transactionInformation == null)
				return;

			string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
			metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Etag etag)
		{
            asyncDatabaseCommands.DeleteAsync(key, etag).Wait();
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
		    asyncDatabaseCommands.PutAttachmentAsync(key, etag, data, metadata).Wait();
		}

		/// <summary>
		/// Updates just the attachment with the specified key's metadata
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="metadata">The metadata.</param>
		public void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata)
		{
		    asyncDatabaseCommands.UpdateAttachmentMetadataAsync(key, etag, metadata).Wait();
		}

		/// <summary>
		/// Gets the attachments starting with the specified prefix
		/// </summary>
		public IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize)
		{
		    return new AsycnEnumerableWrapper<Attachment>(asyncDatabaseCommands.GetAttachmentHeadersStartingWithAsync(idPrefix,
		            start, pageSize).Result);
            
		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
		    return asyncDatabaseCommands.GetAttachmentAsync(key).Result;
		}

		/// <summary>
		/// Retrieves the attachment metadata with the specified key, not the actual attachmet
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment HeadAttachment(string key)
		{
            return asyncDatabaseCommands.HeadAttachmentAsync(key).Result;
		}

		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void DeleteAttachment(string key, Etag etag)
		{
		    asyncDatabaseCommands.DeleteAttachmentAsync(key, etag).Wait();
		}

		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
		    return asyncDatabaseCommands.GetDatabaseNamesAsync(pageSize, start).Result;
		}

		/// <summary>
		/// Gets the index names from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
		public string[] GetIndexNames(int start, int pageSize)
		{
		    return asyncDatabaseCommands.GetIndexNamesAsync(start, pageSize).Result;
		}

		public IndexDefinition[] GetIndexes(int start, int pageSize)
		{
		    return asyncDatabaseCommands.GetIndexesAsync(start, pageSize).Result;
		}

		public TransformerDefinition[] GetTransformers(int start, int pageSize)
		{
		    return asyncDatabaseCommands.GetTransformersAsync(start, pageSize).Result;
		}

		public TransformerDefinition GetTransformer(string name)
		{
		    return asyncDatabaseCommands.GetTransformerAsync(name).Result;
		}

		public void DeleteTransformer(string name)
		{
		    asyncDatabaseCommands.DeleteTransformerAsync(name);
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
		    asyncDatabaseCommands.ResetIndexAsync(name).Wait();
		}

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public IndexDefinition GetIndex(string name)
		{
		    return asyncDatabaseCommands.GetIndexAsync(name).Result;
		}

		/// <summary>
		/// Puts the index.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The definition.</param>
		/// <returns></returns>
		public string PutIndex(string name, IndexDefinition definition)
		{
		    return asyncDatabaseCommands.PutIndexAsync(name, definition, false).Result;
		}

		public string PutTransformer(string name, TransformerDefinition transformerDef)
		{
            return asyncDatabaseCommands.PutTransformerAsync(name, transformerDef).Result;
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
            return asyncDatabaseCommands.PutIndexAsync(name, definition, overwrite).Result;
		}

	    /// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <returns></returns>
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention));
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
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention), overwrite);
		}

		/// <summary>
		/// Queries the specified index.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)
		{
            return asyncDatabaseCommands.QueryAsync(index, query, includes, metadataOnly, indexEntriesOnly).Result;
		}

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo)
		{
		    var reference = new Reference<QueryHeaderInformation>();
		    Task<IAsyncEnumerator<RavenJObject>> streamQueryAsync = asyncDatabaseCommands.StreamQueryAsync(index, query, reference);
		    queryHeaderInfo = reference.Value;
		    return new AsycnEnumerableWrapper<RavenJObject>(streamQueryAsync.Result);
		}

		/// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag, string startsWith, string matches, int start, int pageSize, string exclude)
		{
		    return new AsycnEnumerableWrapper<RavenJObject>(
		            asyncDatabaseCommands.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, exclude).Result);
		}

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
		    asyncDatabaseCommands.DeleteIndexAsync(name).Wait();
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
	    public MultiLoadResult Get(string[] ids, string[] includes, string transformer = null, Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false)
	    {
	        return asyncDatabaseCommands.GetAsync(ids, includes, transformer, queryInputs, metadataOnly).Result;
		}

	    /// <summary>
	    /// Perform a direct get for loading multiple ids in one request
	    /// </summary>
	    /// <param name="ids">The ids.</param>
	    /// <param name="operationUrl">The operation URL.</param>
	    /// <param name="includes">The includes.</param>
	    /// <param name="transformer"></param>
	    /// <param name="metadataOnly"></param>
	    /// <returns></returns>
	    private MultiLoadResult DirectGet(string[] ids, string operationUrl, string[] includes, string transformer, Dictionary<string, RavenJToken> queryInputs, bool metadataOnly)
		{
			var path = operationUrl + "/queries/?";
			if (metadataOnly)
				path += "&metadata-only=true";
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
	        if (!string.IsNullOrEmpty(transformer))
	            path += "&transformer=" + transformer;


			if (queryInputs != null)
			{
				path = queryInputs.Aggregate(path, (current, queryInput) => current + ("&" + string.Format("qp-{0}={1}", queryInput.Key, queryInput.Value)));
			}
		    var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
		    var uniqueIds = new HashSet<string>(ids);
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load that many items are probably going to be rare
			HttpJsonRequest request;
			if (uniqueIds.Sum(x => x.Length) < 1024)
			{
				path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
				request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "GET", metadata, credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
                            .AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			}
			else
			{
				request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "POST", metadata, credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
                            .AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				request.Write(new RavenJArray(uniqueIds).ToString(Formatting.None));
			}


			var result = (RavenJObject)request.ReadResponseJson();

			var results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList();
	        var multiLoadResult = new MultiLoadResult
	        {
	            Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList()
	        };

            if(string.IsNullOrEmpty(transformer)) {
                multiLoadResult.Results = ids.Select(id => results.FirstOrDefault(r => string.Equals(r["@metadata"].Value<string>("@id"), id, StringComparison.OrdinalIgnoreCase))).ToList();
			} 
            else
            {
                multiLoadResult.Results = results;
            }


			var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

			return RetryOperationBecauseOfConflict(docResults, multiLoadResult, () => DirectGet(ids, operationUrl, includes, transformer, queryInputs, metadataOnly));
		}

		private T RetryOperationBecauseOfConflict<T>(IEnumerable<RavenJObject> docResults, T currentResult, Func<T> nextTry)
		{
			bool requiresRetry = docResults.Aggregate(false, (current, docResult) => current | AssertNonConflictedDocumentAndCheckIfNeedToReload(docResult));
			if (!requiresRetry)
				return currentResult;

			if (resolvingConflictRetries)
				throw new InvalidOperationException(
					"Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
			resolvingConflictRetries = true;
			try
			{
				return nextTry();
			}
			finally
			{
				resolvingConflictRetries = false;
			}
		}

		private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult)
		{
			if (docResult == null)
				return false;
			var metadata = docResult[Constants.Metadata];
			if (metadata == null)
				return false;

			if (metadata.Value<int>("@Http-Status-Code") == 409)
			{
				var concurrencyException = TryResolveConflictOrCreateConcurrencyException(metadata.Value<string>("@id"), docResult, HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag")));
				if (concurrencyException == null)
					return true;
				throw concurrencyException;
			}
			return false;
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
		    return asyncDatabaseCommands.BatchAsync(commandDatas.ToArray()).Result;
		}

	    /// <summary>
	    /// Commits the specified tx id.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    public void Commit(string txId)
	    {
	        asyncDatabaseCommands.CommitAsync(txId).Wait();
	    }

	    /// <summary>
	    /// Rollbacks the specified tx id.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    public void Rollback(string txId)
		{
            asyncDatabaseCommands.RollbackAsync(txId).Wait();
		}

		/// <summary>
		/// Prepares the transaction on the server.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void PrepareTransaction(string txId)
		{
            asyncDatabaseCommands.PrepareTransactionAsync(txId).Wait();
	    }

        public BuildNumber GetBuildNumber()
        {
            return asyncDatabaseCommands.GetBuildNumberAsync().Result;
        }

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(
                new AsyncServerClient(url, convention, credentialsForSession,  jsonRequestFactory, currentSessionId, replicationInformerGetter, databaseName, conflictListeners),
                url, convention, credentialsForSession, replicationInformerGetter, databaseName, jsonRequestFactory, currentSessionId, conflictListeners);
		}

		/// <summary>
		/// Get the low level  bulk insert operation
		/// </summary>
		public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
		{
			return new RemoteBulkInsertOperation(options, this, changes);
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public IDisposable ForceReadFromMaster()
		{
            return asyncDatabaseCommands.ForceReadFromMaster();
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IDatabaseCommands ForDatabase(string database)
		{
			if (database == Constants.SystemDatabase)
				return ForSystemDatabase();

			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			databaseUrl = databaseUrl + "/databases/" + database;
            if (databaseUrl == url)
				return this;
			return new ServerClient(
                new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, currentSessionId, replicationInformerGetter, databaseName, conflictListeners),
                databaseUrl, convention, credentials, replicationInformerGetter, database, jsonRequestFactory, currentSessionId, conflictListeners)
				   {
					   OperationsHeaders = OperationsHeaders
				   };
		}

		public IDatabaseCommands ForSystemDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
            if (databaseUrl == url)
				return this;
			return new ServerClient(
                new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, currentSessionId, replicationInformerGetter, databaseName, conflictListeners),
                databaseUrl, convention, credentials, replicationInformerGetter, null, jsonRequestFactory, currentSessionId, conflictListeners)
			{
				OperationsHeaders = OperationsHeaders
			};
		}

		/// <summary>
		/// Gets the URL.
		/// </summary>
		/// <value>The URL.</value>
		public string Url
		{
			get { return url; }
		}

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale = false)
		{
		    return asyncDatabaseCommands.DeleteByIndexAsync(indexName, queryToDelete, allowStale).Result;
        }

	    /// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale = false)
		{
            return asyncDatabaseCommands.UpdateByIndexAsync(indexName, queryToUpdate, patchRequests, allowStale).Result;
		}

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale = false)
		{
		    return asyncDatabaseCommands.UpdateByIndexAsync(indexName, queryToUpdate, patch, allowStale).Result;
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		/// <returns></returns>
		public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
		{
		    return asyncDatabaseCommands.SuggestAsync(index, suggestionQuery).Result;
		}

		/// <summary>
		/// Return a list of documents that based on the MoreLikeThisQuery.
		/// </summary>
		/// <param name="query">The more like this query parameters</param>
		/// <returns></returns>
		public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
		{
		    return asyncDatabaseCommands.MoreLikeThisAsync(query).Result;
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
		    return asyncDatabaseCommands.GetStatisticsAsync().Result;
		}

		/// <summary>
		/// Generate the next identity value from the server
		/// </summary>
		public long NextIdentityFor(string name)
		{
			return ExecuteWithReplication("POST", url =>
			{
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, url + "/identity/next?name=" + Uri.EscapeDataString(name), "POST", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				var readResponseJson = request.ReadResponseJson();

				return readResponseJson.Value<long>("Value");
			});
		}

		/// <summary>
		/// Get the full URL for the given document key
		/// </summary>
		public string UrlFor(string documentKey)
		{
		    return asyncDatabaseCommands.UrlFor(documentKey);
		}

		/// <summary>
		/// Check if the document exists for the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocumentMetadata Head(string key)
		{
		    return asyncDatabaseCommands.HeadAsync(key).Result;
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public GetResponse[] MultiGet(GetRequest[] requests)
		{
		    return asyncDatabaseCommands.MultiGetAsync(requests).Result;
		}

		///<summary>
		/// Get the possible terms for the specified field in the index 
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
		    return asyncDatabaseCommands.GetTermsAsync(index, field, fromValue, pageSize).Result;
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
		    return asyncDatabaseCommands.GetFacetsAsync(index, query, facetSetupDoc, start, pageSize).Result;
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
            return asyncDatabaseCommands.GetFacetsAsync(index, query, facets, start, pageSize).Result;
        }

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public RavenJObject Patch(string key, PatchRequest[] patches)
		{
		    return asyncDatabaseCommands.PatchAsync(key, patches, null).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)
		{
		    return asyncDatabaseCommands.PatchAsync(key, patches, ignoreMissing).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch)
		{
            return asyncDatabaseCommands.PatchAsync(key, patch, null).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
            return asyncDatabaseCommands.PatchAsync(key, patch, ignoreMissing).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)
		{
            return asyncDatabaseCommands.PatchAsync(key, patches, etag).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata)
		{
            return asyncDatabaseCommands.PatchAsync(key, patchesToExisting, patchesToDefault, defaultMetadata).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)
		{
            return asyncDatabaseCommands.PatchAsync(key, patch, etag).Result;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata)
		{
            return asyncDatabaseCommands.PatchAsync(key, patchExisting, patchDefault, defaultMetadata).Result;
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
		    return asyncDatabaseCommands.DisableAllCaching();
		}

		#endregion

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return asyncDatabaseCommands.ProfilingInformation; }
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			GC.SuppressFinalize(this);
            asyncDatabaseCommands.Dispose();
		}

		/// <summary>
		/// Allows an <see cref="T:System.Object"/> to attempt to free resources and perform other cleanup operations before the <see cref="T:System.Object"/> is reclaimed by garbage collection.
		/// </summary>
		~ServerClient()
		{
			Dispose();
		}

		public RavenJToken GetOperationStatus(long id)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, url + "/operation/status?id=" + id, "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			try
			{
				return request.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
				return null;
			}
		}

        //TODO Owin host handles 100s, is this needed?
		public IDisposable Expect100Continue()
		{
			var servicePoint = ServicePointManager.FindServicePoint(new Uri(url));
			servicePoint.Expect100Continue = true;
			return new DisposableAction(() => servicePoint.Expect100Continue = false);
		}

	    /// <summary>
	    /// Admin operations, like create/delete database.
	    /// </summary>
	    public IAdminDatabaseCommands Admin
	    {
	        get { return new AdminServerClient(this); }
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
                return asyncEnumerator.MoveNextAsync().Result;
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
