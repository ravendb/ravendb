//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		private ProfilingInformation profilingInformation;
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly DocumentConvention convention;
		private IDictionary<string, string> operationsHeaders = new Dictionary<string, string>();
		internal readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? sessionId;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private int requestCount;
		private int readStripingBase;

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials,
								 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId,
								 Func<string, ReplicationInformer> replicationInformerGetter, string databaseName)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.url = url;
			if (this.url.EndsWith("/"))
				this.url = this.url.Substring(0, this.url.Length - 1);
			this.jsonRequestFactory = jsonRequestFactory;
			this.sessionId = sessionId;
			this.convention = convention;
			this.credentials = credentials;
			this.databaseName = databaseName;
			this.replicationInformerGetter = replicationInformerGetter;
			this.replicationInformer = replicationInformerGetter(databaseName);
			this.readStripingBase = replicationInformer.GetReadStripingBase();
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
		}


		/// <summary>
		/// Returns a new <see cref="IAsyncDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new AsyncServerClient(url, convention, credentialsForSession, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName);
		}

		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the indexes from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Resets the specified index asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task ResetIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			string requestUri = url + "/indexes/" + name;
			var webRequest = (HttpWebRequest)WebRequest.Create(requestUri);
			AddOperationHeaders(webRequest);
			webRequest.Method = "HEAD";
			webRequest.Credentials = credentials;

			return Task<WebResponse>.Factory.FromAsync(webRequest.BeginGetResponse, webRequest.EndGetResponse, null)
				.ContinueWith(task =>
				{
					try
					{
						task.Result.Close();
						if (overwrite == false)
							throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");

					}
					catch (AggregateException e)
					{
						var we = e.ExtractSingleInnerException() as WebException;
						if (we == null)
							throw;
						var response = we.Response as HttpWebResponse;
						if (response == null || response.StatusCode != HttpStatusCode.NotFound)
							throw;
					}

					var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
					request.AddOperationHeaders(OperationsHeaders);
					var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);
					return Task.Factory.FromAsync(request.BeginWrite, request.EndWrite,serializeObject, null)
						.ContinueWith(writeTask =>  request.ReadResponseJsonAsync()
													.ContinueWith(readJsonTask =>
													{
														return readJsonTask.Result.Value<string>("index");
													})).Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		public Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			string path = queryToDelete.GetIndexQueryUrl(url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "DELETE", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteRequestAsync()
				.ContinueWith(task =>
				{
					var aggregateException = task.Exception;
					if (aggregateException == null)
						return task;
					var e = aggregateException.ExtractSingleInnerException() as WebException;
					if (e == null)
						return task;
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName, e);
					return task;
				}).Unwrap();
		}

		/// <summary>
		/// Deletes the document for the specified id asynchronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		public Task<PutResult> PutAsync(string key, Guid? etag, RavenJObject document, RavenJObject metadata)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			if (etag != null)
				metadata["ETag"] = new RavenJValue(etag.Value.ToString());
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/docs/" + key, method, metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			
			return Task.Factory.FromAsync(request.BeginWrite,request.EndWrite,document.ToString(), null)
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						throw new InvalidOperationException("Unable to write to server");

					return request.ReadResponseJsonAsync()
						.ContinueWith(task1 =>
						{
							try
							{
								return convention.CreateSerializer().Deserialize<PutResult>(new RavenJTokenReader(task1.Result));
							}
							catch (AggregateException e)
							{
								var we = e.ExtractSingleInnerException() as WebException;
								if (we == null)
									throw;
								var httpWebResponse = we.Response as HttpWebResponse;
								if (httpWebResponse == null ||
									httpWebResponse.StatusCode != HttpStatusCode.Conflict)
									throw;
								throw ThrowConcurrencyException(we);
							}
						});
				})
				.Unwrap();
		}

		private void AddOperationHeaders(HttpWebRequest webRequest)
		{
			foreach (var header in OperationsHeaders)
			{
				webRequest.Headers[header.Key] = header.Value;
			}
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IAsyncDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			databaseUrl = databaseUrl + "/databases/" + database + "/";
			if (databaseUrl == url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId)
			{
				operationsHeaders = operationsHeaders
			};
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands ForDefaultDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			if (databaseUrl == url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId)
			{
				operationsHeaders = operationsHeaders
			};
		}

		
		

		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		public IDictionary<string, string> OperationsHeaders
		{
			get { return operationsHeaders; }
		}

		/// <summary>
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<JsonDocument> GetAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/docs/" + key, "GET", metadata, credentials, convention);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					try
					{
						var requestJson = task.Result;
						return SerializationHelper.DeserializeJsonDocument(key, requestJson, request.ResponseHeaders, request.ResponseStatusCode);
					}
					catch (AggregateException e)
					{
						var we = e.ExtractSingleInnerException() as WebException;
						if(we == null)
							throw;
						var httpWebResponse = we.Response as HttpWebResponse;
						if (httpWebResponse == null)
							throw;
						if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
							return null;
						if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
						{
							var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
							var conflictsDoc = RavenJObject.Load(new JsonTextReader(conflicts));
							var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

							throw new ConflictException("Conflict detected on " + key +
														", conflict must be resolved before the document will be accessible")
							{
								ConflictedVersionIds = conflictIds
							};
						}
						throw;
					}
				});
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes)
		{
			var path = url + "/queries/?";
			if (includes != null && includes.Length > 0)
			{
				path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			HttpJsonRequest request;
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load > 128 items are going to be rare
			if (keys.Length < 128)
			{
				path += "&" + string.Join("&", keys.Select(x => "id=" + x).ToArray());
				request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);
				return request.ReadResponseJsonAsync()
					.ContinueWith(task => CompleteMultiGetAsync(task));
			}
			request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "POST", credentials, convention);
			return Task.Factory.FromAsync(request.BeginWrite, request.EndWrite, new RavenJArray(keys).ToString(Formatting.None), null)
				.ContinueWith(writeTask => request.ReadResponseJsonAsync())
				.Unwrap()
				.ContinueWith(task => CompleteMultiGetAsync(task));
		}

		private static MultiLoadResult CompleteMultiGetAsync(Task<RavenJToken> task)
		{
			try
			{
				var result = task.Result;

				return new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				};
			}
			catch (AggregateException e)
			{
				var we = e.ExtractSingleInnerException() as WebException;
				if (we == null)
					throw;
				var httpWebResponse = we.Response as HttpWebResponse;
				if (httpWebResponse == null ||
				    httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(we);
			}
		}

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize)
		{
			var requestUri = url + "/docs/?start=" + start + "&pageSize=" + pageSize;
			return jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention)
						.ReadResponseJsonAsync()
						.ContinueWith(task => ((RavenJArray)task.Result)
												.Cast<RavenJObject>()
												.ToJsonDocuments()
												.ToArray());
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		public Task<IDictionary<string, IEnumerable<FacetValue>>> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc)
		{
			var requestUri = url + string.Format("/facets/{0}?facetDoc={1}&query={2}",
			Uri.EscapeUriString(index),
			Uri.EscapeDataString(facetSetupDoc),
			Uri.EscapeDataString(query.Query));

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJObject) task.Result;
					return json.JsonDeserialization<IDictionary<string, IEnumerable<FacetValue>>>();
				});
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			throw new NotImplementedException();
		}

		public Task<LicensingStatus> GetLicenseStatus()
		{
			throw new NotImplementedException();
		}

		public Task<BuildNumber> GetBuildNumber()
		{
			throw new NotImplementedException();
		}

		public Task StartBackupAsync(string backupLocation)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Perform a single POST requst containing multiple nested GET requests
		/// </summary>
		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{

			var multiGetOperation = new MultiGetOperation(this,  convention, url, requests);

			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, multiGetOperation.RequestUri, "POST",
			                                                               credentials, convention);

			var requestsForServer = multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

			var postedData = JsonConvert.SerializeObject(requestsForServer);

			if (multiGetOperation.CanFullyCache(jsonRequestFactory, httpJsonRequest, postedData))
			{
				var cachedResponses = multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length], jsonRequestFactory)	;
				return Task.Factory.StartNew(() => cachedResponses);
			}


			return Task.Factory.FromAsync(httpJsonRequest.BeginWrite, httpJsonRequest.EndWrite, postedData, null)
				.ContinueWith(
					task =>
					{
						task.Wait();// will throw on error
						return httpJsonRequest.ReadResponseJsonAsync()
							.ContinueWith(replyTask =>
							{
								var responses = convention.CreateSerializer().Deserialize<GetResponse[]>(new RavenJTokenReader(replyTask.Result));
								return multiGetOperation.HandleCachingResponse(responses, jsonRequestFactory);
							})
						;
					})
					.Unwrap();

		}

		/// <summary>
		/// Begins an async get operation for documents whose id starts with the specified prefix
		/// </summary>
		/// <param name="prefix">Prefix that the ids begin with.</param>
		/// <param name="start">Paging start.</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public Task<JsonDocument[]> GetDocumentsStartingWithAsync(string prefix, int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(url, index, "indexes");
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					RavenJObject json;
					try
					{
						json = (RavenJObject) task.Result;
					}
					catch (AggregateException e)
					{
						var we = e.ExtractSingleInnerException() as WebException;
						if(we != null)
						{
							var httpWebResponse = we.Response as HttpWebResponse;
							if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
							{
								var text = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression()).ReadToEnd();
								if (text.Contains("maxQueryString"))
									throw new InvalidOperationException(text, e);
								throw new InvalidOperationException("There is no index named: " + index);
							}
						}
						throw;
					}

					return new QueryResult
					{
						IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
						IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
						IndexEtag = new Guid(request.ResponseHeaders["ETag"]),
						Results = ((RavenJArray)json["Results"]).Cast<RavenJObject>().ToList(),
						TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
						IndexName = json.Value<string>("IndexName"),
						SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString())
					};
				});

		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery)
		{
			if (suggestionQuery == null) throw new ArgumentNullException("suggestionQuery");

			var requestUri = url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}",
				Uri.EscapeUriString(index),
				Uri.EscapeDataString(suggestionQuery.Term),
				Uri.EscapeDataString(suggestionQuery.Field),
				Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToString()),
				Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
				Uri.EscapeDataString(suggestionQuery.Accuracy.ToString()));

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJObject) task.Result;
					return new SuggestionQueryResult
					{
						Suggestions = ((RavenJArray) json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
					};
				});
		}

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var req = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials, convention);
			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
			var data = jArray.ToString(Formatting.None);

			return Task.Factory.FromAsync(req.BeginWrite, req.EndWrite, data, null)
				.ContinueWith(writeTask => req.ReadResponseJsonAsync())
				.Unwrap()
				.ContinueWith(task =>
				{
					RavenJArray response;
					try
					{
						response = (RavenJArray)task.Result;
					}
					catch (AggregateException e)
					{
						var we = e.ExtractSingleInnerException() as WebException;
						if (we == null)
							throw;
						var httpWebResponse = we.Response as HttpWebResponse;
						if (httpWebResponse == null ||
							httpWebResponse.StatusCode != HttpStatusCode.Conflict)
							throw;
						throw ThrowConcurrencyException(we);
					}
					return convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
				});

		}

		private static Exception ThrowConcurrencyException(WebException e)
		{
			using (var sr = new StreamReader(e.Response.GetResponseStreamWithHttpDecompression()))
			{
				var text = sr.ReadToEnd();
				var errorResults = JsonConvert.DeserializeAnonymousType(text, new
				{
					url = (string)null,
					actualETag = Guid.Empty,
					expectedETag = Guid.Empty,
					error = (string)null
				});
				return new ConcurrencyException(errorResults.error)
				{
					ActualETag = errorResults.actualETag,
					ExpectedETag = errorResults.expectedETag
				};
			}
		}

		private static void AddTransactionInformation(RavenJObject metadata)
		{
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		/// <summary>
		/// Begins retrieving the statistics for the database
		/// </summary>
		/// <returns></returns>
		public Task<DatabaseStatistics> GetStatisticsAsync()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the list of databases from the server asynchronously
		/// </summary>
		public Task<string[]> GetDatabaseNamesAsync(int pageSize)
		{
			return url.Databases(pageSize)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJArray)task.Result;
					return json
						.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
						.ToArray();
				});
		}
		
		/// <summary>
		/// Puts the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public Task PutAttachmentAsync(string key, Guid? etag, byte[] data, RavenJObject metadata)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the attachment by the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<Attachment> GetAttachmentAsync(string key)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Deletes the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Guid? etag)
		{
			throw new NotImplementedException();
		}



		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return jsonRequestFactory.DisableAllCaching();
		}

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		public Task EnsureSilverlightStartUpAsync()
		{
			throw new NotImplementedException();
		}

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { replicationInformer.FailoverStatusChanged += value; }
			remove { replicationInformer.FailoverStatusChanged -= value; }
		}

		private Task<T> ExecuteWithReplication<T>(string method, Func<string, Task<T>> operation)
		{
			return ExecuteWithReplication(new ExecuteWithReplicationState<T>(method, operation));
		}

		private Task<T> ExecuteWithReplication<T>(ExecuteWithReplicationState<T> state)
		{
			switch (state.State)
			{
				case ExecuteWithReplicationStates.Start:
					state.CurrentRequest = Interlocked.Increment(ref requestCount);
					state.ReplicationDestinations = replicationInformer.ReplicationDestinations;

					var shouldReadFromAllServers = ((convention.FailoverBehavior & FailoverBehavior.ReadFromAllServers) ==
													FailoverBehavior.ReadFromAllServers);
					if (shouldReadFromAllServers && state.Method == "GET")
					{
						var replicationIndex = readStripingBase % (state.ReplicationDestinations.Count + 1);
						// if replicationIndex == destinations count, then we want to use the master
						// if replicationIndex < 0, then we were explicitly instructed to use the master
						if (replicationIndex < state.ReplicationDestinations.Count && replicationIndex >= 0)
						{
							// if it is failing, ignore that, and move to the master or any of the replicas
							if (replicationInformer.ShouldExecuteUsing(state.ReplicationDestinations[replicationIndex], state.CurrentRequest, state.Method, false))
							{
								return AttemptOperationAndOnFailureCallExecuteWithReplication(state.ReplicationDestinations[replicationIndex],
																							  state.With(ExecuteWithReplicationStates.AfterTryingWithStripedServer));
							}
						}
					}

					goto case ExecuteWithReplicationStates.AfterTryingWithStripedServer;
				case ExecuteWithReplicationStates.AfterTryingWithStripedServer:

					if (!replicationInformer.ShouldExecuteUsing(url, state.CurrentRequest, state.Method, true))
						goto case ExecuteWithReplicationStates.TryAllServers; // skips both checks

					return AttemptOperationAndOnFailureCallExecuteWithReplication(url,
																					state.With(ExecuteWithReplicationStates.AfterTryingWithDefaultUrl));

				case ExecuteWithReplicationStates.AfterTryingWithDefaultUrl:
					if (replicationInformer.IsFirstFailure(url))
						return AttemptOperationAndOnFailureCallExecuteWithReplication(url,
																					  state.With(ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice));
					else
						goto case ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice;

				case ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice:

					replicationInformer.IncrementFailureCount(url);

					goto case ExecuteWithReplicationStates.TryAllServers;
				case ExecuteWithReplicationStates.TryAllServers:

					// The following part (cases ExecuteWithReplicationStates.TryAllServers, and ExecuteWithReplicationStates.TryAllServersSecondAttempt)
					// is a for loop, rolled out using goto and nested calls of the method in continuations
					state.LastAttempt++;
					if (state.LastAttempt >= state.ReplicationDestinations.Count)
						goto case ExecuteWithReplicationStates.AfterTryingAllServers;

					var destination = state.ReplicationDestinations[state.LastAttempt];
					if (!replicationInformer.ShouldExecuteUsing(destination, state.CurrentRequest, state.Method, false))
					{
						// continue the next iteration of the loop
						goto case ExecuteWithReplicationStates.TryAllServers;
					}

					return AttemptOperationAndOnFailureCallExecuteWithReplication(destination,
																				  state.With(ExecuteWithReplicationStates.TryAllServersSecondAttempt));
				case ExecuteWithReplicationStates.TryAllServersSecondAttempt:
					destination = state.ReplicationDestinations[state.LastAttempt];
					if (replicationInformer.IsFirstFailure(destination))
						return AttemptOperationAndOnFailureCallExecuteWithReplication(destination,
																					  state.With(ExecuteWithReplicationStates.TryAllServersFailedTwice));
					else
						goto case ExecuteWithReplicationStates.TryAllServersFailedTwice;

				case ExecuteWithReplicationStates.TryAllServersFailedTwice:
					replicationInformer.IncrementFailureCount(state.ReplicationDestinations[state.LastAttempt]);

					// continue the next iteration of the loop
					goto case ExecuteWithReplicationStates.TryAllServers;

				case ExecuteWithReplicationStates.AfterTryingAllServers:
					throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + state.ReplicationDestinations.Count) + " Raven instances.");

				default:
					throw new InvalidOperationException("Invalid ExecuteWithReplicationState " + state);
			}
		}

		private Task<T> AttemptOperationAndOnFailureCallExecuteWithReplication<T>(string url, ExecuteWithReplicationState<T> state)
		{
			Task<Task<T>> finalTask = state.Operation(url).ContinueWith(task =>
				{
					switch (task.Status)
					{
						case TaskStatus.RanToCompletion:
							var tcs = new TaskCompletionSource<T>();
							tcs.SetResult(task.Result);
							return tcs.Task;

						case TaskStatus.Canceled:
							tcs = new TaskCompletionSource<T>();
							tcs.SetCanceled();
							return tcs.Task;

						case TaskStatus.Faulted:
							if (ServerClient.IsServerDown(task.Exception))
								return ExecuteWithReplication(state);
							else
								throw task.Exception;

						default:
							throw new InvalidOperationException("Unknown task status in AttemptOperationAndOnFailureCallExecuteWithReplication");
					}
				});
			return finalTask.Unwrap();
		}

		private class ExecuteWithReplicationState<T>
		{
			public ExecuteWithReplicationState(string method, Func<string, Task<T>> operation)
			{
				this.Method = method;
				this.Operation = operation;

				this.State = ExecuteWithReplicationStates.Start;
			}

			public readonly string Method;
			public readonly Func<string, Task<T>> Operation;

			public ExecuteWithReplicationStates State = ExecuteWithReplicationStates.Start;
			public int LastAttempt = -1;
			public List<string> ReplicationDestinations;
			public int CurrentRequest;

			public ExecuteWithReplicationState<T> With(ExecuteWithReplicationStates state)
			{
				this.State = state;
				return this;
			}
		}

		private enum ExecuteWithReplicationStates
		{
			Start,
			AfterTryingWithStripedServer,
			AfterTryingWithDefaultUrl,
			TryAllServers,
			AfterTryingAllServers,
			TryAllServersSecondAttempt,
			TryAllServersFailedTwice,
			AfterTryingWithDefaultUrlTwice
		}
	}
}

#endif
