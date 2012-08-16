#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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
	/// <summary>
	/// Access the RavenDB operations using HTTP
	/// </summary>
	public class ServerClient : IDatabaseCommands
	{
		private static int requestCount;

		private readonly string url;
		private readonly DocumentConvention convention;
		private readonly ICredentials credentials;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? currentSessionId;
		private readonly ProfilingInformation profilingInformation;
		private int readStripingBase;

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
		public ServerClient(string url, DocumentConvention convention, ICredentials credentials, Func<string, ReplicationInformer> replicationInformerGetter, string databaseName, HttpJsonRequestFactory jsonRequestFactory, Guid? currentSessionId)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(currentSessionId);
			this.credentials = credentials;
			this.replicationInformerGetter = replicationInformerGetter;
			this.databaseName = databaseName;
			this.replicationInformer = replicationInformerGetter(databaseName);
			this.jsonRequestFactory = jsonRequestFactory;
			this.currentSessionId = currentSessionId;
			this.url = url;

			if (url.EndsWith("/"))
				this.url = url.Substring(0, url.Length - 1);

			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
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
			get;
			set;
		}

		/// <summary>
		/// Gets the document for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument Get(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication("GET", u => DirectGet(u, key));
		}

		/// <summary>
		/// Gets documents for the specified key prefix
		/// </summary>
		public JsonDocument[] StartsWith(string keyPrefix, int start, int pageSize)
		{
			EnsureIsNotNullOrEmpty(keyPrefix, "keyPrefix");

			return ExecuteWithReplication("GET", u => DirectStartsWith(u, keyPrefix, start, pageSize));

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
			EnsureIsNotNullOrEmpty(requestUrl, "url");
			return ExecuteWithReplication("GET", serverUrl =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, serverUrl + requestUrl, "GET", metadata, credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				return request.ReadResponseJson();
			});
		}
		private void ExecuteWithReplication(string method, Action<string> operation)
		{
			ExecuteWithReplication<object>(method, operationUrl =>
			{
				operation(operationUrl);
				return null;
			});
		}

		/// <summary>
		/// Allow to query whatever we are in failover mode or not
		/// </summary>
		/// <returns></returns>
		public bool InFailoverMode()
		{
			return replicationInformer.GetFailureCount(url) > 0;
		}

		private T ExecuteWithReplication<T>(string method, Func<string, T> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			T result;
			var threadSafeCopy = replicationInformer.ReplicationDestinations;

			var shouldReadFromAllServers = ((convention.FailoverBehavior & FailoverBehavior.ReadFromAllServers) == FailoverBehavior.ReadFromAllServers);
			if (shouldReadFromAllServers && method == "GET")
			{
				var replicationIndex = readStripingBase % (threadSafeCopy.Count + 1);
				// if replicationIndex == destinations count, then we want to use the master
				// if replicationIndex < 0, then we were explicitly instructed to use the master
				if (replicationIndex < threadSafeCopy.Count && replicationIndex >= 0)
				{
					// if it is failing, ignore that, and move to the master or any of the replicas
					if (replicationInformer.ShouldExecuteUsing(threadSafeCopy[replicationIndex], currentRequest, method, false))
					{
						if (TryOperation(operation, threadSafeCopy[replicationIndex], true, out result))
							return result;
					}
				}
			}

			if (replicationInformer.ShouldExecuteUsing(url, currentRequest, method, true))
			{
				if (TryOperation(operation, url, true, out result))
					return result;
				if (replicationInformer.IsFirstFailure(url) && TryOperation(operation, url, threadSafeCopy.Count > 0, out result))
					return result;
				replicationInformer.IncrementFailureCount(url);
			}

			for (var i = 0; i < threadSafeCopy.Count; i++)
			{
				var replicationDestination = threadSafeCopy[i];
				if (replicationInformer.ShouldExecuteUsing(replicationDestination, currentRequest, method, false) == false)
					continue;
				if (TryOperation(operation, replicationDestination, true, out result))
					return result;
				if (replicationInformer.IsFirstFailure(url) && TryOperation(operation, replicationDestination, threadSafeCopy.Count > i + 1, out result))
					return result;
				replicationInformer.IncrementFailureCount(url);
			}
			// this should not be thrown, but since I know the value of should...
			throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + threadSafeCopy.Count) + " Raven instances.");
		}



		private bool TryOperation<T>(Func<string, T> operation, string operationUrl, bool avoidThrowing, out T result)
		{
			try
			{
				result = operation(operationUrl);
				replicationInformer.ResetFailureCount(operationUrl);
				return true;
			}
			catch (WebException e)
			{
				if (avoidThrowing == false)
					throw;
				result = default(T);
				if (IsServerDown(e))
					return false;
				throw;
			}
		}

		private static bool IsServerDown(WebException e)
		{
			return e.InnerException is SocketException ||
				e.InnerException is IOException;
		}

		/// <summary>
		/// Perform a direct get for a document with the specified key on the specified server URL.
		/// </summary>
		/// <param name="serverUrl">The server URL.</param>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument DirectGet(string serverUrl, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + key, "GET", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			try
			{
				var responseJson = request.ReadResponseJson();
				var docKey = request.ResponseHeaders[Constants.DocumentIdFieldName] ?? key;
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
					var conflictsDoc = RavenJObject.Load(new JsonTextReader(conflicts));
					var etag = httpWebResponse.GetResponseHeader("ETag");

					throw CreateConcurrencyException(key, conflictsDoc, etag);
				}
				throw;
			}
		}

		private static ConflictException CreateConcurrencyException(string key, RavenJObject conflictsDoc, string etag)
		{
			var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

			return new ConflictException("Conflict detected on " + key +
			                            ", conflict must be resolved before the document will be accessible")
			{
				ConflictedVersionIds = conflictIds,
				Etag = new Guid(etag)
			};
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		public PutResult Put(string key, Guid? etag, RavenJObject document, RavenJObject metadata)
		{
			return ExecuteWithReplication("PUT", u => DirectPut(metadata, key, etag, document, u));
		}

		private JsonDocument[] DirectStartsWith(string operationUrl, string keyPrefix, int start, int pageSize)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var actualUrl = string.Format("{0}/docs?startsWith={1}&start={2}&pageSize={3}", operationUrl, Uri.EscapeDataString(keyPrefix), start, pageSize);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			

			RavenJToken responseJson;
			try
			{
				responseJson = request.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			return SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)responseJson).OfType<RavenJObject>()).ToArray();
		}

		private PutResult DirectPut(RavenJObject metadata, string key, Guid? etag, RavenJObject document, string operationUrl)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			AddTransactionInformation(metadata);
			if (etag != null)
				metadata["ETag"] = new RavenJValue(etag.Value.ToString());
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/docs/" + key, method, metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			request.Write(document.ToString());

			RavenJToken responseJson;
			try
			{
				responseJson = request.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			var jsonSerializer = convention.CreateSerializer();
			return jsonSerializer.Deserialize<PutResult>(new RavenJTokenReader(responseJson));
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
		public void Delete(string key, Guid? etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			ExecuteWithReplication<object>("DELETE", u =>
			{
				DirectDelete(key, etag, u);
				return null;
			});
		}

		/// <summary>
		/// Puts the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public void PutAttachment(string key, Guid? etag, Stream data, RavenJObject metadata)
		{
			ExecuteWithReplication("PUT", operationUrl => DirectPutAttachment(key, metadata, etag, data, operationUrl));
		}

		private void DirectPutAttachment(string key, RavenJObject metadata, Guid? etag, Stream data, string operationUrl)
		{
			if (etag != null)
			{
				metadata["ETag"] = etag.Value.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, "PUT", metadata, credentials, convention));

			webRequest.Write(data);
			try
			{
				webRequest.ExecuteRequest();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.InternalServerError)
					throw;

				using (var stream = httpWebResponse.GetResponseStreamWithHttpDecompression())
				using (var reader = new StreamReader(stream))
				{
					throw new InvalidOperationException("Internal Server Error: " + Environment.NewLine + reader.ReadToEnd());
				}
			}
		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
			return ExecuteWithReplication("GET", operationUrl => DirectGetAttachment("GET", key, operationUrl));
		}

		/// <summary>
		/// Retrieves the attachment metadata with the specified key, not the actual attachmet
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment HeadAttachment(string key)
		{
			return ExecuteWithReplication("HEAD", operationUrl => DirectGetAttachment("HEAD", key, operationUrl));
		}

		private Attachment DirectGetAttachment(string method, string key, string operationUrl)
		{
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, method, credentials, convention));
			Func<Stream> data;
			try
			{
				int len;
				if (method == "GET")
				{
					var memoryStream = new MemoryStream(webRequest.ReadResponseBytes());
					data = () => memoryStream;
					len = (int)memoryStream.Length;
				}
				else
				{
					len = int.Parse(webRequest.ResponseHeaders["Content-Length"]);
					data = () =>
					{
						throw new InvalidOperationException("Cannot get attachment data because it was loaded using: " + method);
					};
				}
				return new Attachment
				{
					Data = data,
					Size = len,
					Etag = new Guid(webRequest.ResponseHeaders["ETag"]),
					Metadata = webRequest.ResponseHeaders.FilterHeaders(isServerDocument: false)
				};
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
				{
					var conflictsDoc = RavenJObject.Load(new BsonReader(httpWebResponse.GetResponseStreamWithHttpDecompression()));
					var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

					throw new ConflictException("Conflict detected on " + key +
												", conflict must be resolved before the attachment will be accessible")
					{
						ConflictedVersionIds = conflictIds,
						Etag = new Guid(httpWebResponse.GetResponseHeader("ETag"))
					};
				}
				if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}
		}

		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void DeleteAttachment(string key, Guid? etag)
		{
			ExecuteWithReplication("DELETE", operationUrl => DirectDeleteAttachment(key, etag, operationUrl));
		}

		public string[] GetDatabaseNames(int pageSize)
		{
			var result = ExecuteGetRequest("".Databases(pageSize).NoCache());

			var json = (RavenJArray)result;

			return json
				.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
				.ToArray();
		}

		private void DirectDeleteAttachment(string key, Guid? etag, string operationUrl)
		{
			var metadata = new RavenJObject();
			if (etag != null)
			{
				metadata["ETag"] = etag.Value.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/static/" + key, "DELETE", metadata, credentials, convention));
			webRequest.ExecuteRequest();
		}

		/// <summary>
		/// Gets the index names from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
		public string[] GetIndexNames(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", u => DirectGetIndexNames(start, pageSize, u));
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
			ExecuteWithReplication("RESET", u => DirectResetIndex(name, u));
		}

		private object DirectResetIndex(string name, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + name, "RESET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			httpJsonRequest.ReadResponseJson();
			return null;
		}

		private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize, "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			var responseJson = httpJsonRequest.ReadResponseJson();
			return ((RavenJArray)responseJson).Select(x => x.Value<string>()).ToArray();
		}

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public IndexDefinition GetIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			return ExecuteWithReplication("GET", u => DirectGetIndex(name, u));
		}

		private IndexDefinition DirectGetIndex(string indexName, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + indexName + "?definition=yes", "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			RavenJToken indexDef;
			try
			{
				indexDef = httpJsonRequest.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null &&
					httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}
			var value = indexDef.Value<RavenJObject>("Index");
			return convention.CreateSerializer().Deserialize<IndexDefinition>(
				new RavenJTokenReader(value)
				);
		}

		private void DirectDelete(string key, Guid? etag, string operationUrl)
		{
			var metadata = new RavenJObject();
			if (etag != null)
				metadata.Add("ETag", etag.Value.ToString());
			AddTransactionInformation(metadata);
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/docs/" + key, "DELETE", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			try
			{
				httpJsonRequest.ExecuteRequest();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
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

		/// <summary>
		/// Puts the index.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The definition.</param>
		/// <returns></returns>
		public string PutIndex(string name, IndexDefinition definition)
		{
			return PutIndex(name, definition, false);
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
			EnsureIsNotNullOrEmpty(name, "name");

			return ExecuteWithReplication("PUT", operationUrl => DirectPutIndex(name, operationUrl, overwrite, definition));
		}

		private string DirectPutIndex(string name, string operationUrl, bool overwrite, IndexDefinition definition)
		{
			string requestUri = operationUrl + "/indexes/" + name;

			var checkIndexExists = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "HEAD", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			

			try
			{
				checkIndexExists.ExecuteRequest();
				if (overwrite == false)
					throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "PUT", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			request.Write(JsonConvert.SerializeObject(definition, Default.Converters));


			var responseJson = (RavenJObject)request.ReadResponseJson();
			return responseJson.Value<string>("index");
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
		public QueryResult Query(string index, IndexQuery query, string[] includes)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			return ExecuteWithReplication("GET", u => DirectQuery(index, query, u, includes));
		}

		private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl, string[] includes)
		{
			string path = query.GetIndexQueryUrl(operationUrl, index, "indexes");
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, path, "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			

			RavenJObject json;
			try
			{
				json = (RavenJObject)request.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
				{
					var text = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression()).ReadToEnd();
					if (text.Contains("maxQueryString"))
						throw new InvalidOperationException(text, e);
					throw new InvalidOperationException("There is no index named: " + index);
				}
				throw;
			}
			var directQuery = SerializationHelper.ToQueryResult(json, request.ResponseHeaders["ETag"]);
			foreach (var docResult in directQuery.Results.Concat(directQuery.Includes))
			{
				AssertNonConflictedDocument(docResult);
			}
			return directQuery;
		}

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			ExecuteWithReplication("DELETE", operationUrl => DirectDeleteIndex(name, operationUrl));
		}

		private void DirectDeleteIndex(string name, string operationUrl)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + name, "DELETE", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			request.ExecuteRequest();
		}

		/// <summary>
		/// Gets the results for the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		public MultiLoadResult Get(string[] ids, string[] includes)
		{
			return ExecuteWithReplication("GET", u => DirectGet(ids, u, includes));
		}

		/// <summary>
		/// Perform a direct get for loading multiple ids in one request
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="operationUrl">The operation URL.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		public MultiLoadResult DirectGet(string[] ids, string operationUrl, string[] includes)
		{
			var path = operationUrl + "/queries/?";
			if (includes != null && includes.Length > 0)
			{
				path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var uniqueIds = new HashSet<string>(ids);
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load that many items are probably going to be rare
			HttpJsonRequest request;
			if (uniqueIds.Sum(x => x.Length) < 1024)
			{
				path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + x).ToArray());
				request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "GET", credentials, convention)
							.AddOperationHeaders(OperationsHeaders));
			}
			else
			{
				request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "POST", credentials, convention)
							.AddOperationHeaders(OperationsHeaders));
				request.Write(new RavenJArray(uniqueIds).ToString(Formatting.None));
			}

			
			var result = (RavenJObject)request.ReadResponseJson();

			var results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList();
			var multiLoadResult = new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = ids.Select(id => results.FirstOrDefault(r => string.Equals(r["@metadata"].Value<string>("@id"), id, StringComparison.InvariantCultureIgnoreCase))).ToList()
			};
			foreach (var docResult in multiLoadResult.Results.Concat(multiLoadResult.Includes))
			{
				
				AssertNonConflictedDocument(docResult);
			}
			return multiLoadResult;
		}

		private static void AssertNonConflictedDocument(RavenJObject docResult)
		{
			if (docResult == null)
				return;
			var metadata = docResult[Constants.Metadata];
			if(metadata == null)
				return;

			if (metadata.Value<int>("@Http-Status-Code") == 409)
				throw CreateConcurrencyException(metadata.Value<string>("@id"), docResult, metadata.Value<string>("@etag"));
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			return ExecuteWithReplication("POST", u => DirectBatch(commandDatas, u));
		}

		private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, string operationUrl)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var req = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/bulk_docs", "POST", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
			req.Write(jArray.ToString(Formatting.None));

			RavenJArray response;
			try
			{
				response = (RavenJArray)req.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			return convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Commit(Guid txId)
		{
			ExecuteWithReplication<object>("POST", u =>
			{
				DirectCommit(txId, u);
				return null;
			});
		}

		private void DirectCommit(Guid txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/commit?tx=" + txId, "POST", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			httpJsonRequest.ReadResponseJson();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Rollback(Guid txId)
		{
			ExecuteWithReplication<object>("POST", u =>
			{
				DirectRollback(txId, u);
				return null;
			});
		}

		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public byte[] PromoteTransaction(Guid fromTxId)
		{
			return ExecuteWithReplication("PUT", u => DirectPromoteTransaction(fromTxId, u));
		}

		/// <summary>
		/// Stores the recovery information.
		/// </summary>
		/// <param name="resourceManagerId">The resource manager Id for this transaction</param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			ExecuteWithReplication<object>("PUT", u =>
			{
				var metadata = new RavenJObject
				{
					{"Resource-Manager-Id", resourceManagerId.ToString()},
					{Constants.NotForReplication, true}
				};
				var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, u + "/static/transactions/recoveryInformation/" + txId, "PUT", metadata, credentials, convention)
							.AddOperationHeaders(OperationsHeaders));

				webRequest.Write(new MemoryStream(recoveryInformation));

				webRequest.ExecuteRequest();
				return null;
			});
		}

		private byte[] DirectPromoteTransaction(Guid fromTxId, string operationUrl)
		{
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/promote?fromTxId=" + fromTxId, "POST", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			return webRequest.ReadResponseBytes();
		}

		private void DirectRollback(Guid txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationUrl + "/transaction/rollback?tx=" + txId, "POST", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			httpJsonRequest.ReadResponseJson();
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(url, convention, credentialsForSession, replicationInformerGetter, databaseName, jsonRequestFactory, currentSessionId);
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public void ForceReadFromMaster()
		{
			readStripingBase = -1;// this means that will have to use the master url first
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			databaseUrl = databaseUrl + "/databases/" + database;
			if (databaseUrl == Url)
				return this;
			return new ServerClient(databaseUrl, convention, credentials, replicationInformerGetter, database, jsonRequestFactory, currentSessionId)
				   {
					   OperationsHeaders = OperationsHeaders
				   };
		}

		public IDatabaseCommands ForDefaultDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			if (databaseUrl == Url)
				return this;
			return new ServerClient(databaseUrl, convention, credentials, replicationInformerGetter, null, jsonRequestFactory, currentSessionId)
			{
				OperationsHeaders = OperationsHeaders
			};
		}



		/// <summary>
		/// Gets a value indicating whether [supports promotable transactions].
		/// </summary>
		/// <value>
		/// 	<c>true</c> if [supports promotable transactions]; otherwise, <c>false</c>.
		/// </value>
		public bool SupportsPromotableTransactions
		{
			get { return true; }
		}

		/// <summary>
		/// Gets the URL.
		/// </summary>
		/// <value>The URL.</value>
		public string Url
		{
			get
			{
				return url;
			}
		}

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			ExecuteWithReplication<object>("DELETE", operationUrl =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, "DELETE", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));
				try
				{
					request.ReadResponseJson();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}
				return null;
			});
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)
		{
			UpdateByIndex(indexName, queryToUpdate, patchRequests, false);
		}

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		public void DeleteByIndex(string indexName, IndexQuery queryToDelete)
		{
			DeleteByIndex(indexName, queryToDelete, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			ExecuteWithReplication<object>("PATCH", operationUrl =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, "PATCH", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));
				
				request.Write(new RavenJArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented));
				try
				{
					request.ReadResponseJson();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}
				return null;
			});
		}


		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		/// <returns></returns>
		public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
		{
			if (suggestionQuery == null) throw new ArgumentNullException("suggestionQuery");

			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = operationUrl + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}",
													 Uri.EscapeUriString(index),
													 Uri.EscapeDataString(suggestionQuery.Term),
													 Uri.EscapeDataString(suggestionQuery.Field),
													 Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToString()),
													 Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
													 Uri.EscapeDataString(suggestionQuery.Accuracy.ToString()));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));
				

				RavenJObject json = (RavenJObject)request.ReadResponseJson();

				return new SuggestionQueryResult
				{
					Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
				};
			});
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url + "/stats", "GET", credentials, convention));

			var jo = (RavenJObject)httpJsonRequest.ReadResponseJson();
			return jo.Deserialize<DatabaseStatistics>(convention);
		}

		/// <summary>
		/// Get the full URL for the given document key
		/// </summary>
		public string UrlFor(string documentKey)
		{
			return url + "/docs/" + documentKey;
		}

		/// <summary>
		/// Check if the document exists for the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocumentMetadata Head(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			return ExecuteWithReplication("HEAD", u => DirectHead(u, key));
		}

		/// <summary>
		/// Do a direct HEAD request against the server for the specified document
		/// </summary>
		public JsonDocumentMetadata DirectHead(string serverUrl, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			HttpJsonRequest request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, serverUrl + "/docs/" + key, "HEAD", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));
			
			try
			{
				request.ExecuteRequest();
				return SerializationHelper.DeserializeJsonDocumentMetadata(key, request.ResponseHeaders, request.ResponseStatusCode);
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
					var conflictsDoc = RavenJObject.Load(new JsonTextReader(conflicts));
					var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

					throw new ConflictException("Conflict detected on " + key +
												", conflict must be resolved before the document will be accessible")
					{
						ConflictedVersionIds = conflictIds,
						Etag = new Guid(httpWebResponse.GetResponseHeader("ETag"))
					};
				}
				throw;
			}
		}

		/// <summary>
		/// Perform a single POST requst containing multiple nested GET requests
		/// </summary>
		public GetResponse[] MultiGet(GetRequest[] requests)
		{
			return ExecuteWithReplication("GET", // this is a logical GET, physical POST
										  operationUrl =>
										  {
											  var multiGetOperation = new MultiGetOperation(this, convention, operationUrl, requests);

											  var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.
											                                                                                                       	RequestUri, "POST", credentials, convention));

											  var requestsForServer =
												  multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

											  var postedData = JsonConvert.SerializeObject(requestsForServer);

											  if (multiGetOperation.CanFullyCache(jsonRequestFactory, httpJsonRequest, postedData))
											  {
												  return multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length],
																								 jsonRequestFactory);
											  }

											  httpJsonRequest.Write(postedData);
											  var results = (RavenJArray)httpJsonRequest.ReadResponseJson();
											  var responses = convention.CreateSerializer().Deserialize<GetResponse[]>(
																				  new RavenJTokenReader(results));

											  return multiGetOperation.HandleCachingResponse(responses, jsonRequestFactory);
										  });
		}

		///<summary>
		/// Get the possible terms for the specified field in the index 
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = operationUrl + string.Format("/terms/{0}?field={1}&pageSize={2}&fromValue={3}",
													 Uri.EscapeUriString(index),
													 Uri.EscapeDataString(field),
													 pageSize,
													 Uri.EscapeDataString(fromValue ?? ""));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));
				

				return request.ReadResponseJson().Values<string>();
			});
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		/// <param name="index"></param>
		/// <param name="query"></param>
		/// <param name="facetSetupDoc"></param>
		/// <returns></returns>
		public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = operationUrl + string.Format("/facets/{0}?facetDoc={1}&query={2}",
															  Uri.EscapeUriString(index),
															  Uri.EscapeDataString(facetSetupDoc),
															  Uri.EscapeUriString(Uri.EscapeDataString(query.Query)));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));
				
				var json = (RavenJObject)request.ReadResponseJson();
				return json.JsonDeserialization<FacetResults>();
			});
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public void Patch(string key, PatchRequest[] patches)
		{
			Patch(key, patches, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public void Patch(string key, PatchRequest[] patches, Guid? etag)
		{
			Batch(new[]
			      	{
			      		new PatchCommandData
			      			{
			      				Key = key,
			      				Patches = patches,
			      				Etag = etag
			      			}
			      	});
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return jsonRequestFactory.DisableAllCaching();
		}



		#endregion

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (ProfilingInformation != null)
			{
				ProfilingInformation.DurationMilliseconds = (SystemTime.Now - ProfilingInformation.At).TotalMilliseconds;
			}
		}

		/// <summary>
		/// Allows an <see cref="T:System.Object"/> to attempt to free resources and perform other cleanup operations before the <see cref="T:System.Object"/> is reclaimed by garbage collection.
		/// </summary>
		~ServerClient()
		{
			Dispose();
		}
	}
}
#endif
