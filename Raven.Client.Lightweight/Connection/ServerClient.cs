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
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Access the RavenDB operations using HTTP
	/// </summary>
	public class ServerClient : IDatabaseCommands
	{
		private int requestCount;

		private readonly string url;
		private readonly DocumentConvention convention;
		private readonly ICredentials credentials;
		private readonly ReplicationInformer replicationInformer;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? currentSessionId;
		private readonly ProfilingInformation profilingInformation;

		/// <summary>
		/// Initializes a new instance of the <see cref="ServerClient"/> class.
		/// </summary>
		public ServerClient(string url, DocumentConvention convention, ICredentials credentials, ReplicationInformer replicationInformer, HttpJsonRequestFactory jsonRequestFactory, Guid? currentSessionId)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(currentSessionId);
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.currentSessionId = currentSessionId;
			this.replicationInformer = replicationInformer;
			this.url = url;

			if (url.EndsWith("/"))
				this.url = url.Substring(0, url.Length - 1);

			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
			replicationInformer.UpdateReplicationInformationIfNeeded(this);
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
		/// and return the result as a string
		/// </summary>
		/// <param name="requestUrl">The relative url to the server</param>
		/// <remarks>
		/// This method respects the replication semantics against the database.
		/// </remarks>
		public string ExecuteGetRequest(string requestUrl)
		{
			EnsureIsNotNullOrEmpty(requestUrl, "url");
			return ExecuteWithReplication("GET", serverUrl =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, serverUrl + requestUrl, "GET", metadata, credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				return request.ReadResponseString();
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
			throw new InvalidOperationException(@"Attempted to conect to master and all replicas have failed, giving up.
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
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, serverUrl + "/docs/" + key, "GET", metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			try
			{
				var requestString = request.ReadResponseString();
				return SerializationHelper.DeserializeJsonDocument(key, requestString, request.ResponseHeaders, request.ResponseStatusCode);
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
						ConflictedVersionIds = conflictIds
					};
				}
				throw;
			}
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
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, actualUrl, "GET", metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			string readResponseString;
			try
			{
				readResponseString = request.ReadResponseString();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			return SerializationHelper.RavenJObjectsToJsonDocuments(RavenJArray.Parse(readResponseString).OfType<RavenJObject>()).ToArray();
		}

		private PutResult DirectPut(RavenJObject metadata, string key, Guid? etag, RavenJObject document, string operationUrl)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			AddTransactionInformation(metadata);
			if (etag != null)
				metadata["ETag"] = new RavenJValue(etag.Value.ToString());
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, method, metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			request.Write(document.ToString());

			string readResponseString;
			try
			{
				readResponseString = request.ReadResponseString();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			return JsonConvert.DeserializeObject<PutResult>(readResponseString, Default.Converters);
		}

		private static void AddTransactionInformation(RavenJObject metadata)
		{
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
		public void PutAttachment(string key, Guid? etag, byte[] data, RavenJObject metadata)
		{
			ExecuteWithReplication("PUT", operationUrl => DirectPutAttachment(key, metadata, etag, data, operationUrl));
		}

		private void DirectPutAttachment(string key, RavenJObject metadata, Guid? etag, byte[] data, string operationUrl)
		{
			var webRequest = WebRequest.Create(operationUrl + "/static/" + key);
			webRequest.Method = "PUT";
			webRequest.Credentials = credentials;
			foreach (var header in metadata)
			{
				if (header.Key.StartsWith("@"))
					continue;

				//need to handle some headers differently, see http://msdn.microsoft.com/en-us/library/system.net.webheadercollection.aspx
				string matchString = header.Key;
				string formattedHeaderValue = StripQuotesIfNeeded(header.Value.ToString(Formatting.None));

				//Just let an exceptions (from Parse(..) functions) bubble-up, so that the user can see they've provided an invalid value
				if (matchString == "Content-Length")
				{
					// we filter out content length, because getting it wrong will cause errors 
					// in the server side when serving the wrong value for this header.
					// worse, if we are using http compression, this value is known to be wrong
					// instead, we rely on the actual size of the data provided for us
					//webRequest.ContentLength = long.Parse(formattedHeaderValue); 	
				}
				else if (matchString == "Content-Type")
					webRequest.ContentType = formattedHeaderValue;
				else
					webRequest.Headers[header.Key] = formattedHeaderValue;
			}
			if (etag != null)
			{
				webRequest.Headers["If-None-Match"] = etag.Value.ToString();
			}
			using (var stream = webRequest.GetRequestStream())
			{
				stream.Write(data, 0, data.Length);
				stream.Flush();
			}
			try
			{
				using (webRequest.GetResponse())
				{

				}
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.InternalServerError)
					throw;

				using (var stream = httpWebResponse.GetResponseStream())
				using (var reader = new StreamReader(stream))
				{
					throw new InvalidOperationException("Internal Server Error: " + Environment.NewLine + reader.ReadToEnd());
				}
			}
		}

		private static string StripQuotesIfNeeded(string str)
		{
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
			return ExecuteWithReplication("GET", operationUrl => DirectGetAttachment(key, operationUrl));
		}

		private Attachment DirectGetAttachment(string key, string operationUrl)
		{
			var webRequest = WebRequest.Create(operationUrl + "/static/" + key);
			webRequest.Credentials = credentials;
			try
			{
				using (var response = webRequest.GetResponse())
				using (var responseStream = response.GetResponseStream())
				{
					return new Attachment
					{
						Data = responseStream.ReadData(),
						Etag = new Guid(response.Headers["ETag"]),
						Metadata = response.Headers.FilterHeaders(isServerDocument: false)
					};
				}
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
						ConflictedVersionIds = conflictIds
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

		public string[] GetDatabaseNames()
		{
			var result = ExecuteGetRequest(url.Databases().NoCache());

			var json = (RavenJArray) RavenJToken.Parse(result);
		
			return json
				.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
				.ToArray();
		}

		private void DirectDeleteAttachment(string key, Guid? etag, string operationUrl)
		{
			var webRequest = WebRequest.Create(operationUrl + "/static/" + key);
			webRequest.Method = "DELETE";
			webRequest.Credentials = credentials;
			if (etag != null)
			{
				webRequest.Headers["If-None-Match"] = etag.Value.ToString();
			}

			using (webRequest.GetResponse())
			{

			}
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
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + name, "RESET", credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
			return null;
		}

		private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize, "GET", credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			var responseString = httpJsonRequest.ReadResponseString();
			return RavenJArray.Parse(responseString).Select(x => x.Value<string>()).ToArray();
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
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + indexName + "?definition=yes", "GET", credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			string indexDefAsString;
			try
			{
				indexDefAsString = httpJsonRequest.ReadResponseString();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null &&
					httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}
			var indexDefResultAsJson = RavenJObject.Load(new JsonTextReader(new StringReader(indexDefAsString)));
			return convention.CreateSerializer().Deserialize<IndexDefinition>(
				new RavenJTokenReader(indexDefResultAsJson["Index"])
				);
		}

		private void DirectDelete(string key, Guid? etag, string operationUrl)
		{
			var metadata = new RavenJObject();
			if (etag != null)
				metadata.Add("ETag", etag.Value.ToString());
			AddTransactionInformation(metadata);
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, "DELETE", metadata, credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			try
			{
				httpJsonRequest.ReadResponseString();
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

			var checkIndexExists = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "HEAD", credentials, convention);
			checkIndexExists.AddOperationHeaders(OperationsHeaders);

			try
			{
				checkIndexExists.ReadResponseString();
				if (overwrite == false)
					throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			request.Write(JsonConvert.SerializeObject(definition, Default.Converters));

			var obj = new { index = "" };
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

		private void AddOperationHeaders(HttpWebRequest webRequest)
		{
			foreach (string header in OperationsHeaders)
			{
				webRequest.Headers[header] = OperationsHeaders[header];
			}
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
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			RavenJObject json;
			try
			{
				using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
					json = (RavenJObject)RavenJToken.Load(reader);
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					throw new InvalidOperationException("There is no index named: " + index);
				throw;
			}
			return SerializationHelper.ToQueryResult(json, request.ResponseHeaders["ETag"]);
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
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + name, "DELETE", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			request.ReadResponseString();
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
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load that many items are probably going to be rare
			HttpJsonRequest request;
			if (ids.Sum(x=>x.Length) < 1024)
			{
				path += "&" + string.Join("&", ids.Select(x => "id=" + x).ToArray());
				request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);
			}
			else
			{
				request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "POST", credentials, convention);
				request.Write(new RavenJArray(ids).ToString(Formatting.None));
			}

			request.AddOperationHeaders(OperationsHeaders);
			var result = RavenJObject.Parse(request.ReadResponseString());

			return new MultiLoadResult
			{
				Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
				Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
			};
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
			var req = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/bulk_docs", "POST", metadata, credentials, convention);
			req.AddOperationHeaders(OperationsHeaders);
			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
			req.Write(jArray.ToString(Formatting.None));

			string response;
			try
			{
				response = req.ReadResponseString();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}
			return JsonConvert.DeserializeObject<BatchResult[]>(response, Default.Converters);
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
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/transaction/commit?tx=" + txId, "POST", credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
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
				var webRequest = (HttpWebRequest)WebRequest.Create(u + "/static/transactions/recoveryInformation/" + txId);
				AddOperationHeaders(webRequest);
				webRequest.Method = "PUT";
				webRequest.Headers["Resource-Manager-Id"] = resourceManagerId.ToString();
				webRequest.Credentials = credentials;
				webRequest.UseDefaultCredentials = true;

				using (var stream = webRequest.GetRequestStream())
				{
					stream.Write(recoveryInformation, 0, recoveryInformation.Length);
				}

				webRequest.GetResponse()
					.Close();

				return null;
			});
		}

		private byte[] DirectPromoteTransaction(Guid fromTxId, string operationUrl)
		{
			var webRequest = (HttpWebRequest)WebRequest.Create(operationUrl + "/transaction/promote?fromTxId=" + fromTxId);
			AddOperationHeaders(webRequest);
			webRequest.Method = "POST";
			webRequest.ContentLength = 0;
			webRequest.Credentials = credentials;
			webRequest.UseDefaultCredentials = true;

			using (var response = webRequest.GetResponse())
			{
				using (var stream = response.GetResponseStreamWithHttpDecompression())
				{
					return stream.ReadData();
				}
			}
		}

		private void DirectRollback(Guid txId, string operationUrl)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, operationUrl + "/transaction/rollback?tx=" + txId, "POST", credentials, convention);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(url, convention, credentialsForSession, replicationInformer, jsonRequestFactory, currentSessionId);
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = url;
			var indexOfDatabases = databaseUrl.IndexOf("/databases/");
			if (indexOfDatabases != -1)
				databaseUrl = databaseUrl.Substring(0, indexOfDatabases);
			if (databaseUrl.EndsWith("/") == false)
				databaseUrl += "/";
			databaseUrl = databaseUrl + "databases/" + database;
			return new ServerClient(databaseUrl, convention, credentials, replicationInformer, jsonRequestFactory, currentSessionId);
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IDatabaseCommands GetRootDatabase()
		{
			var indexOfDatabases = url.IndexOf("/databases/");
			if (indexOfDatabases == -1)
				return this;

			return new ServerClient(url.Substring(0, indexOfDatabases), convention, credentials, replicationInformer, jsonRequestFactory, currentSessionId);
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
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "DELETE", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);
				try
				{
					request.ReadResponseString();
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
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "PATCH", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);
				request.Write(new RavenJArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented));
				try
				{
					request.ReadResponseString();
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

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				RavenJObject json;
				try
				{
					using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
						json = (RavenJObject) RavenJToken.Load(reader);
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.InternalServerError)
						throw new InvalidOperationException("could not execute suggestions at this time");
					throw;
				}

				return new SuggestionQueryResult
				{
					Suggestions = ((RavenJArray) json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
				};
			});
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this,url +"/stats", "GET", credentials, convention);

			var response = httpJsonRequest.ReadResponseString();
			var jo = RavenJObject.Parse(response);
			return jo.Deserialize<DatabaseStatistics>(convention);
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

			                              	var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this,
			                              	                                                               multiGetOperation.
			                              	                                                               	RequestUri, "POST",
			                              	                                                               credentials, convention);

			                              	var requestsForServer =
			                              		multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

			                              	var postedData = JsonConvert.SerializeObject(requestsForServer);

			                              	if (multiGetOperation.CanFullyCache(jsonRequestFactory, httpJsonRequest, postedData))
			                              	{
			                              		return multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length],
			                              		                                               jsonRequestFactory);
			                              	}

			                              	httpJsonRequest.Write(postedData);
			                              	var responses =
			                              		JsonConvert.DeserializeObject<GetResponse[]>(httpJsonRequest.ReadResponseString());

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

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
				{
					return RavenJToken.Load(reader).Values<string>();
				}
			});
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		/// <param name="index"></param>
		/// <param name="query"></param>
		/// <param name="facetSetupDoc"></param>
		/// <returns></returns>
		public IDictionary<string, IEnumerable<FacetValue>> GetFacets(string index, IndexQuery query, string facetSetupDoc)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = operationUrl + string.Format("/facets/{0}?facetDoc={1}&query={2}",
				                                     Uri.EscapeUriString(index),
				                                     Uri.EscapeDataString(facetSetupDoc),
				                                     Uri.EscapeDataString(query.Query));

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
				{
					var json = (RavenJObject) RavenJToken.Load(reader);
					var jsonAsType = json.JsonDeserialization<IDictionary<string, IEnumerable<FacetValue>>>();
					return jsonAsType;
				}
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
