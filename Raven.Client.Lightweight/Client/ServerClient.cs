using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Http.Exceptions;
using Raven.Http.Extensions;
using Raven.Http.Json;

namespace Raven.Client.Client
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

		/// <summary>
		/// Initializes a new instance of the <see cref="ServerClient"/> class.
		/// </summary>
		/// <param name="url">The URL.</param>
		/// <param name="convention">The convention.</param>
		/// <param name="credentials">The credentials.</param>
		/// <param name="replicationInformer">The replication informer.</param>
		public ServerClient(string url, DocumentConvention convention, ICredentials credentials, ReplicationInformer replicationInformer)
		{
			this.credentials = credentials;
			this.replicationInformer = replicationInformer;
			this.url = url;
			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
			replicationInformer.UpdateReplicationInformationIfNeeded(this);
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
		/// Gets the docuent for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument Get(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication(u => DirectGet(u, key));
		}

		private T ExecuteWithReplication<T>(Func<string, T> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			T result;
			var threadSafeCopy = replicationInformer.ReplicationDestinations;
			if (replicationInformer.ShouldExecuteUsing(url, currentRequest))
			{
				if (TryOperation(operation, url, true, out result))
					return result;
				if (replicationInformer.IsFirstFailure(url) && TryOperation(operation, url, threadSafeCopy.Count > 0, out result))
					return result;
				replicationInformer.IncrementFailureCount(url);
			}

			for (int i = 0; i < threadSafeCopy.Count; i++)
			{
				var replicationDestination = threadSafeCopy[i];
				if (replicationInformer.ShouldExecuteUsing(replicationDestination, currentRequest) == false)
					continue;
				if (TryOperation(operation, replicationDestination, true, out result))
					return result;
				if (replicationInformer.IsFirstFailure(url) && TryOperation(operation, replicationDestination, threadSafeCopy.Count > i + 1, out result))
					return result;
				replicationInformer.IncrementFailureCount(url);
			}
			// this should not be thrown, but since I know the value of should...
			throw new InvalidOperationException(@"Attempted to conect to master and all replicas has failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + 1 + threadSafeCopy.Count + " Raven instances.");
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
			return e.InnerException is SocketException;
		}

		/// <summary>
		/// Perform a direct get for a document with the specified key on the sepcified server URL.
		/// </summary>
		/// <param name="serverUrl">The server URL.</param>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument DirectGet(string serverUrl, string key)
		{
			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, serverUrl + "/docs/" + key, "GET", metadata, credentials);
			request.AddOperationHeaders(OperationsHeaders);
			try
			{
				return new JsonDocument
				{
					DataAsJson = JObject.Parse(request.ReadResponseString()),
					NonAuthoritiveInformation = request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
					Key = key,
					Etag = new Guid(request.ResponseHeaders["ETag"]),
					LastModified = DateTime.ParseExact(request.ResponseHeaders["Last-Modified"], "r", CultureInfo.InvariantCulture),
					Metadata = request.ResponseHeaders.FilterHeaders(isServerDocument: false)
				};
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
					var conflictsDoc = JObject.Load(new JsonTextReader(conflicts));
					var conflictIds = conflictsDoc.Value<JArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

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
		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
			return ExecuteWithReplication(u => DirectPut(metadata, key, etag, document, u));
		}

		private PutResult DirectPut(JObject metadata, string key, Guid? etag, JObject document, string operationUrl)
		{
			if (metadata == null)
				metadata = new JObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			AddTransactionInformation(metadata);
			if (etag != null)
				metadata["ETag"] = new JValue(etag.Value.ToString());
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, method, metadata, credentials);
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
			return JsonConvert.DeserializeObject<PutResult>(readResponseString, new JsonEnumConverter());
		}

		private static void AddTransactionInformation(JObject metadata)
		{
			var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			if (transactionInformation == null)
				return;

			string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
			metadata["Raven-Transaction-Information"] = new JValue(txInfo);
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Guid? etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			ExecuteWithReplication<object>(u =>
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
		public void PutAttachment(string key, Guid? etag, byte[] data, JObject metadata)
		{
			var webRequest = WebRequest.Create(url + "/static/" + key);
			webRequest.Method = "PUT";
            webRequest.Credentials = credentials;
			foreach (var header in metadata.Properties())
			{
				if (header.Name.StartsWith("@"))
					continue;

                //need to handle some headers differently, see http://msdn.microsoft.com/en-us/library/system.net.webheadercollection.aspx
                string matchString = header.Name;
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
                    webRequest.Headers[header.Name] = formattedHeaderValue;
			}
			if (etag != null)
			{
				webRequest.Headers[" If-None-Match"] = etag.Value.ToString();
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

                using(var stream = httpWebResponse.GetResponseStream())
                using(var reader = new StreamReader(stream))
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
			var webRequest = WebRequest.Create(url + "/static/" + key);
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
			var webRequest = WebRequest.Create(url + "/static/" + key);
			webRequest.Method = "DELETE";
		    webRequest.Credentials = credentials;
			if (etag != null)
			{
				webRequest.Headers[" If-None-Match"] = etag.Value.ToString();
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
			return ExecuteWithReplication(u => DirectGetIndexNames(start, pageSize, u));
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
			ExecuteWithReplication(u => DirectResetIndex(name, u));
		}

		private object DirectResetIndex(string name, string operationUrl)
		{
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + name, "RESET", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
			return null;
		}

		private string[] DirectGetIndexNames(int start, int pageSize, string operationUrl)
		{
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/indexes/?namesOnly=true&start="+start+"&pageSize="+pageSize, "GET", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			var responseString = httpJsonRequest.ReadResponseString();
			return JArray.Parse(responseString).Select(x => x.Value<string>()).ToArray();
		}

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <returns></returns>
		public IndexDefinition GetIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			return ExecuteWithReplication(u => DirectGetIndex(name, u));
		}

		private IndexDefinition DirectGetIndex(string indexName, string operationUrl)
		{
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + indexName + "?definition=yes", "GET", credentials);
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
			var indexDefResultAsJson = JObject.Load(new JsonTextReader(new StringReader(indexDefAsString)));
			return convention.CreateSerializer().Deserialize<IndexDefinition>(
				new JTokenReader(indexDefResultAsJson["Index"])
				);
		}

		private void DirectDelete(string key, Guid? etag, string operationUrl)
		{
			var metadata = new JObject();
			if (etag != null)
				metadata.Add("ETag", new JValue(etag.Value.ToString()));
			AddTransactionInformation(metadata);
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, "DELETE", metadata, credentials);
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
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		/// <returns></returns>
		public string PutIndex(string name, IndexDefinition definition, bool overwrite)
		{
			EnsureIsNotNullOrEmpty(name, "name");

			string requestUri = url + "/indexes/" + name;
			try
			{
				var webRequest = (HttpWebRequest)WebRequest.Create(requestUri);
				AddOperationHeaders(webRequest);
				webRequest.Method = "HEAD";
				webRequest.Credentials = credentials;

				webRequest.GetResponse().Close();
				if (overwrite == false)
					throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
			}
			catch (WebException e)
			{
				var response = e.Response as HttpWebResponse;
				if (response == null || response.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = HttpJsonRequest.CreateHttpJsonRequest(this, requestUri, "PUT", credentials);
			request.AddOperationHeaders(OperationsHeaders);
			request.Write(JsonConvert.SerializeObject(definition, new JsonEnumConverter()));

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
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef)
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
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite)
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
			return ExecuteWithReplication(u => DirectQuery(index, query, u, includes));
		}

		private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl, string[] includes)
		{
			string path = query.GetIndexQueryUrl(operationUrl, index, "indexes");
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials);
			request.AddOperationHeaders(OperationsHeaders);
			var serializer = convention.CreateSerializer();
			JToken json;
			try
			{
				using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
					json = (JToken)serializer.Deserialize(reader);
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					throw new InvalidOperationException("There is no index named: " + index);
				throw;
			}
			return new QueryResult
			{
				IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
                IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
				Results = json["Results"].Children().Cast<JObject>().ToList(),
				Includes = json["Includes"].Children().Cast<JObject>().ToList(),
				TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
                SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString()),
			};
		}

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/indexes/" + name, "DELETE", credentials);
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
			return ExecuteWithReplication(u => DirectGet(ids, u, includes));
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
			var path = operationUrl + "/queries/";
			if (includes != null && includes.Length > 0)
			{
				path += "?" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "POST", credentials);
			
			request.AddOperationHeaders(OperationsHeaders);
			request.Write(new JArray(ids).ToString(Formatting.None));
			var result = JObject.Parse(request.ReadResponseString());

			return new MultiLoadResult
			{
				Includes = result.Value<JArray>("Includes").Cast<JObject>().ToList(),
				Results = result.Value<JArray>("Results").Cast<JObject>().ToList()
			};
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command datas.</param>
		/// <returns></returns>
		public BatchResult[] Batch(ICommandData[] commandDatas)
		{
			return ExecuteWithReplication(u => DirectBatch(commandDatas, u));
		}

		private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, string operationUrl)
		{
			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var req = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/bulk_docs", "POST", metadata, credentials);
			req.AddOperationHeaders(OperationsHeaders);
			var jArray = new JArray(commandDatas.Select(x => x.ToJson()));
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
			return JsonConvert.DeserializeObject<BatchResult[]>(response);
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Commit(Guid txId)
		{
			ExecuteWithReplication<object>(u =>
			{
				DirectCommit(txId, u);
				return null;
			});
		}

		private void DirectCommit(Guid txId, string operationUrl)
		{
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/transaction/commit?tx=" + txId, "POST", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Rollback(Guid txId)
		{
			ExecuteWithReplication<object>(u =>
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
			return ExecuteWithReplication(u => DirectPromoteTransaction(fromTxId, u));
		}

	    /// <summary>
	    /// Stores the recovery information.
	    /// </summary>
        /// <param name="resourceManagerId">The resource manager Id for this transaction</param>
	    /// <param name="txId">The tx id.</param>
	    /// <param name="recoveryInformation">The recovery information.</param>
	    public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			ExecuteWithReplication<object>(u =>
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
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/transaction/rollback?tx=" + txId, "POST", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			httpJsonRequest.ReadResponseString();
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(url, convention, credentialsForSession, replicationInformer);
		}

        /// <summary>
        /// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
        /// with the specified database
        /// </summary>
	    public IDatabaseCommands ForDatabase(string database)
	    {
	        var databaseUrl = url;
            if (databaseUrl.EndsWith("/") == false)
                databaseUrl += "/";
	        databaseUrl = databaseUrl + "databases/" + database + "/";
            return new ServerClient(databaseUrl, convention, credentials, replicationInformer);
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
			ExecuteWithReplication<object>(operationUrl =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "DELETE", credentials);
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
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{

			ExecuteWithReplication<object>(operationUrl =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "PATCH", credentials);
				request.AddOperationHeaders(OperationsHeaders);
				request.Write(new JArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented));
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

		#endregion
	}
}
