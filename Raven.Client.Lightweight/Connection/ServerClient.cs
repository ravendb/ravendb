#if !SILVERLIGHT && !NETFX_CORE
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
using System.Text;
using Raven.Abstractions.Json;
using Raven.Client.Changes;
using Raven.Client.Listeners;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
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
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Access the RavenDB operations using HTTP
	/// </summary>
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;
		private readonly DocumentConvention convention;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? currentSessionId;
		private readonly IDocumentConflictListener[] conflictListeners;
		private readonly ProfilingInformation profilingInformation;
		private int readStripingBase;

		private bool resolvingConflict;
		private bool resolvingConflictRetries;

		public OperationCredentials Credentials { get; private set; }

		private readonly ICredentials _credentials;
		private readonly string _apiKey;

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
		public ServerClient(string url, DocumentConvention convention, string apiKey, ICredentials credentials, Func<string, ReplicationInformer> replicationInformerGetter, string databaseName, HttpJsonRequestFactory jsonRequestFactory, Guid? currentSessionId, IDocumentConflictListener[] conflictListeners)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(currentSessionId);
			Credentials = new OperationCredentials(apiKey, credentials);
			this._apiKey = apiKey;
			this._credentials = credentials;
			this.replicationInformerGetter = replicationInformerGetter;
			this.databaseName = databaseName;
			this.replicationInformer = replicationInformerGetter(databaseName);
			this.jsonRequestFactory = jsonRequestFactory;
			this.currentSessionId = currentSessionId;
			this.conflictListeners = conflictListeners;
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
			get; set;
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
		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize, bool metadataOnly = false, string exclude = null)
		{
			EnsureIsNotNullOrEmpty(keyPrefix, "keyPrefix");
			return ExecuteWithReplication("GET", u => DirectStartsWith(u, keyPrefix, matches, exclude, start, pageSize, metadataOnly));
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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, "GET", metadata, operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				return request.ReadResponseJson();
			});
		}

	    public HttpJsonRequest CreateRequest(string method, string requestUrl,
	        bool disableRequestCompression = false)
	    {
	        return CreateRequest(new OperationMetadata(url, Credentials), method, requestUrl, disableRequestCompression);
	    }

        public HttpJsonRequest CreateRequest(OperationMetadata operationMetadata, string method, string requestUrl, bool disableRequestCompression = false)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, method, metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders);
			createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
			return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
		}

		private void ExecuteWithReplication(string method, Action<OperationMetadata> operation)
		{
			ExecuteWithReplication<object>(method, operationMetadata =>
			{
				operation(operationMetadata);
				return null;
			});
		}

		private T ExecuteWithReplication<T>(string method, Func<OperationMetadata, T> operation)
		{
			int currentRequest = convention.IncrementRequestCount();
			return replicationInformer.ExecuteWithReplication(method, url, Credentials, currentRequest, readStripingBase, operation);
		}

		/// <summary>
		/// Allow to query whatever we are in failover mode or not
		/// </summary>
		/// <returns></returns>
		public bool InFailoverMode()
		{
			return replicationInformer.GetFailureCount(url) > 0;
		}

		/// <summary>
		/// Perform a direct get for a document with the specified key on the specified server URL.
		/// </summary>
		/// <param name="operationMetadata">The metadata that contains URL and credentials to perform operation</param>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument DirectGet(OperationMetadata operationMetadata, string key, string transformer = null)
		{
			if (key.Length > 127 || string.IsNullOrEmpty(transformer) == false)
			{
				// avoid hitting UrlSegmentMaxLength limits in Http.sys
				var multiLoadResult = DirectGet(new[] {key}, operationMetadata, new string[0], transformer, new Dictionary<string, RavenJToken>(), false);
				var result = multiLoadResult.Results.FirstOrDefault();
				if (result == null)
					return null;
				return SerializationHelper.RavenJObjectToJsonDocument(result);
			}

			var metadata = new RavenJObject();
		    var actualUrl = operationMetadata.Url + "/docs/" + Uri.EscapeDataString(key);

			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
							return DirectGet(operationMetadata, key);
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

		private void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)
		{
			if (!primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
			{
				var forceCheck = headers[Constants.RavenForcePrimaryServerCheck];
				bool shouldForceCheck;
				if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
				{
					this.replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
				}
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
                    if (results.Any(x => x == null))
                    {
                        // one of the conflict documents doesn't exist, means that it was already resolved.
                        // we'll reload the relevant documents again
                        return null;
                    }

					foreach (var conflictListener in conflictListeners)
					{
						JsonDocument resolvedDocument;
						if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
						{
                            try
                            {
                                Put(key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata);
                            }
                            catch (ConcurrencyException)
                            {
                                // we are racing the changes API here, so that is fine
                            }

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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + "/docs/?start=" + start + "&pageSize=" + pageSize;
				if (metadataOnly)
					requestUri += "&metadata-only=true";
				RavenJToken result = jsonRequestFactory
					.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.ReadResponseJson();
				return ((RavenJArray)result).Cast<RavenJObject>().ToJsonDocuments().ToArray();
			});
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
			return ExecuteWithReplication("PUT", u => DirectPut(metadata, key, etag, document, u));
		}

		private JsonDocument[] DirectStartsWith(OperationMetadata operationMetadata, string keyPrefix, string matches, string exclude, int start, int pageSize, bool metadataOnly)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var actualUrl = string.Format("{0}/docs?startsWith={1}&matches={4}&exclude={5}&start={2}&pageSize={3}", operationMetadata.Url,
										  Uri.EscapeDataString(keyPrefix), start.ToInvariantString(), pageSize.ToInvariantString(),
                                          Uri.EscapeDataString(matches ?? ""), Uri.EscapeDataString(exclude ?? ""));
			if (metadataOnly)
				actualUrl += "&metadata-only=true";

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


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

		private PutResult DirectPut(RavenJObject metadata, string key, Etag etag, RavenJObject document, OperationMetadata operationMetadata)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			AddTransactionInformation(metadata);
			if (etag != null)
				metadata["ETag"] = new RavenJValue((string)etag);

			if (key != null)
				key = Uri.EscapeDataString(key);

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, method, metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


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
		public void Delete(string key, Etag etag)
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
		public void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata)
		{
			ExecuteWithReplication("PUT", operationUrl => DirectPutAttachment(key, metadata, etag, data, operationUrl));
		}

		/// <summary>
		/// Updates just the attachment with the specified key's metadata
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="metadata">The metadata.</param>
		public void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata)
		{
			ExecuteWithReplication("POST", operationUrl => DirectUpdateAttachmentMetadata(key, metadata, etag, operationUrl));
		}

		private void DirectUpdateAttachmentMetadata(string key, RavenJObject metadata, Etag etag, OperationMetadata operationMetadata)
		{
			if (etag != null)
			{
				metadata["ETag"] = etag.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/" + key, "POST", metadata, operationMetadata.Credentials, convention))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


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

		private void DirectPutAttachment(string key, RavenJObject metadata, Etag etag, Stream data, OperationMetadata operationMetadata)
		{
			if (etag != null)
			{
				metadata["ETag"] = etag.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/" + key, "PUT", metadata, operationMetadata.Credentials, convention))
				.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
											 convention.FailoverBehavior, HandleReplicationStatusChanges);

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
		/// Gets the attachments starting with the specified prefix
		/// </summary>
		public IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentHeadersStartingWith("GET", idPrefix, start, pageSize, operationMetadata));
		}

		private IEnumerable<Attachment> DirectGetAttachmentHeadersStartingWith(string method, string idPrefix, int start, int pageSize, OperationMetadata operationMetadata)
		{
			var webRequest =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																						 operationMetadata.Url + "/static/?startsWith=" +
																						 idPrefix + "&start=" + start + "&pageSize=" +
																						 pageSize, method, operationMetadata.Credentials, convention))
																						 .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			var result = webRequest.ReadResponseJson();

			return convention.CreateSerializer().Deserialize<Attachment[]>(new RavenJTokenReader(result))
				.Select(x => new Attachment
				{
					Etag = x.Etag,
					Metadata = x.Metadata,
					Size = x.Size,
					Key = x.Key,
					Data = () =>
					{
						throw new InvalidOperationException("Cannot get attachment data from an attachment header");
					}
				});
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

		private Attachment DirectGetAttachment(string method, string key, OperationMetadata operationMetadata)
		{
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/" + key, method, operationMetadata.Credentials, convention))
							.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
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
					webRequest.ExecuteRequest();

					len = int.Parse(webRequest.ResponseHeaders["Content-Length"]);
					data = () =>
					{
						throw new InvalidOperationException("Cannot get attachment data because it was loaded using: " + method);
					};
				}

				HandleReplicationStatusChanges(webRequest.ResponseHeaders, Url, operationMetadata.Url);

				return new Attachment
				{
					Data = data,
					Size = len,
					Etag = webRequest.GetEtagHeader(),
					Metadata = webRequest.ResponseHeaders.FilterHeadersAttachment()
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
												", conflict must be resolved before the attachment will be accessible", true)
					{
						ConflictedVersionIds = conflictIds,
						Etag = httpWebResponse.GetEtagHeader()
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
		public void DeleteAttachment(string key, Etag etag)
		{
			ExecuteWithReplication("DELETE", operationUrl => DirectDeleteAttachment(key, etag, operationUrl));
		}

		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
			var result = ExecuteGetRequest("".Databases(pageSize, start).NoCache());

			var json = (RavenJArray)result;

			return json
				.Select(x => x.Value<string>())
				.ToArray();
		}

		public IDictionary<string, RavenJToken> GetDatabases(int pageSize, int start = 0)
		{
			var result = ExecuteGetRequest("".Databases(pageSize, start).NoCache());

			var json = (RavenJArray)result;

			return json
				.ToDictionary(
					x =>
					x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty));
		}

		private void DirectDeleteAttachment(string key, Etag etag, OperationMetadata operationMetadata)
		{
			var metadata = new RavenJObject();
			if (etag != null)
			{
				metadata["ETag"] = etag.ToString();
			}
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/" + key, "DELETE", metadata, operationMetadata.Credentials, convention))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

		public IndexDefinition[] GetIndexes(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", operationMetadata.Credentials, convention));
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				var result = request.ReadResponseJson();
				var json = ((RavenJArray)result);
				//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
				return json
					.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
					.ToArray();
			});
		}

		public TransformerDefinition[] GetTransformers(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/transformers?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", operationMetadata.Credentials, convention));
				request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				var result = request.ReadResponseJson();
				var json = ((RavenJArray)result);
				//NOTE: To review, I'm not confidence this is the correct way to deserialize the transformer definition
				return json
					.Select(x => JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
					.ToArray();
			});
		}

		public TransformerDefinition GetTransformer(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			return ExecuteWithReplication("GET", u => DirectGetTransformer(name, u));
		}

		public void DeleteTransformer(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			ExecuteWithReplication("DELETE", u => DirectDeleteTransformer(name, u));
		}

		private void DirectDeleteTransformer(string name, OperationMetadata operationMetadata)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transformers/" + name, "DELETE", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			request.ExecuteRequest();
		}

		private TransformerDefinition DirectGetTransformer(string transformerName, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transformers/" + transformerName, "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			RavenJToken transformerDef;
			try
			{
				transformerDef = httpJsonRequest.ReadResponseJson();
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null &&
					httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
				throw;
			}

			var value = transformerDef.Value<RavenJObject>("Transformer");
			return convention.CreateSerializer().Deserialize<TransformerDefinition>(new RavenJTokenReader(value));
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
			ExecuteWithReplication("RESET", u => DirectResetIndex(name, u));
		}

		private object DirectResetIndex(string name, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + name, "RESET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


			httpJsonRequest.ReadResponseJson();
			return null;
		}

		private string[] DirectGetIndexNames(int start, int pageSize, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize, "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


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

		private IndexDefinition DirectGetIndex(string indexName, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + indexName + "?definition=yes", "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

		private void DirectDelete(string key, Etag etag, OperationMetadata operationMetadata)
		{
			var metadata = new RavenJObject();
			if (etag != null)
				metadata.Add("ETag", etag.ToString());
			AddTransactionInformation(metadata);
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, "DELETE", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
					actualETag = Etag.Empty,
					expectedETag = Etag.Empty,
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

		public string PutTransformer(string name, TransformerDefinition indexDef)
		{
			EnsureIsNotNullOrEmpty(name, "name");

			return ExecuteWithReplication("PUT", u => DirectPutTransformer(name, u, indexDef));
	
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

			return ExecuteWithReplication("PUT", u => DirectPutIndex(name, u, overwrite, definition));
		}

		public string DirectPutTransformer(string name, OperationMetadata operationMetadata, TransformerDefinition definition)
		{
			string requestUri = operationMetadata.Url + "/transformers/" + name;

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			request.Write(JsonConvert.SerializeObject(definition, Default.Converters));


            try
            {
			    var responseJson = (RavenJObject)request.ReadResponseJson();
			    return responseJson.Value<string>("Transformer");
            }
            catch (WebException e)
            {
                Exception newException;
                if (ShouldRethrowIndexException(e, out newException))
                {
                    if (newException != null)
                        throw new TransformCompilationException(newException.Message, e);
                }
                throw;
            }
		}

	    public string DirectPutIndex(string name, OperationMetadata operationMetadata, bool overwrite, IndexDefinition definition)
	    {
			string requestUri = operationMetadata.Url + "/indexes/" + name;

	        var checkIndexExists = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "HEAD", operationMetadata.Credentials, convention)
	                .AddOperationHeaders(OperationsHeaders))
													 .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
	                                                                              convention.FailoverBehavior,
	                                                                              HandleReplicationStatusChanges);


	        try
	        {
	            // If the index doesn't exist this will throw a NotFound exception and continue with a PUT request
	            checkIndexExists.ExecuteRequest();
	            if (!overwrite)
	                throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
	        }
	        catch (WebException e)
	        {
	            Exception newException;
	            if (ShouldRethrowIndexException(e, out newException))
	            {
	                if (newException != null)
	                    throw newException;
	                throw;
	            }
                
	        }

	        var request = jsonRequestFactory.CreateHttpJsonRequest(
	            new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention)
	                .AddOperationHeaders(OperationsHeaders))
	                                        .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
	                                                                     convention.FailoverBehavior,
	                                                                     HandleReplicationStatusChanges);

	        request.Write(JsonConvert.SerializeObject(definition, Default.Converters));

	        try
	        {
	            var responseJson = (RavenJObject) request.ReadResponseJson();
	            return responseJson.Value<string>("Index");
	        }
	        catch (WebException e)
	        {
                Exception newException;
                if (ShouldRethrowIndexException(e, out newException))
                {
                    if (newException != null)
                        throw newException;
                }
                throw;
            }
	    }

	    private static bool ShouldRethrowIndexException(WebException e, out Exception newEx)
	    {
	        newEx = null;
	        var httpWebResponse = e.Response as HttpWebResponse;
	        if (httpWebResponse == null)
	            return true;

	        if (httpWebResponse.StatusCode == HttpStatusCode.InternalServerError)
	        {
	            var error = e.TryReadErrorResponseObject(
	                new {Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = ""});

	            if (error == null)
	            {
	                return true;
	            }

                newEx = new IndexCompilationException(error.Message, e)
	            {
	                IndexDefinitionProperty = error.IndexDefinitionProperty,
	                ProblematicText = error.ProblematicText
	            };

	            return true;
	        }

	        return (httpWebResponse.StatusCode != HttpStatusCode.NotFound);
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
			EnsureIsNotNullOrEmpty(index, "index");
			return ExecuteWithReplication("GET", u => DirectQuery(index, query, u, includes, metadataOnly, indexEntriesOnly));
		}

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo)
		{
		    EnsureIsNotNullOrEmpty(index, "index");
		    var headerInfo = new Reference<QueryHeaderInformation>();
		    var result = ExecuteWithReplication("GET", u => DirectStreamQuery(u, index, query, headerInfo));
		    queryHeaderInfo = headerInfo.Value;
            return result;
		}

	    private IEnumerator<RavenJObject> DirectStreamQuery(OperationMetadata u, string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo)
	    {
	        string path = query.GetIndexQueryUrl(u.Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false);
	        var request = jsonRequestFactory.CreateHttpJsonRequest(
	            new CreateHttpJsonRequestParams(this, path, "GET", u.Credentials, convention)
	                .AddOperationHeaders(OperationsHeaders))
	            .AddReplicationStatusHeaders(Url, u.Url, replicationInformer,
	                convention.FailoverBehavior,
	                HandleReplicationStatusChanges);

	        request.RemoveAuthorizationHeader();

	        var token = GetSingleAuthToken(u);

	        try
	        {
	            token = ValidateThatWeCanUseAuthenticateTokens(u, token);
	        }
	        catch (Exception e)
	        {
	            throw new InvalidOperationException(
	                "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
	                e);
	        }

	        request.AddOperationHeader("Single-Use-Auth-Token", token);

	        var webResponse = request.RawExecuteRequest();
	        queryHeaderInfo.Value = new QueryHeaderInformation
	        {
	            Index = webResponse.Headers["Raven-Index"],
	            IndexTimestamp =
	                DateTime.ParseExact(webResponse.Headers["Raven-Index-Timestamp"], Default.DateTimeFormatsToRead,
	                    CultureInfo.InvariantCulture, DateTimeStyles.None),
	            IndexEtag = Etag.Parse(webResponse.Headers["Raven-Index-Etag"]),
	            ResultEtag = Etag.Parse(webResponse.Headers["Raven-Result-Etag"]),
	            IsStable = bool.Parse(webResponse.Headers["Raven-Is-Stale"]),
	            TotalResults = int.Parse(webResponse.Headers["Raven-Total-Results"])
	        };

	        return YieldStreamResults(webResponse);
	    }


	    /// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag, string startsWith, string matches, int start, int pageSize, string exclude)
	    {
	        if (fromEtag != null && startsWith != null)
				throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

	        return ExecuteWithReplication("GET",
	            op => DirectStreamDocs(op, fromEtag, startsWith, matches, start, pageSize, exclude));
	    }

	    private IEnumerator<RavenJObject> DirectStreamDocs(OperationMetadata op, Etag fromEtag, string startsWith, string matches, int start, int pageSize, string exclude)
	    {
	        var sb = new StringBuilder(op.Url).Append("/streams/docs?");

	        if (fromEtag != null)
	        {
	            sb.Append("etag=")
	                .Append(fromEtag)
	                .Append("&");
	        }
	        else
	        {
	            if (startsWith != null)
	            {
	                sb.Append("startsWith=").Append(Uri.EscapeDataString(startsWith)).Append("&");
	            }
	            if (matches != null)
	            {
	                sb.Append("matches=").Append(Uri.EscapeDataString(matches)).Append("&");
	            }
	            if (exclude != null)
	            {
	                sb.Append("exclude=").Append(Uri.EscapeDataString(exclude)).Append("&");
	            }
	        }
	        if (start != 0)
	            sb.Append("start=").Append(start).Append("&");
	        if (pageSize != int.MaxValue)
	            sb.Append("pageSize=").Append(pageSize).Append("&");


	        var request = jsonRequestFactory.CreateHttpJsonRequest(
	            new CreateHttpJsonRequestParams(this, sb.ToString(), "GET", op.Credentials, convention)
	                .AddOperationHeaders(OperationsHeaders))
	            .AddReplicationStatusHeaders(Url, op.Url, replicationInformer, convention.FailoverBehavior,
	                HandleReplicationStatusChanges);

	        request.RemoveAuthorizationHeader();

	        var token = GetSingleAuthToken(op);

	        try
	        {
	            token = ValidateThatWeCanUseAuthenticateTokens(op, token);
	        }
	        catch (Exception e)
	        {
	            throw new InvalidOperationException(
	                "Could not authenticate token for docs streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
	                e);
	        }

	        request.AddOperationHeader("Single-Use-Auth-Token", token);

	        var webResponse = request.RawExecuteRequest();
	        return YieldStreamResults(webResponse);
	    }

	    private static IEnumerator<RavenJObject> YieldStreamResults(WebResponse webResponse)
		{
			using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
			using (var streamReader = new StreamReader(stream))
			using (var reader = new JsonTextReader(streamReader))
			{
				if (reader.Read() == false || reader.TokenType != JsonToken.StartObject)
					throw new InvalidOperationException("Unexpected data at start of stream");

				if (reader.Read() == false || reader.TokenType != JsonToken.PropertyName || Equals("Results", reader.Value) == false)
					throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

				if (reader.Read() == false || reader.TokenType != JsonToken.StartArray)
					throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");

				while (true)
				{
					if(reader.Read() == false)
						throw new InvalidOperationException("Unexpected end of data");

					if (reader.TokenType == JsonToken.EndArray)
						break;

					yield return (RavenJObject)RavenJToken.ReadFrom(reader);
				}
			}
		}

		private QueryResult DirectQuery(string index, IndexQuery query, OperationMetadata operationMetadata, string[] includes, bool metadataOnly, bool includeEntries)
		{
			string path = query.GetIndexQueryUrl(operationMetadata.Url, index, "indexes");
			if (metadataOnly)
				path += "&metadata-only=true";
			if (includeEntries)
				path += "&debug=entries";
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, path, "GET", operationMetadata.Credentials, convention)
				{
					AvoidCachingRequest = query.DisableCaching
				}.AddOperationHeaders(OperationsHeaders))
			    .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer,
			                                    convention.FailoverBehavior,
			                                    HandleReplicationStatusChanges);

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
			var directQuery = SerializationHelper.ToQueryResult(json, request.GetEtagHeader(), request.ResponseHeaders["Temp-Request-Time"]);
			var docResults = directQuery.Results.Concat(directQuery.Includes);
			return RetryOperationBecauseOfConflict(docResults, directQuery,
			                                       () =>
			                                       DirectQuery(index, query, operationMetadata, includes, metadataOnly,
			                                                   includeEntries),
			                                       conflictedResultId =>
			                                       new ConflictException(
				                                       "Conflict detected on " +
				                                       conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.InvariantCulture)) +
				                                       ", conflict must be resolved before the document will be accessible", true)
			                                       {
				                                       ConflictedVersionIds = new[] {conflictedResultId}
			                                       });
		}

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			ExecuteWithReplication("DELETE", u => DirectDeleteIndex(name, u));
		}

		private void DirectDeleteIndex(string name, OperationMetadata operationMetadata)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + name, "DELETE", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			request.ExecuteRequest();
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
			return ExecuteWithReplication("GET", u => DirectGet(ids, u, includes, transformer, queryInputs ?? new Dictionary<string, RavenJToken>(), metadataOnly));
		}

	    /// <summary>
	    /// Perform a direct get for loading multiple ids in one request
	    /// </summary>
	    /// <param name="ids">The ids.</param>
	    /// <param name="operationMetadata">The operation URL.</param>
	    /// <param name="includes">The includes.</param>
	    /// <param name="transformer"></param>
	    /// <param name="metadataOnly"></param>
	    /// <returns></returns>
	    public MultiLoadResult DirectGet(string[] ids, OperationMetadata operationMetadata, string[] includes, string transformer, Dictionary<string, RavenJToken> queryInputs, bool metadataOnly)
		{
			var path = operationMetadata.Url + "/queries/?";
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
						new CreateHttpJsonRequestParams(this, path, "GET", metadata, operationMetadata.Credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
							.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			}
			else
			{
				request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, path, "POST", metadata, operationMetadata.Credentials, convention)
							.AddOperationHeaders(OperationsHeaders))
							.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

			return RetryOperationBecauseOfConflict(docResults, multiLoadResult, () => DirectGet(ids, operationMetadata, includes, transformer, queryInputs, metadataOnly));
		}

		private T RetryOperationBecauseOfConflict<T>(IEnumerable<RavenJObject> docResults, T currentResult, Func<T> nextTry, 
													Func<string, ConflictException> onConflictedQueryResult = null)
		{
			bool requiresRetry = docResults.Aggregate(false, (current, docResult) => 
														current | AssertNonConflictedDocumentAndCheckIfNeedToReload(docResult, onConflictedQueryResult));
			if (!requiresRetry)
				return currentResult;

			if (resolvingConflictRetries)
				throw new InvalidOperationException(
					"Encountered another conflict after already resolving a conflict. Conflict resolution cannot recurse.");
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

		private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult, Func<string, ConflictException> onConflictedQueryResult = null)
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

			if(metadata.Value<bool>(Constants.RavenReplicationConflict) && onConflictedQueryResult != null)
				throw onConflictedQueryResult(metadata.Value<string>("@id"));

			return false;
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

		private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, OperationMetadata operationMetadata)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var req = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/bulk_docs", "POST", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
			req.Write(jArray.ToString(Formatting.None, Default.Converters));

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
	    public void Commit(string txId)
		{
			ExecuteWithReplication<object>("POST", u =>
			{
				DirectCommit(txId, u);
				return null;
			});
		}

		private void DirectCommit(string txId, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/commit?tx=" + txId, "POST", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


			httpJsonRequest.ReadResponseJson();
		}

	    /// <summary>
	    /// Rollbacks the specified tx id.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    public void Rollback(string txId)
		{
			ExecuteWithReplication<object>("POST", u =>
			{
				DirectRollback(txId, u);
				return null;
			});
		}


		private void DirectRollback(string txId, OperationMetadata operationMetadata)
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/rollback?tx=" + txId, "POST", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


			httpJsonRequest.ReadResponseJson();
		}

	    /// <summary>
	    /// Prepares the transaction on the server.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    /// <param name="resourceManagerId"></param>
	    /// <param name="recoveryInformation"></param>
	    public void PrepareTransaction(string txId, Guid? resourceManagerId, byte[] recoveryInformation)
		{
			ExecuteWithReplication<object>("POST", u =>
			{
				DirectPrepareTransaction(txId, resourceManagerId, recoveryInformation, u);
				return null;
			});
		}

		public BuildNumber GetBuildNumber()
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, url + "/build/version", "GET", Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
			var result = httpJsonRequest.ReadResponseJson();

			return ((RavenJObject)result).Deserialize<BuildNumber>(convention);
		}

		private void DirectPrepareTransaction(string txId, Guid? resourceManagerId, byte[] recoveryInformation, OperationMetadata operationMetadata)
		{
			try
			{
			    var opUrl = operationMetadata.Url + "/transaction/prepare?tx=" + txId;
			    if (resourceManagerId != null)
			        opUrl += "&resourceManagerId=" + resourceManagerId;

			    var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, opUrl, "POST",
						operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
						HandleReplicationStatusChanges);

			    if (recoveryInformation != null)
			    {
			        var ms = new MemoryStream(recoveryInformation);
			        httpJsonRequest.Write(ms);
			    }

				httpJsonRequest.ReadResponseJson();
			}
			catch (WebException e)
			{
// more meaningful inner exception with TransactionAbortedException
				if (((HttpWebResponse) (e.Response)).StatusCode == HttpStatusCode.Conflict)
				{
					throw new ConcurrencyException("Failed to commit transaction - concurrency exception. See inner exception for details",e);
				}

				throw;
			}
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(url, convention, _apiKey, credentialsForSession, replicationInformerGetter, databaseName, jsonRequestFactory, currentSessionId, conflictListeners);
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
			var old = readStripingBase;
			readStripingBase = -1;// this means that will have to use the master url first
			return new DisposableAction(() => readStripingBase = old);
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
			if (databaseUrl == Url)
				return this;
			return new ServerClient(databaseUrl, convention, _apiKey, _credentials, replicationInformerGetter, database, jsonRequestFactory, currentSessionId, conflictListeners)
				   {
					   OperationsHeaders = OperationsHeaders
				   };
		}

		public IDatabaseCommands ForSystemDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			if (databaseUrl == Url)
				return this;
			return new ServerClient(databaseUrl, convention,_apiKey, _credentials, replicationInformerGetter, null, jsonRequestFactory, currentSessionId, conflictListeners)
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
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return ExecuteWithReplication<Operation>("DELETE", operationMetadata =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, "DELETE", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
				RavenJToken jsonResponse;
				try
				{
					jsonResponse = request.ReadResponseJson();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}

				// Be compitable with the resopnse from v2.0 server
				if (jsonResponse == null || jsonResponse.Type != JTokenType.Object)
					return null;

				var opId = ((RavenJObject)jsonResponse)["OperationId"];

				if (opId == null || opId.Type != JTokenType.Integer)
					return null;
				
				return new Operation(this, opId.Value<long>());
			});
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)
		{
			return UpdateByIndex(indexName, queryToUpdate, patchRequests, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
		{
			return UpdateByIndex(indexName, queryToUpdate, patch, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			var requestData = new RavenJArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "PATCH");
		}

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "EVAL");
		}

		private Operation UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)
		{
			return ExecuteWithReplication<Operation>(method, operationMetadata =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				request.Write(requestData);
				RavenJToken jsonResponse;
				try
				{
					jsonResponse = request.ReadResponseJson();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}

				return new Operation(this, jsonResponse.Value<long>("OperationId"));
			});
		}

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete)
		{
			return DeleteByIndex(indexName, queryToDelete, false);
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

			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}&popularity={6}",
													 Uri.EscapeUriString(index),
													 Uri.EscapeDataString(suggestionQuery.Term),
													 Uri.EscapeDataString(suggestionQuery.Field),
													 Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
													 Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
													 Uri.EscapeDataString(suggestionQuery.Accuracy.ToInvariantString()),
													 suggestionQuery.Popularity);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);



				var json = (RavenJObject)request.ReadResponseJson();

				return new SuggestionQueryResult
				{
					Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
				};
			});
		}

		/// <summary>
		/// Return a list of documents that based on the MoreLikeThisQuery.
		/// </summary>
		/// <param name="query">The more like this query parameters</param>
		/// <returns></returns>
		public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
		{
			var result = ExecuteGetRequest(query.GetRequestUri());
			return ((RavenJObject)result).Deserialize<MultiLoadResult>(convention);
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url + "/stats", "GET", Credentials, convention));

			var jo = (RavenJObject)httpJsonRequest.ReadResponseJson();
			return jo.Deserialize<DatabaseStatistics>(convention);
		}

		/// <summary>
		/// Generate the next identity value from the server
		/// </summary>
		public long NextIdentityFor(string name)
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/next?name=" + Uri.EscapeDataString(name), "POST", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				var readResponseJson = request.ReadResponseJson();

				return readResponseJson.Value<long>("Value");
			});
		}

		/// <summary>
		/// Seeds the next identity value on the server
		/// </summary>
		public long SeedIdentityFor(string name, long value)
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/seed?name=" + Uri.EscapeDataString(name) + "&value=" + Uri.EscapeDataString(value.ToString(CultureInfo.InvariantCulture)), "POST", operationMetadata.Credentials, convention)
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
		public JsonDocumentMetadata DirectHead(OperationMetadata operationMetadata, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			HttpJsonRequest request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, "HEAD", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


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
					throw new ConflictException("Conflict detected on " + key +
												", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict", true)
					{
						Etag = httpWebResponse.GetEtagHeader()
					};
				}
				throw;
			}
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public GetResponse[] MultiGet(GetRequest[] requests)
		{
			foreach (var getRequest in requests)
			{
				getRequest.Headers["Raven-Client-Version"] = HttpJsonRequest.ClientVersion;
			}
			return ExecuteWithReplication("GET", // this is a logical GET, physical POST
										  operationMetadata =>
										  {
											  var multiGetOperation = new MultiGetOperation(this, convention, operationMetadata.Url, requests);

											  var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.
																																					RequestUri, "POST", operationMetadata.Credentials, convention));

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

											  // 1.0 servers return result as string, not as an object, need to convert here
											  foreach (var response in responses.Where(r => r != null && r.Result != null && r.Result.Type == JTokenType.String))
											  {
												  var value = response.Result.Value<string>();
												  response.Result = string.IsNullOrEmpty(value) ?
													RavenJToken.FromObject(null) :
													RavenJObject.Parse(value);
											  }

											  multiGetOperation.TryResolveConflictOrCreateConcurrencyException(
												  responses,
												  TryResolveConflictOrCreateConcurrencyException);

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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/terms/{0}?field={1}&pageSize={2}&fromValue={3}",
													 Uri.EscapeUriString(index),
													 Uri.EscapeDataString(field),
													 pageSize.ToInvariantString(),
													 Uri.EscapeDataString(fromValue ?? ""));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJson().Values<string>();
			});
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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/facets/{0}?facetDoc={1}&{2}&facetStart={3}&facetPageSize={4}",
																Uri.EscapeUriString(index),
																Uri.EscapeDataString(facetSetupDoc),
																query.GetMinimalQueryString(),
																start,
																pageSize);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);


				var json = (RavenJObject)request.ReadResponseJson();
				return json.JsonDeserialization<FacetResults>();
			});
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
			string facetsJson = JsonConvert.SerializeObject(facets);
	        var method = facetsJson.Length > 1024 ? "POST" : "GET";
			return ExecuteWithReplication(method, operationMetadata =>
            {
				var requestUri = operationMetadata.Url + string.Format("/facets/{0}?{1}&facetStart={2}&facetPageSize={3}",
                                                                Uri.EscapeUriString(index),
                                                                query.GetMinimalQueryString(),
                                                                start,
                                                                pageSize);

				if(method == "GET")
					requestUri += "&facets=" + Uri.EscapeDataString(facetsJson);

                var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri, method, operationMetadata.Credentials, convention)
                        .AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				if (method != "GET")
					request.Write(facetsJson);

                var json = (RavenJObject)request.ReadResponseJson();
                return json.JsonDeserialization<FacetResults>();
            });
        }

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public RavenJObject Patch(string key, PatchRequest[] patches)
		{
			return Patch(key, patches, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			var batchResults = Batch(new[]
			{
				new PatchCommandData
				{
					Key = key,
					Patches = patches
				}
			});
			if (!ignoreMissing && batchResults[0].PatchResult != null && batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch)
		{
			return Patch(key, patch, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			var batchResults = Batch(new[]
				  {
					  new ScriptedPatchCommandData
					  {
						  Key = key,
						  Patch = patch
					  }
				  });
			if (!ignoreMissing && batchResults[0].PatchResult != null && batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)
		{
			var batchResults = Batch(new[]
			      	{
			      		new PatchCommandData
			      			{
			      				Key = key,
			      				Patches = patches,
			      				Etag = etag
			      			}
			      	});
			return batchResults[0].AdditionalData;
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
			var batchResults = Batch(new[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patchesToExisting,
								PatchesIfMissing = patchesToDefault,
								Metadata = defaultMetadata
							}
					});
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)
		{
			var batchResults = Batch(new[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patch,
					Etag = etag
				}
			});
			return batchResults[0].AdditionalData;
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
			var batchResults = Batch(new[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patchExisting,
					PatchIfMissing = patchDefault,
					Metadata = defaultMetadata
				}
			});
			return batchResults[0].AdditionalData;
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
				ProfilingInformation.DurationMilliseconds = (SystemTime.UtcNow - ProfilingInformation.At).TotalMilliseconds;
			}
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
				new CreateHttpJsonRequestParams(this, url + "/operation/status?id=" + id, "GET", Credentials, convention)
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

		public IDisposable Expect100Continue()
		{
			var servicePoint = ServicePointManager.FindServicePoint(new Uri(url));
			servicePoint.Expect100Continue = true;
			return new DisposableAction(() => servicePoint.Expect100Continue = false);
		}

		public string GetSingleAuthToken(OperationMetadata op)
		{
			var tokenRequest = CreateRequest(op, "GET", "/singleAuthToken", disableRequestCompression: true);

			return tokenRequest.ReadResponseJson().Value<string>("Token");
		}

		private string ValidateThatWeCanUseAuthenticateTokens(OperationMetadata op,string token)
		{
			var request = CreateRequest(op,"GET", "/singleAuthToken", disableRequestCompression: true);

			request.DisableAuthentication();
			request.webRequest.ContentLength = 0;
			request.AddOperationHeader("Single-Use-Auth-Token", token);
			var result = request.ReadResponseJson();
			return result.Value<string>("Token");
		}
	}
}
#endif
