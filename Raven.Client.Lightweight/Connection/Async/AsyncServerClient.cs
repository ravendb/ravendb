//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#if !SILVERLIGHT
using System.Transactions;
#endif
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Client.Listeners;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
using Raven.Client.Silverlight.MissingFromSilverlight;
#endif
using Raven.Imports.Newtonsoft.Json;
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
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Bson;

namespace Raven.Client.Connection.Async
{
	using System.Collections.Specialized;

	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly ProfilingInformation profilingInformation;
		private readonly IDocumentConflictListener[] conflictListeners;
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

		public string Url
		{
			get { return url; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials,
								 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId,
								 Func<string, ReplicationInformer> replicationInformerGetter, string databaseName, IDocumentConflictListener[] conflictListeners)
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
			this.conflictListeners = conflictListeners;
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
			return new AsyncServerClient(url, convention, credentialsForSession, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName, conflictListeners);
		}

		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				return operationUrl.IndexNames(start, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						return json.Select(x => x.Value<string>()).ToArray();
					});
			});
		}

		/// <summary>
		/// Gets the indexes from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var url2 = (operationUrl + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", credentials, convention));
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
						return json
							.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
							.ToArray();
					});
			});
		}

		/// <summary>
		/// Resets the specified index asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task ResetIndexAsync(string name)
		{
			return ExecuteWithReplication("RESET", operationUrl =>
			{
				var httpJsonRequestAsync = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl + "/indexes/" + name, "RESET", credentials, convention));
				httpJsonRequestAsync.AddOperationHeaders(OperationsHeaders);
				httpJsonRequestAsync.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return httpJsonRequestAsync.ReadResponseJsonAsync();
			});
		}

		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				return operationUrl.IndexDefinition(name)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
				.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = (RavenJObject)task.Result;
						//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
						return convention.CreateSerializer().Deserialize<IndexDefinition>(new RavenJTokenReader(json["Index"]));
					});
			});
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			return ExecuteWithReplication("PUT", opUrl => DirectPutIndexAsync(name, indexDef, overwrite, opUrl));
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously with url
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		/// <param name="operationUrl">The server's url</param>
		public Task<string> DirectPutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, string operationUrl)
		{
			var requestUri = operationUrl + "/indexes/" + name;
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			webRequest.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return webRequest.ExecuteRequestAsync()
				.ContinueWith(task =>
				{
					try
					{
						task.Wait();
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

					var request = jsonRequestFactory.CreateHttpJsonRequest(
						new CreateHttpJsonRequestParams(this, requestUri, "PUT", credentials, convention)
							.AddOperationHeaders(OperationsHeaders));

					var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);
					return request.WriteAsync(serializeObject)
						.ContinueWith(writeTask => request.ReadResponseJsonAsync()
													.ContinueWith(readJsonTask => { return readJsonTask.Result.Value<string>("index"); })).
						Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return ExecuteWithReplication("DELETE", operationUrl => operationUrl.Indexes(name)
																		.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
																		.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
																		.ExecuteRequestAsync());
		}

		public Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return ExecuteWithReplication("DELETE", operationUrl =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, "DELETE", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
			});
		}

		/// <summary>
		/// Deletes the document for the specified id asynchronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
#if !SILVERLIGHT
			throw new NotImplementedException();
#else
			return ExecuteWithReplication("DELETE", url =>
			{
			    return url.Docs(id)
			        .ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
			        .ExecuteRequestAsync();
			});
#endif
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
			return ExecuteWithReplication("PUT", opUrl => DirectPutAsync(opUrl, key, etag, document, metadata));
		}

		private Task<PutResult> DirectPutAsync(string opUrl, string key, Guid? etag, RavenJObject document, RavenJObject metadata)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			if (etag != null)
				metadata["ETag"] = new RavenJValue(etag.Value.ToString());

			if (key != null)
				key = Uri.EscapeUriString(key);

			var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, opUrl + "/docs/" + key, method, metadata, credentials, convention)
						.AddOperationHeaders(OperationsHeaders));


			request.AddReplicationStatusHeaders(url, opUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.WriteAsync(document.ToString())
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
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId, replicationInformerGetter, database, conflictListeners)
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
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName, conflictListeners)
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

			return ExecuteWithReplication("GET", url => DirectGetAsync(url, key));
		}

		public Task<JsonDocument> DirectGetAsync(string opUrl, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (opUrl + "/docs/" + key).NoCache(), "GET", metadata, credentials, convention)
				.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(url, opUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					try
					{
						var requestJson = task.Result;
						var docKey = request.ResponseHeaders[Constants.DocumentIdFieldName] ?? key;
						docKey = Uri.UnescapeDataString(docKey);
						request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
						var deserializeJsonDocument = SerializationHelper.DeserializeJsonDocument(docKey, requestJson,
																								  request.ResponseHeaders,
																								  request.ResponseStatusCode);
						return (Task<JsonDocument>)new CompletedTask<JsonDocument>(deserializeJsonDocument);
					}
					catch (AggregateException e)
					{
						var we = e.ExtractSingleInnerException() as WebException;
						if (we == null)
							throw;
						var httpWebResponse = we.Response as HttpWebResponse;
						if (httpWebResponse == null)
							throw;
						if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
							return new CompletedTask<JsonDocument>((JsonDocument)null);
						if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
						{
							var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
							var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));

							return TryResolveConflictOrCreateConcurrencyException(opUrl, key, conflictsDoc, httpWebResponse.GetEtagHeader())
								.ContinueWith(conflictTask =>
								{
									if (conflictTask.Result != null)
										throw conflictTask.Result;
									return DirectGetAsync(opUrl, key);
								}).Unwrap();
						}
						throw;
					}
				}).Unwrap();
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", s => DirectGetAsync(s, keys, includes, metadataOnly));
		}

		private Task<MultiLoadResult> DirectGetAsync(string opUrl, string[] keys, string[] includes, bool metadataOnly)
		{
			var path = opUrl + "/queries/?";
			if (metadataOnly)
				path += "metadata-only=true&";
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
				request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials,
																							 convention)
																 .AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, opUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => CompleteMultiGetAsync(opUrl, keys, includes, task))
					.Unwrap();
			}
			request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, "POST", credentials,
																						 convention)
															 .AddOperationHeaders(OperationsHeaders));
			return request.WriteAsync(new RavenJArray(keys).ToString(Formatting.None))
				.ContinueWith(writeTask => request.ReadResponseJsonAsync())
				.Unwrap()
				.ContinueWith(task => CompleteMultiGetAsync(opUrl, keys, includes, task))
				.Unwrap();
		}

		private Task<MultiLoadResult> CompleteMultiGetAsync(string opUrl, string[] keys, string[] includes, Task<RavenJToken> task)
		{
			try
			{
				var result = task.Result;

				var multiLoadResult = new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				};


				var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

				return RetryOperationBecauseOfConflict(opUrl, docResults, multiLoadResult, () => DirectGetAsync(opUrl, keys, includes, false));
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
		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", url =>
		{

			var requestUri = url + "/docs/?start=" + start + "&pageSize=" + pageSize;
			if (metadataOnly)
				requestUri += "&metadata-only=true";
			return jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
				.AddOperationHeaders(OperationsHeaders))
					.ReadResponseJsonAsync()
					.ContinueWith(task => ((RavenJArray)task.Result)
											.Cast<RavenJObject>()
											.ToJsonDocuments()
											.ToArray());
		});
		}

		public Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "EVAL");
		}

		private Task UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)
		{

			return ExecuteWithReplication(method, operationUrl =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, method, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				return request.ExecuteWriteAsync(requestData);
			});
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc
		/// </summary>
		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc)
		{
			return ExecuteWithReplication("GET", operationUrl =>
		{
			var requestUri = operationUrl + string.Format("/facets/{0}?facetDoc={1}&query={2}",
			Uri.EscapeUriString(index),
			Uri.EscapeDataString(facetSetupDoc),
			Uri.EscapeUriString(Uri.EscapeDataString(query.Query)));

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJObject)task.Result;
					return json.JsonDeserialization<FacetResults>();
				});
		});
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = url + "/logs";
				if (errorsOnly)
					requestUri += "?type=error";

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET",
																							 credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(task.Result)));
			});
		}

		public Task<LicensingStatus> GetLicenseStatusAsync()
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/license/status").NoCache(), "GET", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(task.Result)));
		}

		public Task<BuildNumber> GetBuildNumberAsync()
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/build/version").NoCache(), "GET", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<BuildNumber>(new RavenJTokenReader(task.Result)));
		}

		public Task<LicensingStatus> GetLicenseStatus()
		{
			var actualUrl = string.Format("{0}/license/status", url).NoCache();
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", new RavenJObject(), credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => new LicensingStatus
				{
					Error = task.Result.Value<bool>("Error"),
					Message = task.Result.Value<string>("Message"),
					Status = task.Result.Value<string>("Status"),
				});
		}

		public Task<BuildNumber> GetBuildNumber()
		{
			var actualUrl = string.Format("{0}/build/version", url).NoCache();
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", new RavenJObject(), credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => new BuildNumber
				{
					BuildVersion = task.Result.Value<string>("BuildVersion"),
					ProductVersion = task.Result.Value<string>("ProductVersion")
				});

		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/admin/backup").NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
				{
					{"BackupLocation", backupLocation},
					{"DatabaseDocument", RavenJObject.FromObject(databaseDocument)}
				}.ToString(Formatting.None))
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						return task;

					return request.ExecuteRequestAsync();
				}).Unwrap();
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string name = null)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/admin/restore").NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
				{
					{"RestoreLocation", restoreLocation},
					{"DatabaseLocation", databaseLocation}
				}.ToString(Formatting.None))
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						return task;

					return request.ExecuteRequestAsync();
				}).Unwrap();
		}

		public Task StartIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationUrl =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationUrl + "/admin/StartIndexing").NoCache(),
																							 "POST", credentials, convention));

				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task StopIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationUrl =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationUrl + "/admin/StopIndexing").NoCache(),
																							 "POST", credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task<string> GetIndexingStatusAsync()
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationUrl + "/admin/IndexingStatus").
																								NoCache(), "GET", credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => task.Result.Value<string>("IndexingStatus"));
			});
		}

		public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", operationUrl =>
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var actualUrl = string.Format("{0}/docs?startsWith={1}&start={2}&pageSize={3}", operationUrl,
										  Uri.EscapeDataString(keyPrefix), start.ToInvariantString(), pageSize.ToInvariantString());
			if (metadataOnly)
				actualUrl += "&metadata-only=true";
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl.NoCache(), "GET", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync()
					.ContinueWith(
						task =>
						SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)task.Result).OfType<RavenJObject>()).ToArray());

		});
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{
			return ExecuteWithReplication("GET", operationUrl => // logical GET even though the actual request is a POST
			{
				var multiGetOperation = new MultiGetOperation(this, convention, operationUrl, requests);

				var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.RequestUri.NoCache(), "POST", credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				httpJsonRequest.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				var requestsForServer = multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

				var postedData = JsonConvert.SerializeObject(requestsForServer);

				if (multiGetOperation.CanFullyCache(jsonRequestFactory, httpJsonRequest, postedData))
				{
					var cachedResponses = multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length], jsonRequestFactory);
					return Task.Factory.StartNew(() => cachedResponses);
				}


				return httpJsonRequest.WriteAsync(postedData)
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

			});
		}

		public Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
		{
			return UpdateByIndex(indexName, queryToUpdate, patch, false);
		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <returns></returns>
		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", url =>
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(url, index, "indexes");
			if (metadataOnly)
				path += "&metadata-only=true";
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials,
																						 convention));

			request.AddReplicationStatusHeaders(url, url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync()
						.ContinueWith(task => AttemptToProcessResponse(() => SerializationHelper.ToQueryResult((RavenJObject)task.Result, request.GetEtagHeader())));
		});
		}

		private T AttemptToProcessResponse<T>(Func<T> process) where T : class
		{
			try
			{
				return process();
			}
			catch (AggregateException e)
			{
				var webException = e.ExtractSingleInnerException() as WebException;
				if (webException == null)
					throw;

				if (HandleException(webException))
					return null;

				throw;
			}
		}

		/// <summary>
		/// Attempts to handle an exception raised when receiving a response from the server
		/// </summary>
		/// <param name="e">The exception to handle</param>
		/// <returns>returns true if the exception is handled, false if it should be thrown</returns>
		private bool HandleException(WebException e)
		{
			var httpWebResponse = e.Response as HttpWebResponse;
			if (httpWebResponse == null)
			{
				return false;
			}
			if (httpWebResponse.StatusCode == HttpStatusCode.InternalServerError)
			{
				var content = new StreamReader(httpWebResponse.GetResponseStream());
				var jo = RavenJObject.Load(new JsonTextReader(content));
				var error = jo.Deserialize<ServerRequestError>(convention);

				throw new WebException(error.Error);
			}
			return false;
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery)
		{
			if (suggestionQuery == null)
				throw new ArgumentNullException("suggestionQuery");

			return ExecuteWithReplication("GET", operationUrl =>
			{
				var requestUri = operationUrl + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}",
					Uri.EscapeUriString(index),
					Uri.EscapeDataString(suggestionQuery.Term),
					Uri.EscapeDataString(suggestionQuery.Field),
					Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
					Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
					Uri.EscapeDataString(suggestionQuery.Accuracy.ToInvariantString()));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = (RavenJObject)task.Result;
						return new SuggestionQueryResult
						{
							Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
						};
					});
			});
		}

		/// <summary>
		/// Begins the async batch operation
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		/// <returns></returns>
		public Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas)
		{
			return ExecuteWithReplication("POST", operationUrl =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var req = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl + "/bulk_docs", "POST", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				req.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
				var data = jArray.ToString(Formatting.None);

				return req.WriteAsync(data)
							.ContinueWith(writeTask =>
							{
								writeTask.Wait(); // throw
								return req.ReadResponseJsonAsync();
							})
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
#if !SILVERLIGHT
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
#endif
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
			return url.Stats()
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var jo = ((RavenJObject)task.Result);
					return jo.Deserialize<DatabaseStatistics>(convention);
				});
		}

		/// <summary>
		/// Gets the list of databases from the server asynchronously
		/// </summary>
		public Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0)
		{
			return url.Databases(pageSize, start)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJArray)task.Result;
					return json.Select(x => x.ToString())
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
			return ExecuteWithReplication("PUT", operationUrl =>
			{
				if (metadata == null)
					metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue(etag.Value.ToString());

				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationUrl, key), "PUT", metadata, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request
					.ExecuteWriteAsync(data)
					.ContinueWith(write =>
					{
						if (write.Exception != null)
							throw new InvalidOperationException("Unable to write to server");

						return request.ExecuteRequestAsync();
					}).Unwrap();
			});
		}

		/// <summary>
		/// Gets the attachment by the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<Attachment> GetAttachmentAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			return ExecuteWithReplication("GET", operationUrl =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (operationUrl + "/static/" + key).NoCache(), "GET", metadata, credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request
					.ReadResponseBytesAsync()
					.ContinueWith(task =>
					{
						switch (task.Status)
						{
							case TaskStatus.RanToCompletion:
								var memoryStream = new MemoryStream(task.Result);
								return new Attachment
								{
									Data = () => memoryStream,
									Size = task.Result.Length,
									Etag = request.GetEtagHeader(),
									Metadata = request.ResponseHeaders.FilterHeadersAttachment()
								};

							case TaskStatus.Faulted:
								var webException = task.Exception.ExtractSingleInnerException() as WebException;
								if (webException != null)
								{
									var response = webException.Response as HttpWebResponse;
									if (response != null)
									{
										switch (response.StatusCode)
										{
											case HttpStatusCode.NotFound:
												return null;

											case HttpStatusCode.Conflict:
												var conflictsDoc = RavenJObject.Load(new BsonReader(response.GetResponseStreamWithHttpDecompression()));
												var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

												throw new ConflictException("Conflict detected on " + key +
																			", conflict must be resolved before the attachment will be accessible", true)
												{
													ConflictedVersionIds = conflictIds,
													Etag = response.GetEtagHeader()
												};
										}
									}
								}
								// This will rethrow the task's exception.
								task.AssertNotFailed();
								return null;

							case TaskStatus.Canceled:
								throw new TaskCanceledException();

							default:
								throw new InvalidOperationException("Invalid task status");
						}
					});
			});
		}

		/// <summary>
		/// Deletes the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Guid? etag)
		{
			return ExecuteWithReplication("DELETE", operationUrl =>
			{
				var metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue(etag.Value.ToString());

				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationUrl, key), "DELETE", metadata, credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public static string Static(string url, string key)
		{
			return url + "/static/" + Uri.EscapeUriString(key);
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
#if !SILVERLIGHT
			throw new NotSupportedException("Only applicable in silverlight");
#else
			return ExecuteWithReplication("GET", url =>
			{
				return url
					.SilverlightEnsuresStartup()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseBytesAsync();
			});
#endif
		}

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			return ExecuteWithReplication("GET", operationUrl =>
			{
				return operationUrl.Terms(index, field, fromValue, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.AddReplicationStatusHeaders(url, operationUrl, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						return json.Select(x => x.Value<string>()).ToArray();
					});
			});
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

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public void ForceReadFromMaster()
		{
			readStripingBase = -1;// this means that will have to use the master url first
		}

		public HttpJsonRequest CreateRequest(string requestUrl, string method)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, url + requestUrl, method, metadata, credentials, convention).AddOperationHeaders(OperationsHeaders);
			return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
		}

		private void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)
		{
			if (!primaryUrl.Equals(currentUrl, StringComparison.InvariantCultureIgnoreCase))
			{
				var forceCheck = headers[Constants.RavenForcePrimaryServerCheck];
				bool shouldForceCheck;
				if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
				{
					this.replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
				}
			}
		}

		private Task ExecuteWithReplication(string method, Func<string, Task> operation)
		{
			// Convert the Func<string, Task> to a Func<string, Task<object>>
			return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t =>
			{
				t.AssertNotFailed();
				return null;
			}));
		}

		private volatile bool currentlyExecuting;
		private bool resolvingConflict;
		private bool resolvingConflictRetries;

		private Task<T> ExecuteWithReplication<T>(string method, Func<string, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

			currentlyExecuting = true;
			try
			{
				return replicationInformer.ExecuteWithReplicationAsync(method, url, currentRequest, readStripingBase, operation)
					.ContinueWith(task =>
					{
						currentlyExecuting = false;
						return task;
					}).Unwrap();
			}
			catch (Exception)
			{
				currentlyExecuting = false;
				throw;
			}
		}

		private Task<bool> AssertNonConflictedDocumentAndCheckIfNeedToReload(string opUrl, RavenJObject docResult)
		{
			if (docResult == null)
				return new CompletedTask<bool>(false);
			var metadata = docResult[Constants.Metadata];
			if (metadata == null)
				return new CompletedTask<bool>(false);

			if (metadata.Value<int>("@Http-Status-Code") == 409)
			{
				return TryResolveConflictOrCreateConcurrencyException(opUrl, metadata.Value<string>("@id"), docResult,
															   HttpExtensions.EtagHeaderToGuid(metadata.Value<string>("@etag")))
					.ContinueWith(task =>
					{
						if (task.Result == null)
							return true;
						throw task.Result;
					});

			}
			return new CompletedTask<bool>(false);
		}

		private Task<ConflictException> TryResolveConflictOrCreateConcurrencyException(string opUrl, string key, RavenJObject conflictsDoc, Guid etag)
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
					return DirectGetAsync(opUrl, conflictIds, null, false)
						.ContinueWith(task =>
						{
							var results = task.Result.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

							foreach (var conflictListener in conflictListeners)
							{
								JsonDocument resolvedDocument;
								if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
								{
									return DirectPutAsync(opUrl, key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata)
										.ContinueWith(_ =>
										{
											_.AssertNotFailed();
											return (ConflictException)null;
										});

								}
							}
							return new CompletedTask<ConflictException>(new ConflictException("Conflict detected on " + key +
																			  ", conflict must be resolved before the document will be accessible",
																			  true)
							{
								ConflictedVersionIds = conflictIds,
								Etag = etag
							});
						}).Unwrap();

				}
				finally
				{
					resolvingConflict = false;
				}
			}

			return new CompletedTask<ConflictException>(new ConflictException("Conflict detected on " + key +
																			  ", conflict must be resolved before the document will be accessible",
																			  true)
			{
				ConflictedVersionIds = conflictIds,
				Etag = etag
			});
		}

		private Task<T> RetryOperationBecauseOfConflict<T>(string opUrl, IEnumerable<RavenJObject> docResults, T currentResult, Func<Task<T>> nextTry)
		{
			return RetryOperationBecauseOfConflictLoop(opUrl, docResults.GetEnumerator(), false, currentResult, nextTry);
		}

		private Task<T> RetryOperationBecauseOfConflictLoop<T>(string opUrl, IEnumerator<RavenJObject> enumerator, bool requiresRetry, T currentResult, Func<Task<T>> nextTry)
		{
			if (enumerator.MoveNext() == false)
				return RetryOperationBecauseOfConflictContinuation<T>(requiresRetry, currentResult, nextTry);

			return AssertNonConflictedDocumentAndCheckIfNeedToReload(opUrl, enumerator.Current)
				.ContinueWith(task =>
				{
					requiresRetry |= task.Result;

					return RetryOperationBecauseOfConflictLoop<T>(opUrl, enumerator, requiresRetry, currentResult, nextTry);
				}).Unwrap();
		}

		private Task<T> RetryOperationBecauseOfConflictContinuation<T>(bool requiresRetry, T currentResult, Func<Task<T>> nextTry)
		{
			if (!requiresRetry)
				return new CompletedTask<T>(currentResult);

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
	}
}
