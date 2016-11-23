//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#if SILVERLIGHT || NETFX_CORE
using Raven.Abstractions.Replication;
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using System.Transactions;
#endif
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Listeners;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using System.Collections.Specialized;

namespace Raven.Client.Connection.Async
{
	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands, IAsyncAdminDatabaseCommands, IAsyncInfoDatabaseCommands, IAsyncGlobalAdminDatabaseCommands
	{
		private readonly ProfilingInformation profilingInformation;
		private readonly IDocumentConflictListener[] conflictListeners;
		private readonly string _url;
		private readonly string rootUrl;
		private readonly OperationCredentials credentials;
		private readonly DocumentConvention convention;
		private IDictionary<string, string> operationsHeaders = new Dictionary<string, string>();
		internal readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? sessionId;
		private readonly Func<string, ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private int requestCount;
		private int readStripingBase;

		private readonly ICredentials _credentials;
		private readonly string _apiKey;

		public string Url
		{
			get { return _url; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		public AsyncServerClient(string url, DocumentConvention convention, string apiKey, ICredentials credentials,
								 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId,
								 Func<string, ReplicationInformer> replicationInformerGetter, string databaseName, IDocumentConflictListener[] conflictListeners)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			_url = url;
			if (_url.EndsWith("/"))
				_url = _url.Substring(0, _url.Length - 1);
			rootUrl = _url;
			var databasesIndex = rootUrl.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
			if (databasesIndex > 0)
			{
				rootUrl = rootUrl.Substring(0, databasesIndex);
			}
			this.jsonRequestFactory = jsonRequestFactory;
			this.sessionId = sessionId;
			this.convention = convention;
			this.credentials = new OperationCredentials(apiKey, credentials);
			_apiKey = apiKey;
			_credentials = credentials;
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
			return new AsyncServerClient(Url, convention, _apiKey, credentialsForSession, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName, conflictListeners);
		}

		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				return operationMetadata.Url.IndexNames(start, pageSize)
					.NoCache()
					.ToJsonRequest(this, operationMetadata.Credentials, convention)
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						return json.Select(x => x.Value<string>()).ToArray();
					});
			});
		}

		public Task<RavenJObject> GetDatabaseConfigurationAsync()
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/debug/config").NoCache();
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET",
						operationMetadata.Credentials, convention));
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
					HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync().ContinueWith(x => (RavenJObject) x.Result);
			});
		}

		/// <summary>
		/// Gets the indexes from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", operationMetadata.Credentials, convention));
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
		/// Gets the transformers from the server asynchronously
		/// </summary>
		public Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var url2 = (operationMetadata.Url + "/transformers?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url2, "GET", operationMetadata.Credentials, convention));
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						//NOTE: To review, I'm not confidence this is the correct way to deserialize the transformer definition
						return json
							.Select(x => JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
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
			return ExecuteWithReplication("RESET", operationMetadata =>
			{
				var httpJsonRequestAsync = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + name, "RESET", operationMetadata.Credentials, convention));
				httpJsonRequestAsync.AddOperationHeaders(OperationsHeaders);
				httpJsonRequestAsync.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return httpJsonRequestAsync.ReadResponseJsonAsync();
			});
		}

		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				return operationMetadata.Url.IndexDefinition(name)
				.NoCache()
				.ToJsonRequest(this, operationMetadata.Credentials, convention)
				.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
				.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						try
						{
							var indexDefinitionJson = (RavenJObject)task.Result;
							var value = indexDefinitionJson.Value<RavenJObject>("Index");
							return convention.CreateSerializer().Deserialize<IndexDefinition>(new RavenJTokenReader(value));
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
								return null;

							throw;
						}
					});
			});
		}

		/// <summary>
		/// Gets the transformer definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<TransformerDefinition> GetTransformerAsync(string name)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				return operationMetadata.Url.Transformer(name)
				.NoCache()
				.ToJsonRequest(this, operationMetadata.Credentials, convention)
				.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
				.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						try
						{
							var transformerDefinitionJson = (RavenJObject)task.Result;
							var value = transformerDefinitionJson.Value<RavenJObject>("Transformer");
							return convention.CreateSerializer().Deserialize<TransformerDefinition>(new RavenJTokenReader(value));
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
								return null;

							throw;
						}
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
			return ExecuteWithReplication("PUT", u => DirectPutIndexAsync(name, indexDef, overwrite, u));
		}

		/// <summary>
		/// Puts the transformer definition for the specified name asynchronously
		/// </summary>
		public Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition)
		{
			return ExecuteWithReplication("PUT", u => DirectPutTransformerAsync(name, transformerDefinition, u));
		}

		/// <summary>
		/// Puts the index definition for the specified name asynchronously with url
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		/// <param name="operationMetadata">The server's url</param>
		public Task<string> DirectPutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, OperationMetadata operationMetadata)
		{
			var requestUri = operationMetadata.Url + "/indexes/" + Uri.EscapeUriString(name) + "?definition=yes";
			var webRequest = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			webRequest.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
						new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention)
							.AddOperationHeaders(OperationsHeaders));

					var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);
					return request.WriteAsync(serializeObject)
						.ContinueWith(writeTask => request.ReadResponseJsonAsync()
													.ContinueWith(readJsonTask =>
													{
														try
														{
															return readJsonTask.Result.Value<string>("Index");
														}
														catch (AggregateException e)
														{
															var we = e.ExtractSingleInnerException() as WebException;
															if (we == null)
																throw;

															var response = we.Response as HttpWebResponse;

															if (response.StatusCode == HttpStatusCode.BadRequest)
															{
																var error = we.TryReadErrorResponseObject(
																	new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" });

																if (error == null)
																{
																	throw;
																}

																var compilationException = new IndexCompilationException(error.Message)
																{
																	IndexDefinitionProperty = error.IndexDefinitionProperty,
																	ProblematicText = error.ProblematicText
																};

																throw compilationException;
															}

															throw;
														}
													})).
						Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Puts the transfromer definition for the specified name asynchronously with url
		/// </summary>
		public Task<string> DirectPutTransformerAsync(string name, TransformerDefinition transformerDefinition, OperationMetadata operationMetadata)
		{
			var requestUri = operationMetadata.Url + "/transformers/" + name;

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			var serializeObject = JsonConvert.SerializeObject(transformerDefinition, Default.Converters);
			return request.WriteAsync(serializeObject)
				.ContinueWith(writeTask => request.ReadResponseJsonAsync()
											.ContinueWith(readJsonTask =>
											{
												try
												{
													return readJsonTask.Result.Value<string>("Transformer");
												}
												catch (AggregateException e)
												{
													var we = e.ExtractSingleInnerException() as WebException;
													if (we == null)
														throw;

													var response = we.Response as HttpWebResponse;

													if (response.StatusCode == HttpStatusCode.BadRequest)
													{
														var error = we.TryReadErrorResponseObject(
															new { Error = "", Message = "" });

														if (error == null)
														{
															throw;
														}

														var compilationException = new TransformCompilationException(error.Message);

														throw compilationException;
													}

													throw;
												}

											}))
				.Unwrap();
		}

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return ExecuteWithReplication("DELETE", operationMetadata => operationMetadata.Url.Indexes(name)
																		.ToJsonRequest(this, operationMetadata.Credentials, convention, OperationsHeaders, "DELETE")
																		.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
																		.ExecuteRequestAsync());
		}

		public async Task<Operation> DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			RavenJToken op = await ExecuteWithReplication("DELETE", operationMetadata =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, "DELETE", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

			return new Operation(this, op.Value<long>("OperationId"));
		}

		public Task DeleteTransformerAsync(string name)
		{
			return ExecuteWithReplication("DELETE", operationMetadata => operationMetadata.Url.Transformer(name)
																		.ToJsonRequest(this, operationMetadata.Credentials, convention, OperationsHeaders, "DELETE")
																		.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
																		.ExecuteRequestAsync());
		}
		
		/// <summary>
		/// Deletes the document for the specified id asynchronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
			return ExecuteWithReplication("DELETE", operationMetadata =>
			{
				return operationMetadata.Url.Document(id)
			        .ToJsonRequest(this, operationMetadata.Credentials, convention, OperationsHeaders, "DELETE")
			        .ExecuteRequestAsync();
			});
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patches,
							}
					}).ConfigureAwait(false);
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
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, Etag etag)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patches,
								Etag = etag
							}
					}).ConfigureAwait(false);
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata)
		{
			var batchResults = await BatchAsync(new ICommandData[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patchesToExisting,
								PatchesIfMissing = patchesToDefault,
								Metadata = defaultMetadata
							}
					}).ConfigureAwait(false);
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patch,
				}
			}).ConfigureAwait(false);
			if (!ignoreMissing && batchResults[0].PatchResult != null && batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, Etag etag)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patch,
					Etag = etag
				}
			}).ConfigureAwait(false);
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata)
		{
			var batchResults = await BatchAsync(new ICommandData[]
			{
				new ScriptedPatchCommandData
				{
					Key = key,
					Patch = patchExisting,
					PatchIfMissing = patchDefault,
					Metadata = defaultMetadata
				}
			}).ConfigureAwait(false);
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		public Task<PutResult> PutAsync(string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			return ExecuteWithReplication("PUT", u => DirectPutAsync(u, key, etag, document, metadata));
		}

		private Task<PutResult> DirectPutAsync(OperationMetadata operationMetadata, string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			if (metadata == null)
				metadata = new RavenJObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			if (etag != null)
				metadata["ETag"] = new RavenJValue((string)etag);

			if (key != null)
				key = Uri.EscapeDataString(key);

			var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, method, metadata, operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));


			request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
        /// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IAsyncDatabaseCommands ForDatabase(string database)
		{
            if (database == Constants.SystemDatabase)
                return ForSystemDatabase();

			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);
			databaseUrl = databaseUrl + "/databases/" + database + "/";
			if (databaseUrl == Url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, _apiKey, _credentials, jsonRequestFactory, sessionId, replicationInformerGetter, database, conflictListeners)
			{
				operationsHeaders = operationsHeaders
			};
		}

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands ForSystemDatabase()
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);
			if (databaseUrl == Url)
				return this;
			return new AsyncServerClient(databaseUrl, convention, _apiKey, _credentials, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName, conflictListeners)
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

			return ExecuteWithReplication("GET", operationMetadata => DirectGetAsync(operationMetadata, key));
		}


		public async Task<JsonDocument> DirectGetAsync(OperationMetadata operationMetadata, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (operationMetadata.Url + "/docs/" + Uri.EscapeDataString(key)).NoCache(), "GET", metadata, operationMetadata.Credentials, convention)
				.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			HttpWebResponse httpWebResponse;
		    try
		    {
		        var requestJson = await request.ReadResponseJsonAsync();
		        var docKey = request.ResponseHeaders[Constants.DocumentIdFieldName] ?? key;
		        docKey = Uri.UnescapeDataString(docKey);
		        request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
		        var deserializeJsonDocument = SerializationHelper.DeserializeJsonDocument(docKey, requestJson,
		            request.ResponseHeaders,
		            request.ResponseStatusCode);
		        return deserializeJsonDocument;
		    }
		    catch (WebException we)
		    {
		        httpWebResponse = we.Response as HttpWebResponse;
		        if (httpWebResponse == null)
		            throw;
		        if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
		            return null;
		        if (httpWebResponse.StatusCode != HttpStatusCode.Conflict)
		            throw;
		    }

			var conflicts = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression());
			var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));

			var conflictException = await TryResolveConflictOrCreateConcurrencyException(operationMetadata, key, conflictsDoc, httpWebResponse.GetEtagHeader());
			if (conflictException != null)
				throw conflictException;

			return await DirectGetAsync(operationMetadata, key);
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, string transformer = null, Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false)
		{
			return ExecuteWithReplication("GET", u => DirectGetAsync(u, keys, includes, transformer, queryInputs, metadataOnly));
		}

		private Task<MultiLoadResult> DirectGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer, Dictionary<string, RavenJToken> queryInputs, bool metadataOnly)
		{
			var path = operationMetadata.Url + "/queries/?";
			if (metadataOnly)
				path += "metadata-only=true&";
			if (includes != null && includes.Length > 0)
			{
				path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			if (string.IsNullOrEmpty(transformer) == false)
				path += "&transformer=" + transformer;

			if (queryInputs != null)
			{
				path = queryInputs.Aggregate(path, (current, queryInput) => current + ("&" + string.Format("qp-{0}={1}", queryInput.Key, queryInput.Value)));
			}

			var uniqueIds = new HashSet<string>(keys);
			HttpJsonRequest request;
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load > 128 items are going to be rare
			if (uniqueIds.Sum(x => x.Length) < 1024)
			{
				path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
				request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", operationMetadata.Credentials,
																							 convention)
																 .AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => CompleteMultiGetAsync(operationMetadata, keys, includes, task))
					.Unwrap();
			}
			request =
				jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, "POST", operationMetadata.Credentials,
																						 convention)
															 .AddOperationHeaders(OperationsHeaders));
			return request.WriteAsync(new RavenJArray(uniqueIds).ToString(Formatting.None))
				.ContinueWith(writeTask => request.ReadResponseJsonAsync())
				.Unwrap()
				.ContinueWith(task => CompleteMultiGetAsync(operationMetadata, keys, includes, task))
				.Unwrap();
		}

		private Task<MultiLoadResult> CompleteMultiGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, Task<RavenJToken> task)
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

				return RetryOperationBecauseOfConflict(operationMetadata, docResults, multiLoadResult, () => DirectGetAsync(operationMetadata, keys, includes, null, null, false));
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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + "/docs/?start=" + start + "&pageSize=" + pageSize;
				if (metadataOnly)
					requestUri += "&metadata-only=true";
				return jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
						.ReadResponseJsonAsync()
						.ContinueWith(task => ((RavenJArray)task.Result)
												.Cast<RavenJObject>()
												.ToJsonDocuments()
												.ToArray());
			});
		}

		public Task<Operation> UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
			return UpdateByIndexImpl(indexName, queryToUpdate, allowStale, requestData, "EVAL");
		}

		private async Task<Operation> UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, bool allowStale, String requestData, String method)
		{
			RavenJToken reponse = await ExecuteWithReplication(method, operationMetadata =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + allowStale;

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				return request.ExecuteWriteAsync(requestData);
			});
			return new Operation(this, reponse.Value<long>("OperationId"));
		}

		/// <summary>
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/facets/{0}?facetDoc={1}&query={2}&facetStart={3}&facetPageSize={4}",
				Uri.EscapeUriString(index),
				Uri.EscapeDataString(facetSetupDoc),
				Uri.EscapeDataString(query.Query),
				start,
				pageSize);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = (RavenJObject)task.Result;
						return json.JsonDeserialization<FacetResults>();
					});
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
		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start = 0, int? pageSize = null)
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

				if (method == "GET")
					requestUri += "&facets=" + Uri.EscapeDataString(facetsJson);

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), method, operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders))
						.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				if (method != "GET")
					request.WriteAsync(facetsJson).Wait();

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
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + "/logs";
				if (errorsOnly)
					requestUri += "?type=error";

				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET",
																							 operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(task.Result)));
			});
		}

		public Task<LicensingStatus> GetLicenseStatusAsync()
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/license/status").NoCache(), "GET", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(task.Result)));
		}

		public async Task<LicensingStatus> GetLicenseStatus()
		{
			var actualUrl = string.Format("{0}/license/status", Url).NoCache();
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", new RavenJObject(), credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

            var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
			return new LicensingStatus
			{
				Error = result.Value<bool>("Error"),
				Message = result.Value<string>("Message"),
				Status = result.Value<string>("Status"),
			};
		}

		public async Task<BuildNumber> GetBuildNumberAsync()
		{
			var actualUrl = string.Format("{0}/build/version", Url).NoCache();
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl, "GET", new RavenJObject(), credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

            RavenJToken result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
			return new BuildNumber
			{
				BuildVersion = result.Value<string>("BuildVersion"),
				ProductVersion = result.Value<string>("ProductVersion")
			};
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/admin/backup").NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
			                                 {
				                                 {"BackupLocation", backupLocation},
				                                 {"DatabaseDocument", RavenJObject.FromObject(databaseDocument)}
			                                 }.ToString(Formatting.None));
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string name = null, bool defrag = false)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/admin/restore?defrag=" + defrag).NoCache(), "POST", credentials, convention));
			request.AddOperationHeaders(OperationsHeaders);
			return request.ExecuteWriteAsync(new RavenJObject
			{
				{"RestoreLocation", restoreLocation},
				{"DatabaseLocation", databaseLocation},
				{"DatabaseName", name}
			}.ToString(Formatting.None));
		}

		public Task StartIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationMetadata.Url + "/admin/StartIndexing").NoCache(),
																							 "POST", operationMetadata.Credentials, convention));

				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task StopIndexingAsync()
		{
			return ExecuteWithReplication("POST", operationMetadata =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationMetadata.Url + "/admin/StopIndexing").NoCache(),
																							 "POST", operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteRequestAsync();
			});
		}

		public Task<string> GetIndexingStatusAsync()
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var request =
					jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
																							 (operationMetadata.Url + "/admin/IndexingStatus").
																								NoCache(), "GET", operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => task.Result.Value<string>("IndexingStatus"));
			});
		}

		public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize, bool metadataOnly = false, string exclude = null)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var actualUrl = string.Format("{0}/docs?startsWith={1}&exclude={4}&start={2}&pageSize={3}", operationMetadata.Url,
										  Uri.EscapeDataString(keyPrefix), start.ToInvariantString(), pageSize.ToInvariantString(), exclude);
			if (metadataOnly)
				actualUrl += "&metadata-only=true";
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, actualUrl.NoCache(), "GET", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
			return ExecuteWithReplication("GET", operationMetadata => // logical GET even though the actual request is a POST
			{
				var multiGetOperation = new MultiGetOperation(this, convention, operationMetadata.Url, requests);

				var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.RequestUri.NoCache(), "POST", operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				httpJsonRequest.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

		public Task<Operation> UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
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
			return ExecuteWithReplication("GET", operationMetadata =>
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(operationMetadata.Url, index, "indexes");
			if (metadataOnly)
				path += "&metadata-only=true";
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}

			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", operationMetadata.Credentials, convention)
				{
					AvoidCachingRequest = query.DisableCaching
				}.AddOperationHeaders(OperationsHeaders));

			request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync()
						.ContinueWith(task => AttemptToProcessResponse(() => SerializationHelper.ToQueryResult((RavenJObject)task.Result, request.GetEtagHeader(), request.ResponseHeaders["Temp-Request-Time"])));
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

			return ExecuteWithReplication("GET", operationMetadata =>
			{
				var requestUri = operationMetadata.Url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}",
					Uri.EscapeUriString(index),
					Uri.EscapeDataString(suggestionQuery.Term),
					Uri.EscapeDataString(suggestionQuery.Field),
					Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
					Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
					Uri.EscapeDataString(suggestionQuery.Accuracy.ToInvariantString()));

				var request = jsonRequestFactory.CreateHttpJsonRequest(
					new CreateHttpJsonRequestParams(this, requestUri.NoCache(), "GET", operationMetadata.Credentials, convention)
						.AddOperationHeaders(OperationsHeaders));

				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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
			return ExecuteWithReplication("POST", async operationMetadata =>
			{
				var metadata = new RavenJObject();
				AddTransactionInformation(metadata);
				var req = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/bulk_docs", "POST", metadata, operationMetadata.Credentials, convention)
					.AddOperationHeaders(OperationsHeaders));

				req.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
				var data = jArray.ToString(Formatting.None);

				await req.WriteAsync(data);
				RavenJArray response;
				try
				{
					response = (RavenJArray) (await req.ReadResponseJsonAsync());
				}
				catch (WebException we)
				{
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

		private void AddTransactionInformation(RavenJObject metadata)
		{
#if !SILVERLIGHT && !NETFX_CORE
			if (convention.EnlistInDistributedTransactions == false)
				return;

			var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			if (transactionInformation == null)
				return;

			string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
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
			return Url.Stats()
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
			return Url.Databases(pageSize, start)
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
		public Task PutAttachmentAsync(string key, Etag etag, byte[] data, RavenJObject metadata)
		{
			return ExecuteWithReplication("PUT", operationMetadata =>
			{
				if (metadata == null)
					metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue((string)etag);

				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "PUT", metadata, operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

				return request.ExecuteWriteAsync(data);
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

			return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "GET"));
		}

        public Task<Attachment> HeadAttachmentAsync(string key)
        {
            EnsureIsNotNullOrEmpty(key, "key");

            return ExecuteWithReplication("HEAD", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "HEAD"));
        }

	    private Task<Attachment> DirectGetAttachmentAsync(string key, OperationMetadata operationMetadata, string method)
	    {
	        var metadata = new RavenJObject();
	        AddTransactionInformation(metadata);
	        var request =
	            jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this,
	                                                                                     (operationMetadata.Url + "/static/" +
	                                                                                      key).NoCache(), method, metadata,
	                                                                                     operationMetadata.Credentials,
	                                                                                     convention)
	                                                         .AddOperationHeaders(OperationsHeaders));

	        request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior,
	                                            HandleReplicationStatusChanges);

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
	                            Data = () =>
	                            {
	                                if (method != "GET")
	                                    throw new InvalidOperationException( "Cannot get attachment data from an attachment loaded using HEAD request.");
                                    return memoryStream;
	                            },
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
	                                        var conflictsDoc =
	                                            RavenJObject.Load(
	                                                new BsonReader(response.GetResponseStreamWithHttpDecompression()));
	                                        var conflictIds =
	                                            conflictsDoc.Value<RavenJArray>("Conflicts")
	                                                        .Select(x => x.Value<string>())
	                                                        .ToArray();

	                                        throw new ConflictException("Conflict detected on " + key +
	                                                                    ", conflict must be resolved before the attachment will be accessible",
	                                                                    true)
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
	    }

	    /// <summary>
		/// Deletes the attachment with the specified key asynchronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Etag etag)
		{
			return ExecuteWithReplication("DELETE", operationMetadata =>
			{
				var metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue((string)etag);

				var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "DELETE", metadata, operationMetadata.Credentials, convention));
				request.AddOperationHeaders(OperationsHeaders);
				request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

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

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			return ExecuteWithReplication("GET", operationMetadata =>
			{
				return operationMetadata.Url.Terms(index, field, fromValue, pageSize)
					.NoCache()
					.ToJsonRequest(this, operationMetadata.Credentials, convention)
					.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges)
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

		public Task<JsonDocumentMetadata> HeadAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");
			return ExecuteWithReplication("HEAD", u => DirectHeadAsync(u, key));
		}

#if NETFX_CORE
		//TODO: Mono implement 
		public Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo)
		{
			throw new NotImplementedException();
		}

		public Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0,
		                            int pageSize = Int32.MaxValue)
		{
			throw new NotImplementedException();
		}

#else
		public async Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			string path = query.GetIndexQueryUrl(Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false);
			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, path.NoCache(), "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
											.AddReplicationStatusHeaders(Url, Url, replicationInformer,
																		 convention.FailoverBehavior,
																		 HandleReplicationStatusChanges);

			request.RemoveAuthorizationHeader();

            var token = await GetSingleAuthToken().ConfigureAwait(false);

			try
			{
                token = await ValidateThatWeCanUseAuthenticateTokens(token).ConfigureAwait(false);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
					e);
			}

			request.AddOperationHeader("Single-Use-Auth-Token", token);

            var webResponse = await request.RawExecuteRequestAsync().ConfigureAwait(false);
			queryHeaderInfo.Value = new QueryHeaderInformation
			{
				Index = webResponse.Headers["Raven-Index"],
				IndexTimestamp = DateTime.ParseExact(webResponse.Headers["Raven-Index-Timestamp"], Default.DateTimeFormatsToRead,
										CultureInfo.InvariantCulture, DateTimeStyles.None),
				IndexEtag = Etag.Parse(webResponse.Headers["Raven-Index-Etag"]),
				ResultEtag = Etag.Parse(webResponse.Headers["Raven-Result-Etag"]),
				IsStable = bool.Parse(webResponse.Headers["Raven-Is-Stale"]),
				TotalResults = int.Parse(webResponse.Headers["Raven-Total-Results"])
			};

			return new YieldStreamResults(webResponse);
		}

		public class YieldStreamResults : IAsyncEnumerator<RavenJObject>
		{
			private readonly WebResponse webResponse;
			private readonly Stream stream;
			private readonly StreamReader streamReader;
			private readonly JsonTextReaderAsync reader;
			private bool complete;

			private bool wasInitalized;

			public YieldStreamResults(WebResponse webResponse)
			{
				this.webResponse = webResponse;
				stream = webResponse.GetResponseStreamWithHttpDecompression();
				streamReader = new StreamReader(stream);
				reader = new JsonTextReaderAsync(streamReader);

			}

			private async Task InitAsync()
			{
                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartObject)
					throw new InvalidOperationException("Unexpected data at start of stream");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.PropertyName || Equals("Results", reader.Value) == false)
					throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartArray)
					throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
			}

			public void Dispose()
			{
				reader.Close();
				streamReader.Close();
				stream.Close();
				webResponse.Close();
			}

			public async Task<bool> MoveNextAsync()
			{
				if (complete)
				{
					// to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
					// also return false, rather than throwing
					return false;
				}

				if (wasInitalized == false)
				{
					await InitAsync();
					wasInitalized = true;
				}

                if (await reader.ReadAsync().ConfigureAwait(false) == false)
					throw new InvalidOperationException("Unexpected end of data");

				if (reader.TokenType == JsonToken.EndArray)
				{
					complete = true;
					return false;
				}

                Current = (RavenJObject)await RavenJToken.ReadFromAsync(reader).ConfigureAwait(false);
				return true;
			}

			public RavenJObject Current { get; private set; }
		}


		public async Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0,
									int pageSize = Int32.MaxValue)
		{
			if (fromEtag != null && startsWith != null)
				throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

			var sb = new StringBuilder(Url).Append("/streams/docs?");

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
			}
			if (start != 0)
				sb.Append("start=").Append(start).Append("&");
			if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");


			var request = jsonRequestFactory.CreateHttpJsonRequest(
				new CreateHttpJsonRequestParams(this, sb.ToString().NoCache(), "GET", credentials, convention)
					.AddOperationHeaders(OperationsHeaders))
				.AddReplicationStatusHeaders(Url, Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
			request.RemoveAuthorizationHeader();

            var token = await GetSingleAuthToken().ConfigureAwait(false);

			try
			{
                token = await ValidateThatWeCanUseAuthenticateTokens(token).ConfigureAwait(false);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not authenticate token for docs streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
					e);
			}

			request.AddOperationHeader("Single-Use-Auth-Token", token);


            var webResponse = await request.RawExecuteRequestAsync().ConfigureAwait(false);
			return new YieldStreamResults(webResponse);
		}
#endif
#if SILVERLIGHT
		/// <summary>
		/// Get the low level  bulk insert operation
		/// </summary>
		public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
		{
			return new RemoteBulkInsertOperation(options, this, changes);
		}
#endif

		/// <summary>
		/// Do a direct HEAD request against the server for the specified document
		/// </summary>
		private Task<JsonDocumentMetadata> DirectHeadAsync(OperationMetadata operationMetadata, string key)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			HttpJsonRequest request = jsonRequestFactory.CreateHttpJsonRequest(
																			   new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, "HEAD", operationMetadata.Credentials, convention)
																				   .AddOperationHeaders(OperationsHeaders))
														.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

			return request.ReadResponseJsonAsync().ContinueWith(task =>
			{
				try
				{
                    task.AssertNotFailed();
					var deserializeJsonDocumentMetadata = SerializationHelper.DeserializeJsonDocumentMetadata(key, request.ResponseHeaders, request.ResponseStatusCode);
					return (Task<JsonDocumentMetadata>)new CompletedTask<JsonDocumentMetadata>(deserializeJsonDocumentMetadata);
				}
				catch (AggregateException e)
				{
					var webException = e.ExtractSingleInnerException() as WebException;
					if (webException == null)
						throw;
					var httpWebResponse = webException.Response as HttpWebResponse;
					if (httpWebResponse == null)
						throw;
					if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						return new CompletedTask<JsonDocumentMetadata>((JsonDocumentMetadata)null);
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
			}).Unwrap();
		}

		public HttpJsonRequest CreateRequest(string requestUrl, string method, bool disableRequestCompression = false)
		{
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, Url + requestUrl, method, metadata, credentials, convention).AddOperationHeaders(OperationsHeaders);
			createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
			return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
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


		private volatile bool currentlyExecuting;
		private bool resolvingConflict;
		private bool resolvingConflictRetries;

		private async Task<T> ExecuteWithReplication<T>(string method, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

			currentlyExecuting = true;
			try
			{
				return await replicationInformer.ExecuteWithReplicationAsync(method, Url, credentials, currentRequest, readStripingBase, operation);
			}
			finally
			{
				currentlyExecuting = false;
			}
		}

		private async Task<bool> AssertNonConflictedDocumentAndCheckIfNeedToReload(OperationMetadata operationMetadata, RavenJObject docResult)
		{
			if (docResult == null)
				return (false);
			var metadata = docResult[Constants.Metadata];
			if (metadata == null)
				return (false);

			if (metadata.Value<int>("@Http-Status-Code") == 409)
			{
				var etag = HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag"));
                var e = await TryResolveConflictOrCreateConcurrencyException(operationMetadata, metadata.Value<string>("@id"), docResult, etag).ConfigureAwait(false);
				if (e != null)
					throw e;
				return true;

			}
			return (false);
		}

		private Task<ConflictException> TryResolveConflictOrCreateConcurrencyException(OperationMetadata operationMetadata, string key, RavenJObject conflictsDoc, Etag etag)
		{
			var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
			if (ravenJArray == null)
				throw new InvalidOperationException("Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

			var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();

			return TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, operationMetadata)
				.ContinueWith(t =>
				{
					if (t.Result)
					{
						return (ConflictException)null;
					}

					return new ConflictException("Conflict detected on " + key +
												 ", conflict must be resolved before the document will be accessible",
												 true)
					{
						ConflictedVersionIds = conflictIds,
						Etag = etag
					};
				});
		}

		internal Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string key, Etag etag, string[] conflictIds, OperationMetadata operationMetadata = null)
		{
			if (operationMetadata == null)
				operationMetadata = new OperationMetadata(Url);

			if (conflictListeners.Length > 0 && resolvingConflict == false)
			{
				resolvingConflict = true;
				try
				{
					return DirectGetAsync(operationMetadata, conflictIds, null, null, null, false)
						.ContinueWith(task =>
						{
							var results = task.Result.Results.Select(SerializationHelper.ToJsonDocument).ToArray();
                            if (results.Any(x => x == null))
                            {
                                // one of the conflict documents doesn't exist, means that it was already resolved.
                                // we'll reload the relevant documents again
                                return new CompletedTask<bool>(true);
                            }

							foreach (var conflictListener in conflictListeners)
							{
								JsonDocument resolvedDocument;
								if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
								{
                                    return DirectPutAsync(operationMetadata, key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata)
                                        .ContinueWith(_ =>
                                        {
                                            try
                                            {
                                                _.AssertNotFailed();
                                            }
                                            catch (AggregateException e)
                                            {
                                                var inner = e.ExtractSingleInnerException();
                                                if (inner is ConcurrencyException == false)
                                                    throw;

                                                // we are racing the changes API here, so that is fine
                                            }

                                            return true;
                                        });
								}
							}

							return new CompletedTask<bool>(false);
						}).Unwrap();

				}
				finally
				{
					resolvingConflict = false;
				}
			}

			return new CompletedTask<bool>(false);
		}

		private async Task<T> RetryOperationBecauseOfConflict<T>(OperationMetadata operationMetadata, IEnumerable<RavenJObject> docResults, T currentResult, Func<Task<T>> nextTry)
		{
			bool requiresRetry = false;
			foreach (var docResult in docResults)
			{
                requiresRetry |= await AssertNonConflictedDocumentAndCheckIfNeedToReload(operationMetadata, docResult).ConfigureAwait(false);
			}
			if (!requiresRetry)
				return currentResult;

			if (resolvingConflictRetries)
				throw new InvalidOperationException(
					"Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
			resolvingConflictRetries = true;
			try
			{
                return await nextTry().ConfigureAwait(false);
			}
			finally
			{
				resolvingConflictRetries = false;
			}
		}

		public Task<RavenJToken> GetOperationStatusAsync(long id)
		{
			var request = jsonRequestFactory
				.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/operation/status?id=" + id).NoCache(), "GET", credentials, convention)
				.AddOperationHeaders(OperationsHeaders));

			return request.ReadResponseJsonAsync()
						  .ContinueWith(task =>
						  {
							  if (task.IsFaulted)
							  {
								  var webException = task.Exception.ExtractSingleInnerException() as WebException;

								  if (webException != null)
								  {
									  var httpWebResponse = webException.Response as HttpWebResponse;
									  if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
									  {
										  return null;
									  }
								  }
							  }
							  return task.Result;
						  });
		}

		public async Task<string> GetSingleAuthToken()
		{
			var tokenRequest = CreateRequest("/singleAuthToken".NoCache(), "GET", disableRequestCompression: true);

            var response = await tokenRequest.ReadResponseJsonAsync().ConfigureAwait(false);
			return response.Value<string>("Token");
		}

		private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
		{
			var request = CreateRequest("/singleAuthToken".NoCache(), "GET", disableRequestCompression: true);

			request.DisableAuthentication();
			request.webRequest.ContentLength = 0;
			request.AddOperationHeader("Single-Use-Auth-Token", token);
            var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
			return result.Value<string>("Token");
		}

		#region IAsyncGlobalAdminDatabaseCommands

		public IAsyncGlobalAdminDatabaseCommands GlobalAdmin
		{
			get { return this; }
		}

		Task<AdminStatistics> IAsyncGlobalAdminDatabaseCommands.GetStatisticsAsync()
		{
			return rootUrl.AdminStats()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var jo = ((RavenJObject)task.Result);
						return jo.Deserialize<AdminStatistics>(convention);
					});
		}

		#endregion

		#region IAsyncAdminDatabaseCommands

		public IAsyncAdminDatabaseCommands Admin
		{
			get { return this; }
		}

		#endregion

		#region IAsyncInfoDatabaseCommands

		public IAsyncInfoDatabaseCommands Info
		{
			get { return this; }
		}

		Task<ReplicationStatistics> IAsyncInfoDatabaseCommands.GetReplicationInfoAsync()
		{
			return Url.ReplicationInfo()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var jo = ((RavenJObject)task.Result);
						return jo.Deserialize<ReplicationStatistics>(convention);
					});
		}

		#endregion
	}
}
