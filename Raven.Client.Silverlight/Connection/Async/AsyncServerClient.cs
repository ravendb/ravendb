//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Text;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Client.Connection.Async;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Exceptions;
using Raven.Client.Silverlight.Data;
using Raven.Client.Document;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Client.Extensions;
using System.Threading;

#if !NET35

namespace Raven.Client.Silverlight.Connection.Async
{
	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Guid? sessionId;
		private readonly Task veryFirstRequest;
		private readonly DocumentConvention convention;
		private readonly ProfilingInformation profilingInformation;
		private readonly Func<string, Raven.Client.Connection.ReplicationInformer> replicationInformerGetter;
		private readonly string databaseName;
		private readonly ReplicationInformer replicationInformer;
		private int readStripingBase;
		private int requestCount;

		/// <summary>
		/// Get the current json request factory
		/// </summary>
		public HttpJsonRequestFactory JsonRequestFactory
		{
			get { return jsonRequestFactory; }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials,
								 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId, Task veryFirstRequest,
								 Func<string, ReplicationInformer> replicationInformerGetter, string databaseName)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.url = url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url;
			this.convention = convention;
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.sessionId = sessionId;
			this.veryFirstRequest = veryFirstRequest;
			this.databaseName = databaseName;
			this.replicationInformerGetter = replicationInformerGetter;
			this.replicationInformer = replicationInformerGetter(databaseName);
			this.readStripingBase = replicationInformer.GetReadStripingBase();

			jsonRequestFactory.ConfigureRequest += (sender, args) =>
			{
				args.JsonRequest.WaitForTask = veryFirstRequest;
			};
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
		}

		public HttpJsonRequest CreateRequest(string relativeUrl, string method)
		{
			return jsonRequestFactory.CreateHttpJsonRequest(this, url + relativeUrl, method, credentials, convention);
		}

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IAsyncDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			databaseUrl = databaseUrl + "/databases/" + database + "/";
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId, veryFirstRequest, replicationInformerGetter, database)
			{
				operationsHeaders = operationsHeaders
			};
		}

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the default database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands ForDefaultDatabase()
		{
			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
			if (rootDatabaseUrl == url)
				return this;
			return new AsyncServerClient(rootDatabaseUrl, convention, credentials, jsonRequestFactory, sessionId, veryFirstRequest, replicationInformerGetter, databaseName)
			{
				operationsHeaders = operationsHeaders
			};
		}

		/// <summary>
		/// Returns a new <see cref="IAsyncDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new AsyncServerClient(url, convention, credentialsForSession, jsonRequestFactory, sessionId, veryFirstRequest, replicationInformerGetter, databaseName);
		}

		private IDictionary<string, string> operationsHeaders = new Dictionary<string, string>();

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
			return ExecuteWithReplication("GET", url => DirectGetAsync(url, key));
		}

		public Task<JsonDocument> DirectGetAsync(string url, string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			key = key.Replace("\\", @"/"); //NOTE: the present of \ causes the SL networking stack to barf, even though the Uri seemingly makes this translation itself

			var request = url.Docs(key)
				.NoCache()
				.ToJsonRequest(this, credentials, convention);

			return request
				.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					try
					{
						var token = task.Result;
						var docKey = key;
						IList<string> list;
						if(request.ResponseHeaders.TryGetValue(Constants.DocumentIdFieldName, out list))
						{
							docKey = list.FirstOrDefault() ?? key;
							request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
						}
						return new JsonDocument
						{
							DataAsJson = (RavenJObject)token,
							NonAuthoritativeInformation = request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
							Key = docKey,
							LastModified = DateTime.ParseExact(request.ResponseHeaders[Constants.LastModified].First(), "r", CultureInfo.InvariantCulture).ToLocalTime(),
							Etag = request.GetEtagHeader(),
							Metadata = request.ResponseHeaders.FilterHeaders()
						};
					}
					catch (AggregateException e)
					{
						var webException = e.ExtractSingleInnerException() as WebException;
						if (webException != null)
						{
							if (HandleWebExceptionForGetAsync(key, webException))
								return null;
						}
						throw;
					}
				});
		}

		private static bool HandleWebExceptionForGetAsync(string key, WebException e)
		{
			var httpWebResponse = e.Response as HttpWebResponse;
			if (httpWebResponse == null)
			{
				return false;
			}
			if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
			{
				return true;
			}
			if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
			{
				var conflicts = new StreamReader(httpWebResponse.GetResponseStream());
				var conflictsDoc = RavenJObject.Load(new JsonTextReader(conflicts));
				var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

				throw new ConflictException("Conflict detected on " + key +
											", conflict must be resolved before the document will be accessible")
				{
					ConflictedVersionIds = conflictIds,
					Etag = httpWebResponse.GetEtagHeader()
				};
			}
			return false;
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
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{
			return ExecuteWithReplication("GET", url =>
			{
				var postedData = JsonConvert.SerializeObject(requests);

				var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/multi_get/", "POST",
																			   credentials, convention);

				return httpJsonRequest.WriteAsync(postedData)
					.ContinueWith(
						task =>
						{
							task.Wait();// will throw if write errored
							return httpJsonRequest.ReadResponseJsonAsync()
								.ContinueWith(replyTask => convention.CreateSerializer().Deserialize<GetResponse[]>(new RavenJTokenReader(replyTask.Result)));
						})
					.Unwrap();
			});
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			var requestUri = url + "/logs";
			if (errorsOnly)
				requestUri += "?type=error";

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri.NoCache(), "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(task.Result)));
		}

		public Task<LicensingStatus> GetLicenseStatus()
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, (url + "/license/status").NoCache(), "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(task.Result)));
		}

		public Task StartBackupAsync(string backupLocation)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, (url + "/admin/backup").NoCache(), "POST", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			return request.WriteAsync(new RavenJObject
				{
					{"BackupLocation", backupLocation}
				}.ToString(Formatting.None))
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						return task;

					return request.ExecuteRequest();
				}).Unwrap();
		}

		public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize)
		{
			var metadata = new RavenJObject();
			var actualUrl = string.Format("{0}/docs?startsWith={1}&start={2}&pageSize={3}", url, Uri.EscapeDataString(keyPrefix), start, pageSize);
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, actualUrl, "GET", metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)task.Result).OfType<RavenJObject>()).ToArray());
	
		}

		public Task<BuildNumber> GetBuildNumber()
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, (url + "/build/version").NoCache(), "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task => convention.CreateSerializer().Deserialize<BuildNumber>(new RavenJTokenReader(task.Result)));
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

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri.NoCache(), "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJObject)task.Result;
					return json.JsonDeserialization<IDictionary<string, IEnumerable<FacetValue>>>();
				});
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes)
		{
			return ExecuteWithReplication("GET", url =>
			{
				var path = url + "/queries/?";
				if (includes != null && includes.Length > 0)
				{
					path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
				}
				// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
				// we are fine with that, requests to load > 128 items are going to be rare
				HttpJsonRequest request;
				if (keys.Length < 128)
				{
					path += "&" + string.Join("&", keys.Select(x => "id=" + x).ToArray());
					request = jsonRequestFactory.CreateHttpJsonRequest(this, path.NoCache(), "GET", credentials, convention);
					return request.ReadResponseJsonAsync()
						.ContinueWith(task => CompleteMultiGet(task));
				}
				request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "POST", credentials, convention);
				return request.WriteAsync(new JArray(keys).ToString(Formatting.None))
					.ContinueWith(writeTask => request.ReadResponseJsonAsync())
					.ContinueWith(task => CompleteMultiGet(task.Result));
			});
		}

		private static MultiLoadResult CompleteMultiGet(Task<RavenJToken> task)
		{
			try
			{
				var result = (RavenJObject)task.Result;

				return new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				};
			}
			catch (AggregateException e)
			{
				var webException = e.ExtractSingleInnerException() as WebException;
				if (webException == null)
					throw;

				var httpWebResponse = webException.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(webException);
			}
		}

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", url =>
			{
				return url.Docs(start, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task => ((RavenJArray)task.Result)
											.Cast<RavenJObject>()
											.ToJsonDocuments()
											.ToArray());
			});
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
			return ExecuteWithReplication("GET", url =>
			{
				return url.DocsStartingWith(prefix, start, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task => ((RavenJArray)task.Result)
											.Cast<RavenJObject>()
											.ToJsonDocuments()
											.ToArray());
			});
		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		/// <returns></returns>
		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes)
		{
			return ExecuteWithReplication("GET", url =>
			{
				EnsureIsNotNullOrEmpty(index, "index");
				var path = query.GetIndexQueryUrl(url, index, "indexes");
				if (includes != null && includes.Length > 0)
				{
					path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
				}
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, path.NoCache(), "GET", credentials, convention);

				return request.ReadResponseJsonAsync()
					.ContinueWith(task => AttemptToProcessResponse(() => SerializationHelper.ToQueryResult((RavenJObject)task.Result, request.GetEtagHeader())));
			});
		}

		public Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return ExecuteWithReplication("DELETE", url =>
			{
				string path = queryToDelete.GetIndexQueryUrl(url, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "DELETE", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);
				return request.ExecuteRequest()
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
			return ExecuteWithReplication("DELETE", url =>
			{
				return url.Docs(id)
					.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
					.ExecuteRequest();
			});
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
			return ExecuteWithReplication("PUT", url =>
			{
				if (metadata == null)
					metadata = new RavenJObject();
				var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
				if (etag != null)
					metadata["ETag"] = new RavenJValue(etag.Value.ToString());
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/docs/" + key, method, metadata, credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				return request.WriteAsync(document.ToString())
					.ContinueWith(task =>
					{
						if (task.Exception != null)
							throw new InvalidOperationException("Unable to write to server", task.Exception);

						return request.ReadResponseJsonAsync()
							.ContinueWith(task1 =>
							{
								try
								{
									return convention.CreateSerializer().Deserialize<PutResult>(new RavenJTokenReader(task1.Result));
								}
								catch (AggregateException e)
								{
									var webexception = e.ExtractSingleInnerException() as WebException;
									if (ShouldThrowForPutAsync(webexception))
										throw;
									throw ThrowConcurrencyException(webexception);
								}
							});
					})
					.Unwrap();
			});
		}

		static bool ShouldThrowForPutAsync(WebException e)
		{
			if (e == null)
				return true;
			var httpWebResponse = e.Response as HttpWebResponse;
			return (httpWebResponse == null ||
				httpWebResponse.StatusCode != HttpStatusCode.Conflict);
		}

		/// <summary>
		/// Gets the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return ExecuteWithReplication("GET", url =>
			{
				return url.IndexDefinition(name)
				.NoCache()
				.ToJsonRequest(this, credentials, convention).ReadResponseJsonAsync()
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
			return ExecuteWithReplication("PUT", url =>
			{
				string requestUri = url + "/indexes/" + Uri.EscapeUriString(name) + "?definition=yes";
				var webRequest = requestUri
					.ToJsonRequest(this, credentials, convention, OperationsHeaders, "GET");

				return webRequest.ExecuteRequest()
					.ContinueWith(task =>
					{
						try
						{
							task.Wait(); // should throw if it is bad
							if (overwrite == false)
								throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
						}
						catch (AggregateException e)
						{
							var webException = e.ExtractSingleInnerException() as WebException;
							if (ShouldThrowForPutIndexAsync(webException))
								throw;
						}

						var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
						request.AddOperationHeaders(OperationsHeaders);

						var serializeObject = JsonConvert.SerializeObject(indexDef, new JsonEnumConverter());
						return request
							.WriteAsync(serializeObject)
							.ContinueWith(writeTask => AttemptToProcessResponse(() => request
								.ReadResponseJsonAsync()
								.ContinueWith(readStrTask => AttemptToProcessResponse(() =>
									{
										//NOTE: JsonConvert.DeserializeAnonymousType() doesn't work in Silverlight because the ctor is private!
										var obj = convention.CreateSerializer().Deserialize<IndexContainer>(new RavenJTokenReader(readStrTask.Result));
										return obj.Index;
									})))
						).Unwrap();
					}).Unwrap();
			});
		}

		/// <summary>
		/// Used for deserialization only :-P
		/// </summary>
		public class IndexContainer
		{
			public string Index { get; set; }
		}

		/// <summary>
		/// Deletes the index definition for the specified name asynchronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return ExecuteWithReplication("DELETE", url =>
			{
				return url.Indexes(name)
					.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
					.ExecuteRequest();
			});
		}

		private static bool ShouldThrowForPutIndexAsync(WebException e)
		{
			if (e == null)
				return true;
			var response = e.Response as HttpWebResponse;
			return (response == null || response.StatusCode != HttpStatusCode.NotFound);
		}


		/// <summary>
		/// Gets the index names from the server asynchronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return ExecuteWithReplication("GET", url =>
			{
				return url.IndexNames(start, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
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
			return ExecuteWithReplication("GET", url =>
			{
				var url2 = (url + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, url2, "GET", credentials, convention);

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
			return ExecuteWithReplication("RESET", url =>
			{
				var httpJsonRequestAsync = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/indexes/" + name, "RESET", credentials, convention);
				httpJsonRequestAsync.AddOperationHeaders(OperationsHeaders);
				return httpJsonRequestAsync.ReadResponseJsonAsync();
			});
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery)
		{
			return ExecuteWithReplication("GET", url =>
			{
				if (suggestionQuery == null)
					throw new ArgumentNullException("suggestionQuery");

				var requestUri = url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&distance={4}&accuracy={5}",
					Uri.EscapeUriString(index),
					Uri.EscapeDataString(suggestionQuery.Term),
					Uri.EscapeDataString(suggestionQuery.Field),
					Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToString()),
					Uri.EscapeDataString(suggestionQuery.Distance.ToString()),
					Uri.EscapeDataString(suggestionQuery.Accuracy.ToString()));

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri.NoCache(), "GET", credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

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
			return ExecuteWithReplication("POST", url =>
			{
				var metadata = new RavenJObject();
				var req = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials, convention);
				var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));

				return req.WriteAsync(jArray.ToString(Formatting.None))
					.ContinueWith(writeTask => req.ReadResponseJsonAsync())
					.Unwrap()
					.ContinueWith(task =>
					{
						RavenJToken response;
						try
						{
							response = task.Result;
						}
						catch (AggregateException e)
						{
							var webException = e.ExtractSingleInnerException() as WebException;
							if (webException == null)
								throw;

							var httpWebResponse = webException.Response as HttpWebResponse;
							if (httpWebResponse == null ||
								httpWebResponse.StatusCode != HttpStatusCode.Conflict)
								throw;
							throw ThrowConcurrencyException(webException);
						}
						return convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
					});
			});
		}

		public class ConcurrencyExceptionResult
		{
			private readonly string url1;
			private readonly Guid actualETag1;
			private readonly Guid expectedETag1;
			private readonly string error1;

			public string url
			{
				get { return url1; }
			}

			public Guid actualETag
			{
				get { return actualETag1; }
			}

			public Guid expectedETag
			{
				get { return expectedETag1; }
			}

			public string error
			{
				get { return error1; }
			}

			public ConcurrencyExceptionResult(string url, Guid actualETag, Guid expectedETag, string error)
			{
				url1 = url;
				actualETag1 = actualETag;
				expectedETag1 = expectedETag;
				error1 = error;
			}
		}

		private static Exception ThrowConcurrencyException(WebException e)
		{
			using (var sr = new StreamReader(e.Response.GetResponseStream()))
			{
				var text = sr.ReadToEnd();
				var errorResults = JsonConvert.DeserializeAnonymousType(text, new ConcurrencyExceptionResult((string)null, Guid.Empty, Guid.Empty, (string)null));
				return new ConcurrencyException(errorResults.error)
				{
					ActualETag = errorResults.actualETag,
					ExpectedETag = errorResults.expectedETag
				};
			}
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
			return ExecuteWithReplication("GET", url =>
			{
				return url.Databases(pageSize, start)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						return json
							.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
							.ToArray();
					});
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
			return ExecuteWithReplication("PUT", url =>
			{
				if (metadata == null)
					metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue(etag.Value.ToString());

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, url.Static(key), "PUT", metadata, credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				return request
					.WriteAsync(data)
					.ContinueWith(write =>
					{
						if (write.Exception != null)
							throw new InvalidOperationException("Unable to write to server");

						return request.ExecuteRequest();
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

			return ExecuteWithReplication("GET", url =>
			{
				var request = url.Static(key)
					.ToJsonRequest(this, credentials, convention);

				return request
					.ReadResponseBytesAsync()
					.ContinueWith(task =>
					{
						try
						{
							var buffer = task.Result;
							return new Attachment
									{
										Data = () => new MemoryStream(buffer),
										Etag = request.GetEtagHeader(),
										Metadata = request.ResponseHeaders.FilterHeaders()
									};
						}
						catch (AggregateException e)
						{
							var webException = e.ExtractSingleInnerException() as WebException;
							if (webException != null)
							{
								if (HandleWebExceptionForGetAsync(key, webException))
									return null;
							}
							throw;
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
			return ExecuteWithReplication("DELETE", url =>
			{
				var metadata = new RavenJObject();

				if (etag != null)
					metadata["ETag"] = new RavenJValue(etag.Value.ToString());

				var request = jsonRequestFactory.CreateHttpJsonRequest(this, url.Static(key), "DELETE", metadata, credentials, convention);
				request.AddOperationHeaders(OperationsHeaders);

				return request.ExecuteRequest();
			});
		}

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		public Task EnsureSilverlightStartUpAsync()
		{
			return ExecuteWithReplication("GET", url =>
			{
				return url
					.SilverlightEnsuresStartup()
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseBytesAsync();
			});
		}

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			return ExecuteWithReplication("GET", url =>
			{
				return url.Terms(index, field, fromValue, pageSize)
					.NoCache()
					.ToJsonRequest(this, credentials, convention)
					.ReadResponseJsonAsync()
					.ContinueWith(task =>
					{
						var json = ((RavenJArray)task.Result);
						return json.Select(x => x.Value<string>()).ToArray();
					});
			});
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return null; // we don't implement this
		}

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}

		public string Url
		{
			get { return url; }
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public void ForceReadFromMaster()
		{
			readStripingBase = -1;// this means that will have to use the master url first
		}

		private Task ExecuteWithReplication(string method, Func<string, Task> operation)
		{
			// Convert the Func<string, Task> to a Func<string, Task<object>>
			return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t => null));
		}

		private Task<T> ExecuteWithReplication<T>(string method, Func<string, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			return replicationInformer.ExecuteWithReplicationAsync(method, url, currentRequest, readStripingBase, operation);
		}
	}
}

#endif