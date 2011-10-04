//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
using System.Net.Browser;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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

#if !NET_3_5

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
		private readonly DocumentConvention convention;
		private readonly ProfilingInformation profilingInformation;

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
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.url = url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url;
			this.convention = convention;
			this.credentials = credentials;
			this.jsonRequestFactory = jsonRequestFactory;
			this.sessionId = sessionId;
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
		}

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interacts
		/// with the specified database
		/// </summary>
		public IAsyncDatabaseCommands ForDatabase(string database)
		{
			var databaseUrl = url;
			var indexOfDatabases = databaseUrl.IndexOf("/databases/");
			if (indexOfDatabases != -1)
				databaseUrl = databaseUrl.Substring(0, indexOfDatabases);
			if (databaseUrl.EndsWith("/") == false)
				databaseUrl += "/";
			databaseUrl = databaseUrl + "databases/" + database + "/";
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory, sessionId)
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
			return new AsyncServerClient(url, convention, credentialsForSession, jsonRequestFactory, sessionId);
		}

		/// <summary>
		/// Create a new instance of <see cref="IAsyncDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands GetRootDatabase()
		{
			var indexOfDatabases = url.IndexOf("/databases/");
			if (indexOfDatabases == -1)
				return this;

			return new AsyncServerClient(url.Substring(0, indexOfDatabases), convention, credentials, jsonRequestFactory, sessionId);
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
			EnsureIsNotNullOrEmpty(key, "key");

			key = key.Replace("\\",@"/"); //NOTE: the present of \ causes the SL networking stack to barf, even though the Uri seemingly makes this translation itself

			var request = url.Docs(key)
				.ToJsonRequest(this, credentials, convention);

			return request
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					try
					{
						var responseString = task.Result;
						return new JsonDocument
						{
							DataAsJson = RavenJObject.Parse(responseString),
							NonAuthoritiveInformation = request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
							Key = key,
							LastModified = DateTime.ParseExact(request.ResponseHeaders[Constants.LastModified].First(), "r", CultureInfo.InvariantCulture).ToLocalTime(),
							Etag = new Guid(request.ResponseHeaders["ETag"].First()),
							Metadata = request.ResponseHeaders.FilterHeaders(isServerDocument: false)
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
					catch (WebException e)
					{
						if (HandleWebExceptionForGetAsync(key, e))
							return null;
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
					ConflictedVersionIds = conflictIds
				};
			}
			return false;
		}

		private T AttemptToProcessResponse<T>(Func<T> process) where T:class 
		{
			try
			{
				return process();
			}
			catch (AggregateException e)
			{
				var webException = e.ExtractSingleInnerException() as WebException;
				if (webException == null) throw;

				if (HandleException(webException)) return null; 
				
				throw; 
			}
			catch (WebException e)
			{
				if (HandleException(e)) return null;

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
		/// Perform a single POST requst containing multiple nested GET requests
		/// </summary>
		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{
			var postedData = JsonConvert.SerializeObject(requests);

			var httpJsonRequest = jsonRequestFactory.CreateHttpJsonRequest(this, url+ "/multi_get/", "POST",
																		   credentials, convention);

			return httpJsonRequest.WriteAsync(postedData)
				.ContinueWith(
					task =>
					{
						task.Wait();// will throw if write errored
						return httpJsonRequest.ReadResponseStringAsync()
							.ContinueWith(replyTask => JsonConvert.DeserializeObject<GetResponse[]>(replyTask.Result));
					})
				.Unwrap();
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			var requestUri = url + "/logs";
			if (errorsOnly)
				requestUri += "?type=error";

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						return convention.CreateSerializer().Deserialize<LogItem[]>(reader);
					}
				});
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

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (RavenJObject)RavenJToken.Load(reader);
						var jsonAsType = json.JsonDeserialization<IDictionary<string, IEnumerable<FacetValue>>>();
						return jsonAsType;
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
			// if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
			// we are fine with that, requests to load > 128 items are going to be rare
			HttpJsonRequest request;
			if (keys.Length < 128)
			{
				path += "&" + string.Join("&", keys.Select(x => "id=" + x).ToArray());
				request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);
				return request.ReadResponseStringAsync()
					.ContinueWith(task => CompleteMultiGet(task));
			}
			request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "POST", credentials, convention);
			return request.WriteAsync(new JArray(keys).ToString(Formatting.None))
				.ContinueWith(writeTask => request.ReadResponseStringAsync())
				.ContinueWith(task => CompleteMultiGet(task.Result));
				
		}

		private static MultiLoadResult CompleteMultiGet(Task<string> task)
		{
			try
			{
				var result = RavenJObject.Parse(task.Result);

				return new MultiLoadResult
				{
					Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
					Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
				};
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
			return url.Docs(start, pageSize)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task => RavenJArray.Parse(task.Result)
										.Cast<RavenJObject>()
										.ToJsonDocuments()
										.ToArray());
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
			return url.DocsStartingWith(prefix, start, pageSize)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task => RavenJArray.Parse(task.Result)
										.Cast<RavenJObject>()
										.ToJsonDocuments()
										.ToArray());
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
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(url, index, "indexes");
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task => AttemptToProcessResponse( ()=>
				{
					RavenJObject json;
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
						json = (RavenJObject)RavenJToken.ReadFrom(reader);

					return SerializationHelper.ToQueryResult(json, request.ResponseHeaders["ETag"].First());
				}));
		}

		/// <summary>
		/// Deletes the document for the specified id asyncronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
			return url.Docs(id)
				.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
				.ReadResponseStringAsync();
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

			return request.WriteAsync(document.ToString())
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						throw new InvalidOperationException("Unable to write to server", task.Exception);

					return request.ReadResponseStringAsync()
						.ContinueWith(task1 =>
						{
							try
							{
								return JsonConvert.DeserializeObject<PutResult>(task1.Result, new JsonEnumConverter(), new JsonToJsonConverter());
							}
							catch (AggregateException e)
							{
								var webexception = e.ExtractSingleInnerException() as WebException;
								if (ShouldThrowForPutAsync(webexception)) throw;
								throw ThrowConcurrencyException(webexception);
							}
							catch (WebException e)
							{
								if (ShouldThrowForPutAsync(e)) throw;
								throw ThrowConcurrencyException(e);
							}
						});
				})
				.Unwrap();
		}

		static bool ShouldThrowForPutAsync(WebException e)
		{
			if (e == null) return true;
			var httpWebResponse = e.Response as HttpWebResponse;
			return (httpWebResponse == null ||
				httpWebResponse.StatusCode != HttpStatusCode.Conflict);
		}

		/// <summary>
		/// Gets the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return url.IndexDefinition(name)
			.NoCache()
			.ToJsonRequest(this, credentials, convention).ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var json = JObject.Parse(task.Result);
					//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
					return JsonConvert.DeserializeObject<IndexDefinition>(json["Index"].ToString(), new JsonToJsonConverter());
				});
		}

		/// <summary>
		/// Puts the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			string requestUri = url + "/indexes/" + Uri.EscapeUriString(name);
			var webRequest = requestUri
				.ToJsonRequest(this, credentials, convention, OperationsHeaders, "HEAD");

			return webRequest.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					try
					{
						if (overwrite == false)
							throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
					}
					catch (AggregateException e)
					{
						var webException = e.ExtractSingleInnerException() as WebException;
						if (ShouldThrowForPutIndexAsync(webException))
							throw;
					}
					catch (WebException e)
					{
						if (ShouldThrowForPutIndexAsync(e))
							throw;
					}

					var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
					request.AddOperationHeaders(OperationsHeaders);

					var serializeObject = JsonConvert.SerializeObject(indexDef, new JsonEnumConverter());
					return request
						.WriteAsync(serializeObject)
						.ContinueWith(writeTask => AttemptToProcessResponse( ()=> request
							.ReadResponseStringAsync()
							.ContinueWith(readStrTask => AttemptToProcessResponse( ()=>
								{
									//NOTE: JsonConvert.DeserializeAnonymousType() doesn't work in Silverlight because the ctr is private!
									var obj = JsonConvert.DeserializeObject<IndexContainer>(readStrTask.Result, new JsonToJsonConverter());
									return obj.Index;
								})))
					).Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Used for deserialization only :-P
		/// </summary>
		public class IndexContainer
		{
			public string Index { get; set; }
		}

		/// <summary>
		/// Deletes the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return url.Indexes(name)
				.ToJsonRequest(this, credentials, convention, OperationsHeaders, "DELETE")
				.ReadResponseStringAsync();
		}

		private static bool ShouldThrowForPutIndexAsync(WebException e)
		{
			if (e == null) return true;
			var response = e.Response as HttpWebResponse;
			return (response == null || response.StatusCode != HttpStatusCode.NotFound);
		}


		/// <summary>
		/// Gets the index names from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return url.IndexNames(start, pageSize)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var json = RavenJArray.Parse(task.Result);
					return json.Select(x => x.Value<string>()).ToArray();
				});
		}

		/// <summary>
		/// Gets the indexes from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
		    var url2 = (url + "/indexes/?start=" + start + "&pageSize=" + pageSize).NoCache();
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url2, "GET", credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var json = RavenJArray.Parse(task.Result);
					//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
					return json
						.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
						.ToArray();
				});
		}

		/// <summary>
		/// Resets the specified index asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task ResetIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		private void AddOperationHeaders(HttpWebRequest webRequest)
		{
			foreach (var header in OperationsHeaders)
			{
				webRequest.Headers[header.Key] = header.Value;
			}
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

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (RavenJObject)RavenJToken.Load(reader);
						return new SuggestionQueryResult
						{
							Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
						};
					}
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
			var req = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials, convention);
			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));

			return req.WriteAsync(jArray.ToString(Formatting.None))
				.ContinueWith(writeTask => req.ReadResponseStringAsync())
				.ContinueWith(task =>
				{
					string response;
					try
					{
						response = task.Result.Result;
					}
					catch (WebException e)
					{
						var httpWebResponse = e.Response as HttpWebResponse;
						if (httpWebResponse == null ||
							httpWebResponse.StatusCode != HttpStatusCode.Conflict)
							throw;
						throw ThrowConcurrencyException(e);
					}
					return JsonConvert.DeserializeObject<BatchResult[]>(response, new JsonToJsonConverter());
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
				.AddOperationHeader("Raven-Timer-Request" , "true")
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var response = task.Result;
					var jo = RavenJObject.Parse(response);
					return jo.Deserialize<DatabaseStatistics>(convention);
				});
		}

		/// <summary>
		/// Gets the list of databases from the server asyncronously
		/// </summary>
		public Task<string[]> GetDatabaseNamesAsync()
		{
			return url.Databases()
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var json = (RavenJArray)RavenJToken.Parse(task.Result);
					return json
						.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
						.ToArray();
				});
		}

		/// <summary>
		/// Puts the attachment with the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public Task PutAttachmentAsync(string key, Guid? etag, byte[] data, RavenJObject metadata)
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

					return request.ReadResponseStringAsync();
				}).Unwrap();
		}

		/// <summary>
		/// Gets the attachment by the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<Attachment> GetAttachmentAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			var request = url.Static(key)
				.ToJsonRequest(this, credentials, convention);

			return request
				.ReadResponseBytesAsync()
				.ContinueWith(task =>
				{
					try
					{
						return new Attachment
								{
									Data = task.Result,
									Etag = new Guid(request.ResponseHeaders["ETag"].First()),
									Metadata = request.ResponseHeaders.FilterHeaders(isServerDocument: false)
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
					catch (WebException e)
					{
						if (HandleWebExceptionForGetAsync(key, e))
							return null;
						throw;
					}
				});
		}

		/// <summary>
		/// Deletes the attachment with the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Guid? etag)
		{
			var metadata = new RavenJObject();

			if (etag != null)
				metadata["ETag"] = new RavenJValue(etag.Value.ToString());

			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url.Static(key), "DELETE", metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.ReadResponseStringAsync();
		}

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		public Task EnsureSilverlightStartUpAsync()
		{
			return url
				.SilverlightEnsuresStartup()
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseBytesAsync();
		}

		///<summary>
		/// Get the possible terms for the specified field in the index asynchronously
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
	    public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
	    {
			return url.Terms(index,field,fromValue,pageSize)
				.NoCache()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = RavenJArray.Load(reader);
						return json.Select(x => x.Value<string>()).ToArray();
					}
				});
	    }

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			return null; // we dont implement this
		}

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}
	}
}

#endif