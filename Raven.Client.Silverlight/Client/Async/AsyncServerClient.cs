//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5

namespace Raven.Client.Client.Async
{
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
	using Abstractions.Data;
	using Client;
	using Document;
	using Exceptions;
	using Database;
	using Database.Data;
	using Database.Indexing;
	using Http.Exceptions;
	using Http.Json;
	using Extensions;
	using Silverlight.Client;

	/// <summary>
	/// Access the database commands in async fashion
	/// </summary>
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly DocumentConvention convention;

		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		/// <param name="url">The URL.</param>
		/// <param name="convention">The convention.</param>
		/// <param name="credentials">The credentials.</param>
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials)
		{
		    this.url = url.EndsWith("/") ? url.Substring(0, url.Length - 1) : url;
			this.convention = convention;
			this.credentials = credentials;
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
			return new AsyncServerClient(databaseUrl, convention, credentials)
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
			return new AsyncServerClient(url, convention, credentialsForSession);
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

			return new AsyncServerClient(url.Substring(0, indexOfDatabases), convention, credentials);
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

			var metadata = new JObject();
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/docs/" + key, "GET", metadata, credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					try
					{
						var responseString = task.Result;
						return new JsonDocument
						{
							DataAsJson = JObject.Parse(responseString),
							NonAuthoritiveInformation = request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
							Key = key,
							LastModified = DateTime.ParseExact(request.ResponseHeaders["Last-Modified"].First(), "r", CultureInfo.InvariantCulture),
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
				var conflictsDoc = JObject.Load(new JsonTextReader(conflicts));
				var conflictIds = conflictsDoc.Value<JArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

				throw new ConflictException("Conflict detected on " + key +
				                            ", conflict must be resolved before the document will be accessible")
				{
					ConflictedVersionIds = conflictIds
				};
			}
			return false;
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		/// <param name="keys">The keys.</param>
		/// <returns></returns>
		public Task<JsonDocument[]> MultiGetAsync(string[] keys)
		{
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/queries/", "POST", credentials, convention);
			var array = Encoding.UTF8.GetBytes(new JArray(keys).ToString(Formatting.None));
			return request.WriteAsync(array)
				.ContinueWith(writeTask => request.ReadResponseStringAsync())
				.ContinueWith(task =>
				{
					JArray responses;
					try
					{
						responses = JObject.Parse(task.Result.Result).Value<JArray>("Results");
					}
					catch (WebException e)
					{
						var httpWebResponse = e.Response as HttpWebResponse;
						if (httpWebResponse == null ||
							httpWebResponse.StatusCode != HttpStatusCode.Conflict)
							throw;
						throw ThrowConcurrencyException(e);
					}

					return SerializationHelper.JObjectsToJsonDocuments(responses.Cast<JObject>())
						.ToArray();
				});
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
			return url.Docs(start,pageSize).ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task => JArray.Parse(task.Result)
				                      	.Cast<JObject>()
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
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					JToken json;
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
						json = (JToken)convention.CreateSerializer().Deserialize(reader);

					return new QueryResult
					{
						IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
						IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
						IndexEtag = new Guid(request.ResponseHeaders["ETag"].First()),
						Results = json["Results"].Children().Cast<JObject>().ToList(),
						TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
						SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString()),
						Includes = json["Includes"].Children().Cast<JObject>().ToList(), 
					};
				});
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		public Task<PutResult> PutAsync(string key, Guid? etag, JObject document, JObject metadata)
		{
			if (metadata == null)
				metadata = new JObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			if (etag != null)
				metadata["ETag"] = new JValue(etag.Value.ToString());
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/docs/" + key, method, metadata, credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);

			return request.WriteAsync(Encoding.UTF8.GetBytes(document.ToString()))
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						throw new InvalidOperationException("Unable to write to server");

					return request.ReadResponseStringAsync()
						.ContinueWith(task1 =>
						{
							try
							{
								return JsonConvert.DeserializeObject<PutResult>(task1.Result, new JsonEnumConverter());
							}
							catch(AggregateException e)
							{
								var webexception = e.ExtractSingleInnerException() as WebException;
								if(ShouldThrowForPutAsync(webexception)) throw;
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
			if(e == null) return true;
			var httpWebResponse = e.Response as HttpWebResponse;
			return (httpWebResponse == null ||
				httpWebResponse.StatusCode != HttpStatusCode.Conflict);
		}

		/// <summary>
		/// Puts the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">Should overwrite index</param>
		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			string requestUri = url + "/indexes/" + name;
			var webRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(requestUri));
			AddOperationHeaders(webRequest);
			webRequest.Method = "HEAD";
			webRequest.Credentials = credentials;

			return webRequest.GetResponseAsync()
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
						var webException = e.ExtractSingleInnerException() as WebException;
						if (ShouldThrowForPutIndexAsync(webException))
							throw;
					}
					catch (WebException e)
					{
						if (ShouldThrowForPutIndexAsync(e))
							throw;
					}

					var request = HttpJsonRequest.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
					request.AddOperationHeaders(OperationsHeaders);

					var serializeObject = JsonConvert.SerializeObject(indexDef, new JsonEnumConverter());
					return request
						.WriteAsync(Encoding.UTF8.GetBytes(serializeObject))
						.ContinueWith(writeTask => request.ReadResponseStringAsync()
													.ContinueWith(readStrTask =>
													{
														//NOTE: JsonConvert.DeserializeAnonymousType() doesn't work in Silverlight because the ctr is private!
														var obj = JsonConvert.DeserializeObject<IndexContainer>(readStrTask.Result);
														return obj.Index;
													})).Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Used for deserialization only :-P
		/// </summary>
		public class IndexContainer
		{
			public string Index {get;set;}
		}

		/// <summary>
		/// Deletes the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			return url.Indexes(name)
				.ToRequest(OperationsHeaders, credentials, "DELETE")
				.GetResponseAsync();
		}

		private static bool ShouldThrowForPutIndexAsync(WebException e)
		{
			if(e == null) return true;
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
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/indexes/?namesOnly=true&start=" + start + "&pageSize=" + pageSize, "GET", credentials, convention);
			
			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var serializer = convention.CreateSerializer();
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (JToken)serializer.Deserialize(reader);
						return json.Select(x => x.Value<string>()).ToArray();

					}
				});
		}

		/// <summary>
		/// Gets the indexes from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/indexes/?start=" + start + "&pageSize=" + pageSize, "GET", credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var serializer = convention.CreateSerializer();
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (JToken)serializer.Deserialize(reader);
						//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
						return json
							.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(x["definition"].ToString()))
							.ToArray();
					}
				});
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

			var request = HttpJsonRequest.CreateHttpJsonRequest(this, requestUri, "GET", credentials, convention);
			request.AddOperationHeaders(OperationsHeaders);
			var serializer = convention.CreateSerializer();

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (JToken)serializer.Deserialize(reader);
						return new SuggestionQueryResult
						{
							Suggestions = json["Suggestions"].Children().Select(x => x.Value<string>()).ToArray(),
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
			var metadata = new JObject();
			var req = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials, convention);
			var jArray = new JArray(commandDatas.Select(x => x.ToJson()));
			var data = Encoding.UTF8.GetBytes(jArray.ToString(Formatting.None));

			return req.WriteAsync(data)
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
					return JsonConvert.DeserializeObject<BatchResult[]>(response);
				});

		}

		private static Exception ThrowConcurrencyException(WebException e)
		{
			using (var sr = new StreamReader(e.Response.GetResponseStream()))
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
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/stats", "GET", credentials, convention);

			return request.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var response = task.Result;
					var jo = JObject.Parse(response);
					return jo.Deserialize<DatabaseStatistics>(convention);
				});
		}

		/// <summary>
		/// Gets the list of databases from the server asyncronously
		/// </summary>
		public Task<string[]> GetDatabaseNamesAsync()
		{
			return url.Databases()
				.ToJsonRequest(this, credentials, convention)
				.ReadResponseStringAsync()
				.ContinueWith(task =>
				{
					var serializer = convention.CreateSerializer();
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (JToken)serializer.Deserialize(reader);
						return json
							.Children()
							.Select(x => x.Value<JObject>("@metadata").Value<string>("@id").Replace("Raven/Databases/", string.Empty))
							.ToArray();
					}
				});
		}

		public Task<Collection[]> GetCollectionsAsync(int start, int pageSize)
		{
			var query =  new IndexQuery {Start = start,PageSize = pageSize, SortedFields = new[]{new SortedField("Name"), }};

			return QueryAsync("Raven/DocumentCollections", query, new string[]{})
					.ContinueWith(task => task.Result.Results.Select(x => x.Deserialize<Collection>(convention)).ToArray());
		}
	}
}

#endif