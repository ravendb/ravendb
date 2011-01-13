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
using System.Net.Browser;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Http.Exceptions;
using Raven.Http.Json;

namespace Raven.Client.Client.Async
{
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
			this.url = url;
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
						while (true)
						{
							if (e.InnerExceptions.Count != 1)
								throw;
							var aggregateException = e.InnerExceptions[0] as AggregateException;
							if (aggregateException == null)
								break;
							e = aggregateException;
						}
						
						var webException = e.InnerExceptions[0] as WebException;
						if (webException != null)
						{
							if (HandleWebException(key, webException))
								return null;
						}
						throw;
					}
					catch (WebException e)
					{
						if (HandleWebException(key, e))
							return null;
						throw;
					}
				});
		}

		private bool HandleWebException(string key, WebException e)
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
						SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString())
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
							catch (WebException e)
							{
								var httpWebResponse = e.Response as HttpWebResponse;
								if (httpWebResponse == null ||
									httpWebResponse.StatusCode != HttpStatusCode.Conflict)
									throw;
								throw ThrowConcurrencyException(e);
							}
						});
				})
				.Unwrap();
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
					catch (WebException e)
					{
						var response = e.Response as HttpWebResponse;
						if (response == null || response.StatusCode != HttpStatusCode.NotFound)
							throw;
					}

					var request = HttpJsonRequest.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
					request.AddOperationHeaders(OperationsHeaders);
					var serializeObject = JsonConvert.SerializeObject(indexDef, new JsonEnumConverter());
					return request.WriteAsync(Encoding.UTF8.GetBytes(serializeObject))
						.ContinueWith(writeTask => request.ReadResponseStringAsync()
													.ContinueWith(readStrTask =>
													{
														var obj = new { index = "" };
														obj = JsonConvert.DeserializeAnonymousType(readStrTask.Result, obj);
														return obj.index;
													})).Unwrap();
				}).Unwrap();
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
	}
}

#endif