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
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Http.Exceptions;
using Raven.Http.Json;
using Raven.Json.Linq;

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
		private readonly IDictionary<string, string> operationsHeaders = new Dictionary<string, string>();
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		/// <summary>
		/// Initializes a new instance of the <see cref="AsyncServerClient"/> class.
		/// </summary>
		/// <param name="url">The URL.</param>
		/// <param name="convention">The convention.</param>
		/// <param name="credentials">The credentials.</param>
		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials, HttpJsonRequestFactory jsonRequestFactory)
		{
			this.url = url;
			this.jsonRequestFactory = jsonRequestFactory;
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
		/// Returns a new <see cref="IDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new AsyncServerClient(url, convention, credentialsForSession, jsonRequestFactory);
		}

		/// <summary>
		/// Gets the index names from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the indexes from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Resets the specified index asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task ResetIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			throw new NotImplementedException();
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
			var webRequest = (HttpWebRequest)WebRequest.Create(requestUri);
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

					var request = jsonRequestFactory.CreateHttpJsonRequest(this, requestUri, "PUT", credentials, convention);
					request.AddOperationHeaders(OperationsHeaders);
					var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);
					byte[] bytes = Encoding.UTF8.GetBytes(serializeObject);
					return Task.Factory.FromAsync(request.BeginWrite, request.EndWrite,bytes, null)
						.ContinueWith(writeTask => Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
													.ContinueWith(readStrTask =>
													{
														var obj = new { index = "" };
														obj = JsonConvert.DeserializeAnonymousType(readStrTask.Result, obj);
														return obj.index;
													})).Unwrap();
				}).Unwrap();
		}

		/// <summary>
		/// Deletes the index definition for the specified name asyncronously
		/// </summary>
		/// <param name="name">The name.</param>
		public Task DeleteIndexAsync(string name)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Deletes the document for the specified id asyncronously
		/// </summary>
		/// <param name="id">The id.</param>
		public Task DeleteDocumentAsync(string id)
		{
			throw new NotImplementedException();
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

			var bytes = Encoding.UTF8.GetBytes(document.ToString());
			return Task.Factory.FromAsync(request.BeginWrite,request.EndWrite,bytes, null)
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						throw new InvalidOperationException("Unable to write to server");

					return Task.Factory.FromAsync<string>(request.BeginReadResponseString,request.EndReadResponseString, null)
						.ContinueWith(task1 =>
						{
							try
							{
								return JsonConvert.DeserializeObject<PutResult>(task1.Result, Default.Converters);
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

		private void AddOperationHeaders(HttpWebRequest webRequest)
		{
			foreach (var header in OperationsHeaders)
			{
				webRequest.Headers[header.Key] = header.Value;
			}
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
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
			return new AsyncServerClient(databaseUrl, convention, credentials, jsonRequestFactory);
		}

		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IAsyncDatabaseCommands GetRootDatabase()
		{
			var indexOfDatabases = url.IndexOf("/databases/");
			if (indexOfDatabases == -1)
				return this;

			return new AsyncServerClient(url.Substring(0, indexOfDatabases), convention, credentials, jsonRequestFactory);
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

			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/docs/" + key, "GET", metadata, credentials, convention);

			return Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
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
							LastModified = DateTime.ParseExact(request.ResponseHeaders["Last-Modified"], "r", CultureInfo.InvariantCulture).ToLocalTime(),
							Etag = new Guid(request.ResponseHeaders["ETag"]),
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
				});
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		/// <param name="keys">The keys.</param>
		/// <returns></returns>
		public Task<JsonDocument[]> MultiGetAsync(string[] keys)
		{
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/queries/", "POST", credentials, convention);
			var array = Encoding.UTF8.GetBytes(new RavenJArray(keys).ToString(Formatting.None));
			return Task.Factory.FromAsync(request.BeginWrite, request.EndWrite, array, null)
				.ContinueWith(writeTask => Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null))
				.Unwrap()
				.ContinueWith(task =>
				{
					RavenJArray responses;
					try
					{
						responses = RavenJObject.Parse(task.Result).Value<RavenJArray>("Results");
					}
					catch (WebException e)
					{
						var httpWebResponse = e.Response as HttpWebResponse;
						if (httpWebResponse == null ||
							httpWebResponse.StatusCode != HttpStatusCode.Conflict)
							throw;
						throw ThrowConcurrencyException(e);
					}

					return SerializationHelper.RavenJObjectsToJsonDocuments(responses.Cast<RavenJObject>())
						.ToArray();
				});
		}

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
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
			throw new NotImplementedException();
		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The include paths</param>
		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(url, index, "indexes");
			if (includes != null && includes.Length > 0)
			{
				path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(this, path, "GET", credentials, convention);

			return Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
				.ContinueWith(task =>
				{
					RavenJObject json;
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
						json = (RavenJObject)RavenJToken.Load(reader);

					return new QueryResult
					{
						IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
						IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
						IndexEtag = new Guid(request.ResponseHeaders["ETag"]),
						Results = json["Results"].Children().Cast<RavenJObject>().ToList(),
						TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
						IndexName = json.Value<string>("IndexName"),
						SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString())
					};
				});

		}

		/// <summary>
		/// Begins the async query.
		/// </summary>
		/// <param name="query">A string representation of a Linq query</param>
		public Task<QueryResult> LinearQueryAsync(string query, int start, int pageSize)
		{
			throw new NotImplementedException();
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
			var serializer = convention.CreateSerializer();

			return Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
				.ContinueWith(task =>
				{
					using (var reader = new JsonTextReader(new StringReader(task.Result)))
					{
						var json = (RavenJObject)serializer.Deserialize(reader);
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
			var metadata = new RavenJObject();
			AddTransactionInformation(metadata);
			var req = jsonRequestFactory.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials, convention);
			var jArray = new RavenJArray(commandDatas.Select(x => x.ToJson()));
			var data = Encoding.UTF8.GetBytes(jArray.ToString(Formatting.None));

			return Task.Factory.FromAsync(req.BeginWrite, req.EndWrite, data, null)
				.ContinueWith(writeTask => Task.Factory.FromAsync<string>(req.BeginReadResponseString, req.EndReadResponseString, null))
				.Unwrap()
				.ContinueWith(task =>
				{
					string response;
					try
					{
						response = task.Result;
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
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
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
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the list of databases from the server asyncronously
		/// </summary>
		public Task<string[]> GetDatabaseNamesAsync()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the list of collections from the server asyncronously
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public Task<Collection[]> GetCollectionsAsync(int start, int pageSize)
		{
			throw new NotImplementedException();
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
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the attachment by the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<Attachment> GetAttachmentAsync(string key)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Deletes the attachment with the specified key asyncronously
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public Task DeleteAttachmentAsync(string key, Guid? etag)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Ensures that the silverlight startup tasks have run
		/// </summary>
		public Task EnsureSilverlightStartUpAsync()
		{
			throw new NotImplementedException();
		}

        ///<summary>
        /// Get the possible terms for the specified field in the index asynchronously
        /// You can page through the results by use fromValue parameter as the 
        /// starting point for the next query
        ///</summary>
        ///<returns></returns>
        public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
        {
            throw new NotImplementedException();
        }
	}
}

#endif