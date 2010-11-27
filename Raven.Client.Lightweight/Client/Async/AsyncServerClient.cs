#if !NET_3_5

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;
using Raven.Http.Exceptions;

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
		/// Begins an async get operation
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Task<JsonDocument> GetAsync(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/docs/" + key, "GET", metadata, credentials);

		    return Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
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
                            LastModified = DateTime.ParseExact(request.ResponseHeaders["Last-Modified"], "r", CultureInfo.InvariantCulture),
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
		        });
		}

		/// <summary>
		/// Begins an async multi get operation
		/// </summary>
		/// <param name="keys">The keys.</param>
		/// <returns></returns>
        public Task<JsonDocument[]> MultiGetAsync(string[] keys)
		{
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/queries/", "POST", credentials);
			var array = Encoding.UTF8.GetBytes(new JArray(keys).ToString(Formatting.None));
            return Task.Factory.FromAsync(request.BeginWrite, request.EndWrite, array, null)
                .ContinueWith(writeTask => Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null))
                .Unwrap()
		        .ContinueWith(task =>
		        {
                    JArray responses;
                    try
                    {
                        responses = JObject.Parse(task.Result).Value<JArray>("Results");
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
		/// <returns></returns>
        public Task<QueryResult> QueryAsync(string index, IndexQuery query)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = query.GetIndexQueryUrl(url, index, "indexes");
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials);

		    return Task.Factory.FromAsync<string>(request.BeginReadResponseString, request.EndReadResponseString, null)
		        .ContinueWith(task =>
		        {
		            JToken json;
		            using (var reader = new JsonTextReader(new StringReader(task.Result)))
		                json = (JToken) convention.CreateSerializer().Deserialize(reader);

		            return new QueryResult
		            {
		                IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
		                IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
		                Results = json["Results"].Children().Cast<JObject>().ToList(),
		                TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
		                SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString())
		            };
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
			AddTransactionInformation(metadata);
			var req = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials);
			var jArray = new JArray(commandDatas.Select(x => x.ToJson()));
			var data = Encoding.UTF8.GetBytes(jArray.ToString(Formatting.None));

		    return Task.Factory.FromAsync(req.BeginWrite, req.EndWrite, data, null)
                .ContinueWith(writeTask => Task.Factory.FromAsync<string>(req.BeginReadResponseString, req.EndReadResponseString,null))
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
		            return JsonConvert.DeserializeObject<BatchResult[]>(response);
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

		private static void AddTransactionInformation(JObject metadata)
		{
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new JValue(txInfo);
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}
	}
}

#endif