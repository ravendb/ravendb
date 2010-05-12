using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Transactions;
using System.Web;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Client.Client
{
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;
		private readonly DocumentConvention convention;
	    private readonly ICredentials credentials;

	    public ServerClient(string url, DocumentConvention convention, ICredentials credentials)
		{
	        this.credentials = credentials;
	        this.url = url;
			this.convention = convention;
		}

		#region IDatabaseCommands Members

		public JsonDocument Get(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

		    var metadata = new JObject();
		    AddTransactionInformation(metadata);
			var request = new HttpJsonRequest(url + "/docs/" + key, "GET", metadata, credentials);
			try
			{
				return new JsonDocument
				{
					DataAsJson = JObject.Parse(request.ReadResponseString()),
					Key = key,
					Etag = new Guid(request.ResponseHeaders["ETag"]),
					Metadata = request.ResponseHeaders.FilterHeaders()
				};
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
				return null;
			}
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if(string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
            if (metadata == null)
                metadata = new JObject();
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
            AddTransactionInformation(metadata);
            if (etag != null)
                metadata["ETag"] = new JValue(etag.Value.ToString());
		    var request = new HttpJsonRequest(url + "/docs/" + key, method, metadata, credentials);
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
	        if (Transaction.Current == null) 
                return;

	        string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
	        metadata["Raven-Transaction-Information"] = new JValue(txInfo);
	    }

	    public void Delete(string key, Guid? etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
	        var metadata = new JObject();
            if (etag != null)
                metadata.Add("ETag", new JValue(etag.Value.ToString()));
	        AddTransactionInformation(metadata);
	        var httpJsonRequest = new HttpJsonRequest(url + "/docs/" + key, "DELETE", metadata, credentials);
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
	        using (var sr = new StreamReader(e.Response.GetResponseStream()))
	        {
	            var text = sr.ReadToEnd();
	            var errorResults = JsonConvert.DeserializeAnonymousType(text, new
	            {
	                url = (string) null,
	                actualETag = Guid.Empty,
	                expectedETag = Guid.Empty,
	                error = (string) null
	            });
	            return new ConcurrencyException(errorResults.error)
	            {
	                ActualETag = errorResults.actualETag,
	                ExpectedETag = errorResults.expectedETag
	            };
	        }
	    }

        public string PutIndex(string name, IndexDefinition definition)
        {
            return PutIndex(name, definition, false);
        }

        public string PutIndex(string name, IndexDefinition definition, bool overwrite)
        {
			EnsureIsNotNullOrEmpty(name, "name");

            string requestUri = url+"/indexes/"+name;
            try
            {
                var webRequest = (HttpWebRequest)WebRequest.Create(requestUri);
                webRequest.Method = "HEAD";
                webRequest.GetResponse().Close();
                if(overwrite == false)
                    throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response == null || response.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }

            var request = new HttpJsonRequest(requestUri, "PUT", credentials);
			request.Write(JsonConvert.SerializeObject(definition, new JsonEnumConverter()));

			var obj = new {index = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention));
		}


        public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite)
        {
            return PutIndex(name, indexDef.ToIndexDefinition(convention), overwrite);
        }

		public QueryResult Query(string index, IndexQuery query)
		{
            EnsureIsNotNullOrEmpty(index, "index");
            var path = string.Format("{0}/indexes/{1}?query={2}&start={3}&pageSize={4}", url, index, query.Query, query.Start, query.PageSize);
            if (query.FieldsToFetch != null && query.FieldsToFetch.Length > 0)
            {
                path = query.FieldsToFetch.Aggregate(
                        new StringBuilder(path),
                        (sb, field) => sb.Append("&fetch=").Append(field)
                    ).ToString();
            }
            if(query.SortedFields!=null && query.SortedFields.Length>0)
            {
                path = query.SortedFields.Aggregate(
                        new StringBuilder(path),
						(sb, field) => sb.Append("&sort=").Append(field.Descending ? "-" : "").Append(field.Field)
                    ).ToString();
            }
            var request = new HttpJsonRequest(path, "GET", credentials);
            var serializer = new JsonSerializer();
            JToken json;
            using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
                json = (JToken)serializer.Deserialize(reader);

            return new QueryResult
            {
                IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
                Results = json["Results"].Children().Cast<JObject>().ToArray(),
                TotalResults =  Convert.ToInt32(json["TotalResults"].ToString())
            }; 
	    }

	    public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
            var request = new HttpJsonRequest(url + "/indexes/" + name, "DELETE", credentials);
		    request.ReadResponseString();
		}

	    public JsonDocument[] Get(string[] ids)
	    {
            var request = new HttpJsonRequest(url + "/queries/", "POST", credentials);
            request.Write(new JArray(ids).ToString(Formatting.None));
            var responses = JArray.Parse(request.ReadResponseString());

	        return (from doc in responses.Cast<JObject>()
	                let metadata = (JObject) doc["@metadata"]
	                let _ = doc.Remove("@metadata")
	                select new JsonDocument
	                {
	                    Key = metadata["@id"].Value<string>(),
	                    Etag = new Guid(metadata["@etag"].Value<string>()),
	                    Metadata = metadata,
	                    DataAsJson = doc,
	                })
	            .ToArray();
	    }

		public BatchResult[] Batch(ICommandData[] commandDatas)
		{
			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var req = new HttpJsonRequest(url + "/bulk_docs", "POST",metadata, credentials);
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

		public void Commit(Guid txId)
	    {
	        var httpJsonRequest = new HttpJsonRequest(url + "/transaction/commit?tx=" + txId, "POST", credentials);
	        httpJsonRequest.ReadResponseString();
	    }

	    public void Rollback(Guid txId)
	    {
            var httpJsonRequest = new HttpJsonRequest(url + "/transaction/rollback?tx=" + txId, "POST", credentials);
            httpJsonRequest.ReadResponseString();
	    }

	    public IDatabaseCommands With(ICredentials credentialsForSession)
	    {
	        return new ServerClient(url, convention, credentialsForSession);
	    }

	    #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}