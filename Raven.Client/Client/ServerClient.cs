using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Client
{
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;

		public ServerClient(string server, int port)
		{
			url = String.Format("http://{0}:{1}", server, port);
		}

		#region IDatabaseCommands Members

		public JsonDocument Get(string key)
		{
			EnsureIsNotNullOrEmpty(key, "key");

		    var metadata = new JObject();
		    AddTransactionInformation(metadata);
			var request = new HttpJsonRequest(url + "/docs/" + key, "GET", metadata);
			return new JsonDocument
			{
				Data = Encoding.UTF8.GetBytes(request.ReadResponseString()),
				Key = key,
			};
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if(string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public string Put(string key, Guid? etag, JObject document, JObject metadata)
		{
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
            AddTransactionInformation(metadata);
		    var request = new HttpJsonRequest(url + "/docs/" + key, method, metadata);
			request.Write(document.ToString());

			var obj = new {id = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.id;
		}

	    private static void AddTransactionInformation(JObject metadata)
	    {
	        if (Transaction.Current == null) 
                return;

	        string txInfo = Transaction.Current.TransactionInformation.DistributedIdentifier + ", " +
	                        TransactionManager.DefaultTimeout.ToString("c");
	        metadata["Raven-Transaction-Information"] = new JValue(txInfo);
	    }

	    public void Delete(string key, Guid? etag)
		{
			EnsureIsNotNullOrEmpty(key, "key");
	        var metadata = new JObject();
            if (etag != null)
                metadata.Add("ETag", new JValue(etag.Value.ToString()));
	        AddTransactionInformation(metadata);
	        var httpJsonRequest = new HttpJsonRequest(url + "/docs/" + key, "DELETE", metadata);
	        httpJsonRequest.ReadResponseString();
		}

		public string PutIndex(string name, string indexDef)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			var request = new HttpJsonRequest(url + "/indexes/" + name, "PUT");
			request.Write(indexDef);

			var obj = new {index = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

		public QueryResult Query(string index, string query, int start, int pageSize)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			var path = url + "/indexes/" + index + "?query=" + query + "&start=" + start + "&pageSize=" + pageSize;
			var request = new HttpJsonRequest(path, "GET");
			var serializer = new JsonSerializer();
			JToken json;
			using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
				json = (JToken) serializer.Deserialize(reader);

			return new QueryResult
			{
				IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
				Results = json["Results"].Children().Cast<JObject>().ToArray(),
			};
		}

		public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
			throw new NotImplementedException();
		}

		public JArray GetDocuments(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		public JArray GetIndexNames(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		public JArray GetIndexes(int start, int pageSize)
		{
			throw new NotImplementedException();
		}

	    public void Commit(Guid txId)
	    {
	        throw new NotImplementedException();
	    }

	    public void Rollback(Guid txId)
	    {
	        throw new NotImplementedException();
	    }

	    #endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}