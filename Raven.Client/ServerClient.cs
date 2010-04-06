using System;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client
{
	public class ServerClient : IDatabaseCommands
	{
		private readonly string url;

		public ServerClient(string localhost, int port)
		{
			url = String.Format("http://{0}:{1}", localhost, port);
		}

		#region IDatabaseCommands Members

		public JsonDocument Get(string key)
		{
			var request = new HttpJsonRequest(url + "/docs/" + key, "GET");
			return new JsonDocument
			{
				Data = Encoding.UTF8.GetBytes(request.ReadResponseString()),
				Key = key,
			};
		}

		public string Put(string key, Guid? etag, JObject document, JObject metadata)
		{
			var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
			var request = new HttpJsonRequest(url + "/docs/" + key, method);
			request.Write(document.ToString());

			var obj = new {id = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.id;
		}

		public void Delete(string key, Guid? etag)
		{
			throw new NotImplementedException();
		}

		public string PutIndex(string name, string indexDef)
		{
			var request = new HttpJsonRequest(url + "/indexes/" + name, "PUT");
			request.Write(indexDef);

			var obj = new {index = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

		public QueryResult Query(string index, string query, int start, int pageSize)
		{
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

		#endregion

        #region IDisposable Members

        public void Dispose()
        {
        }

        #endregion
    }
}