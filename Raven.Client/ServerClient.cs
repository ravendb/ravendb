using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Json;

namespace Raven.Client
{
    public class ServerClient : IDatabaseCommands
    {
        private readonly string url;

        public ServerClient(string localhost, int port)
        {
            url = String.Format("http://{0}:{1}", localhost, port);
        }

        public JsonDocument Get(string key)
        {
            var request = WebRequest.Create(url + "/docs/" + key);
            request.Method = "GET";
            request.ContentType = "application/json";
            var response = request.GetResponse();
            using (var responseString = response.GetResponseStream())
            {
                var reader = new StreamReader(responseString);
                var text = reader.ReadToEnd();
                reader.Close();

                return new JsonDocument
                {
                    Data = Encoding.UTF8.GetBytes(text),
                    Key = key,
                };
            }
        }

        public string Put(string key, JObject document, JObject metadata)
        {
            var request = WebRequest.Create(url + "/docs/" + key);

            request.Method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
            request.ContentType = "application/json";

            using (var dataStream = request.GetRequestStream())
            {
                var byteArray = Encoding.UTF8.GetBytes(document.ToString());
                dataStream.Write(byteArray, 0, byteArray.Length);
                dataStream.Close();
            }

            var response = request.GetResponse();
            using (var responseString = response.GetResponseStream())
            {
                var reader = new StreamReader(responseString);
                var text = reader.ReadToEnd();
                var id = new JsonDynamicObject(text)["id"];
                reader.Close();
                return id.ToString();
            }
        }

        public void Delete(string key)
        {
            throw new NotImplementedException();
        }

        public string PutIndex(string name, string indexDef)
        {
            var request = WebRequest.Create(url + "/indexes/" + name);

            request.Method = "PUT";
            request.ContentType = "application/json";

            using (var dataStream = request.GetRequestStream())
            {
                var byteArray = Encoding.UTF8.GetBytes(indexDef);
                dataStream.Write(byteArray, 0, byteArray.Length);
                dataStream.Close();
            }

            var response = request.GetResponse();
            using (var responseString = response.GetResponseStream())
            {
                var reader = new StreamReader(responseString);
                var text = reader.ReadToEnd();
                var id = new JsonDynamicObject(text)["index"];
                reader.Close();
                return id.ToString();
            }
        }

        public QueryResult Query(string index, string query, int start, int pageSize)
        {
            throw new NotImplementedException();
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
    }
}