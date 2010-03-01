using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Client
{
    public class DocumentStore : IDisposable
    {
        private readonly string localhost;
        private readonly int port;

        public DocumentStore(string localhost, int port) : this()
        {
            this.localhost = localhost;
            this.port = port;
        }

        public DocumentStore()
        {
            Conventions = new DocumentConvention();
        }

        public string Database { get; set; }

        public DocumentConvention Conventions { get; set; }

        public DocumentSession OpenSession()
        {
            return new DocumentSession(this, database);
        }

        public void Dispose()
        {
            var embeddedDatabase = (DocumentDatabase)database;
            if (embeddedDatabase != null)
                embeddedDatabase.Dispose();
        }

        public void Initialise()
        {
            if (String.IsNullOrEmpty(localhost))
            {
                var embeddedDatabase = new DocumentDatabase(Database);
                embeddedDatabase.SpinBackgroundWorkers();
                database = embeddedDatabase;
            }
            else
            {
                database = new ServerClient(localhost, port);
            }

            database.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        private IDatabaseCommands database;

        public void Delete(Guid id)
        {
            database.Delete(id.ToString());
        }
    }

    public class ServerClient : IDatabaseCommands
    {
        private string url;

        public ServerClient(string localhost, int port)
        {
            url = String.Format("http://{0}:{1}", localhost, port);
        }

        public byte[] Get(string key)
        {
            throw new NotImplementedException();
        }

        public string Put(JObject document)
        {
            var request = WebRequest.Create(url + "/docs");
            request.Method = "POST";
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
                var id = new Json.JsonDynamicObject(text)["id"];
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
            //throw new NotImplementedException();
            return "";
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