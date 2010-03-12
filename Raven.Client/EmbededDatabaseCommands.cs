using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Storage;

namespace Raven.Client
{
    public class EmbededDatabaseCommands : IDatabaseCommands
    {
        private readonly DocumentDatabase database;

        public DatabaseStatistics Statistics
        {
            get { return database.Statistics; }
        }

        public TransactionalStorage TransactionalStorage
        {
            get { return database.TransactionalStorage; }
        }

        public IndexDefinitionStorage IndexDefinitionStorage
        {
            get { return database.IndexDefinitionStorage; }
        }

        public IndexStorage IndexStorage
        {
            get { return database.IndexStorage; }
        }

        public void Dispose()
        {
            database.Dispose();
        }

        public void SpinBackgroundWorkers()
        {
            database.SpinBackgroundWorkers();
        }

        public JsonDocument Get(string key)
        {
            return database.Get(key);
        }

        public string Put(string key, Guid? etag, JObject document, JObject metadata)
        {
            return database.Put(key, etag, document, metadata);
        }

        public void Delete(string key, Guid? etag)
        {
            database.Delete(key, etag);
        }

        public string PutIndex(string name, string indexDef)
        {
            return database.PutIndex(name, indexDef);
        }

        public QueryResult Query(string index, string query, int start, int pageSize)
        {
            return database.Query(index, query, start, pageSize);
        }

        public QueryResult Query(string index, string query, int start, int pageSize, string[] fieldsToFetch)
        {
            return database.Query(index, query, start, pageSize, fieldsToFetch);
        }

        public void DeleteIndex(string name)
        {
            database.DeleteIndex(name);
        }

        public Attachment GetStatic(string name)
        {
            return database.GetStatic(name);
        }

        public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
        {
            database.PutStatic(name, etag, data, metadata);
        }

        public void DeleteStatic(string name, Guid? etag)
        {
            database.DeleteStatic(name, etag);
        }

        public JArray GetDocuments(int start, int pageSize)
        {
            return database.GetDocuments(start, pageSize);
        }

        public JArray GetIndexNames(int start, int pageSize)
        {
            return database.GetIndexNames(start, pageSize);
        }

        public JArray GetIndexes(int start, int pageSize)
        {
            return database.GetIndexes(start, pageSize);
        }

        public EmbededDatabaseCommands(DocumentDatabase database)
        {
            this.database = database;
        }
    }
}