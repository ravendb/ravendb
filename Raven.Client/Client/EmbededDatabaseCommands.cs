using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Storage;

namespace Raven.Client.Client
{
	public class EmbededDatabaseCommands : IDatabaseCommands
	{
		private readonly DocumentDatabase database;

		public EmbededDatabaseCommands(DocumentDatabase database)
		{
			this.database = database;
		}

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

		#region IDatabaseCommands Members

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
			var indexDefJson = JObject.Parse(indexDef);
			var reduceDef = indexDefJson.Property("Reduce") != null
				? indexDefJson.Property("Reduce").Value.Value<string>()
				: null;
			return database.PutIndex(name, indexDefJson.Property("Map").Value.Value<string>(),
			                         reduceDef);
		}

		public QueryResult Query(string index, IndexQuery query)
		{
			return database.Query(index, query);
		}

		public void DeleteIndex(string name)
		{
			database.DeleteIndex(name);
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

		#endregion

		public void Dispose()
		{
			database.Dispose();
		}

		public void SpinBackgroundWorkers()
		{
			database.SpinBackgroundWorkers();
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
	}
}