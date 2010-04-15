using System;
using System.Transactions;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using TransactionInformation = Raven.Database.TransactionInformation;

namespace Raven.Client.Client
{
	public class EmbededDatabaseCommands : IDatabaseCommands, IDisposable
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
			return database.Get(key,GetTransactionInformation());
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
            return database.Put(key, etag, document, metadata, GetTransactionInformation());
		}

	    private static TransactionInformation GetTransactionInformation()
	    {
            if (Transaction.Current == null)
                return null;
            return new TransactionInformation
            {
                Id = Transaction.Current.TransactionInformation.DistributedIdentifier,
                Timeout = TransactionManager.DefaultTimeout
            };
	    }

	    public void Delete(string key, Guid? etag)
		{
            database.Delete(key, etag, GetTransactionInformation());
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

		public QueryResult Query(string index, string query, int start, int pageSize)
		{
			return database.Query(index, query, start, pageSize);
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

	    public void Commit(Guid txId)
	    {
	        database.Commit(txId);
	    }

	    public void Rollback(Guid txId)
	    {
	        database.Rollback(txId);
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

		public QueryResult Query(string index, string query, int start, int pageSize, string[] fieldsToFetch)
		{
			return database.Query(index, query, start, pageSize, fieldsToFetch);
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