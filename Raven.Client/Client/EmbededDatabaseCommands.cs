using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using TransactionInformation = Raven.Database.TransactionInformation;

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

		public string PutIndex(string name, IndexDefinition definition)
		{
			return database.PutIndex(name, definition);
		}

		public QueryResult Query(string index, IndexQuery query)
		{
			return database.Query(index, query);
		}

		public void DeleteIndex(string name)
		{
			database.DeleteIndex(name);
		}

        public JsonDocument[] Get(string[] ids)
	    {
            return ids
                .Select(id => database.Get(id, GetTransactionInformation()))
                .Where(document => document != null)
                .ToArray();
	    }

		public BatchResult[] Batch(ICommandData[] commandDatas)
		{
			foreach (var commandData in commandDatas)
			{
				commandData.TransactionInformation = GetTransactionInformation();
			}
			return database.Batch(commandDatas);
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