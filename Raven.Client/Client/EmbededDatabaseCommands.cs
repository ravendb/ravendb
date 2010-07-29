using System;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Storage;

namespace Raven.Client.Client
{
	public class EmbededDatabaseCommands : IDatabaseCommands
	{
		private readonly DocumentDatabase database;
		private readonly DocumentConvention convention;

		public EmbededDatabaseCommands(DocumentDatabase database, DocumentConvention convention)
		{
			this.database = database;
			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
		}

		public DatabaseStatistics Statistics
		{
			get { return database.Statistics; }
		}

		public ITransactionalStorage TransactionalStorage
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

		public NameValueCollection OperationsHeaders { get; set; }

		public JsonDocument Get(string key)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			return database.Get(key, RavenTransactionAccessor.GetTransactionInformation());
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			return database.Put(key, etag, document, metadata, RavenTransactionAccessor.GetTransactionInformation());
		}

	    public void Delete(string key, Guid? etag)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			database.Delete(key, etag, RavenTransactionAccessor.GetTransactionInformation());
		}

		public IndexDefinition GetIndex(string name)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			return database.GetIndexDefinition(name);
		}

		public string PutIndex(string name, IndexDefinition definition)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
		    return PutIndex(name, definition, false);
		}

        public string PutIndex(string name, IndexDefinition definition, bool overwrite)
        {
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			if (overwrite == false && database.IndexStorage.Indexes.Contains(name))
                throw new InvalidOperationException("Cannot put index: " + name + ", index already exists"); 
            return database.PutIndex(name, definition);
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
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			return database.Query(index, query);
		}

		public void DeleteIndex(string name)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.DeleteIndex(name);
		}

        public JsonDocument[] Get(string[] ids)
	    {
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			return ids
				.Select(id => database.Get(id, RavenTransactionAccessor.GetTransactionInformation()))
                .Where(document => document != null)
                .ToArray();
	    }

		public BatchResult[] Batch(ICommandData[] commandDatas)
		{
			foreach (var commandData in commandDatas)
			{
				commandData.TransactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			}
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			return database.Batch(commandDatas);
		}

		public void Commit(Guid txId)
	    {
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			database.Commit(txId);
	    }

	    public void Rollback(Guid txId)
	    {
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.Rollback(txId);
	    }

		public byte[] PromoteTransaction(Guid fromTxId)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			return database.PromoteTransaction(fromTxId);
		}

		public void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.PutStatic("transactions/recoveryInformation/" + txId, null, recoveryInformation, new JObject());
		}

		public IDatabaseCommands With(ICredentials credentialsForSession)
	    {
	        return this;
	    }

		/// <summary>
		/// It seems that we can't promote a transaction inside the same process
		/// </summary>
		public bool SupportsPromotableTransactions
		{
			get { return false; }
		}

		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			var databaseBulkOperations = new DatabaseBulkOperations(database, RavenTransactionAccessor.GetTransactionInformation());
			databaseBulkOperations.DeleteByIndex(indexName, queryToDelete, allowStale);
		}

		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			var databaseBulkOperations = new DatabaseBulkOperations(database, RavenTransactionAccessor.GetTransactionInformation());
			databaseBulkOperations.UpdateByIndex(indexName, queryToUpdate, patchRequests, allowStale);
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
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			return database.GetStatic(name);
		}

		public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.PutStatic(name, etag, data, metadata);
		}

		public void DeleteStatic(string name, Guid? etag)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.DeleteStatic(name, etag);
		}
	}
}