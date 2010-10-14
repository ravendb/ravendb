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
using Raven.Database.Extensions;

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

		public void PutAttachment(string key, Guid? etag, byte[] data, JObject metadata)
		{
			// we filter out content length, because getting it wrong will cause errors 
			// in the server side when serving the wrong value for this header.
			// worse, if we are using http compression, this value is known to be wrong
			// instead, we rely on the actual size of the data provided for us
			metadata.Remove("Content-Length");
			database.PutStatic(key, etag, data, metadata);
		}

		public Attachment GetAttachment(string key)
		{
			return database.GetStatic(key);
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			database.DeleteStatic(key, etag);
		}

		public string[] GetIndexNames(int start, int pageSize)
		{
			return database.GetIndexNames(start, pageSize)
				.Select(x => x.Value<string>()).ToArray();
		}

		public void ResetIndex(string name)
		{
			database.ResetIndex(name);
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

		public QueryResult Query(string index, IndexQuery query, string[] ignored)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;

            if (index.StartsWith("dynamic", StringComparison.InvariantCultureIgnoreCase))
            {
                string entityName = null;
                if (index.StartsWith("dynamic/"))
                    entityName = index.Substring("dynamic/".Length);
                return database.ExecuteDynamicQuery(entityName, query);
            }
            else
            {
                return database.Query(index, query);
            }
		}

		public void DeleteIndex(string name)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders; 
			database.DeleteIndex(name);
		}

		public MultiLoadResult Get(string[] ids, string[] includes)
	    {
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
			return new MultiLoadResult
			{
				Results = ids
					.Select(id => database.Get(id, RavenTransactionAccessor.GetTransactionInformation()))
					.Where(document => document != null)
					.Select(x => x.ToJson())
					.ToList()
			};
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

		public void StoreRecoveryInformation(Guid resourceManagerId,Guid txId, byte[] recoveryInformation)
		{
			CurrentRavenOperation.Headers.Value = OperationsHeaders;
            database.PutStatic("transactions/recoveryInformation/" + txId, null, recoveryInformation, new JObject(new JProperty("Resource-Manager-Id", resourceManagerId.ToString())));
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


        /// <summary>
        /// Create a new instance of <see cref="IDatabaseCommands"/> that will interacts
        /// with the specified database
        /// </summary>
        public IDatabaseCommands ForDatabase(string database)
	    {
	        throw new NotSupportedException("Multiple databases are not supported in the embedded API currently");
	    }

	    #endregion

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
