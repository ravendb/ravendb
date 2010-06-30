using System;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Client.Document;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    public interface IDatabaseCommands : IDisposable
	{
		JsonDocument Get(string key);
		PutResult Put(string key, Guid? etag, JObject document, JObject metadata);
		void Delete(string key, Guid? etag);

    	IndexDefinition GetIndex(string name);
		string PutIndex(string name, IndexDefinition indexDef);
		string PutIndex<TDocument,TReduceResult>(string name, IndexDefinition<TDocument,TReduceResult> indexDef);

        string PutIndex(string name, IndexDefinition indexDef, bool overwrite);
        string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite);
		
        QueryResult Query(string index, IndexQuery query);
		void DeleteIndex(string name);
        JsonDocument[] Get(string[] ids);

		BatchResult[] Batch(ICommandData[] commandDatas);

        void Commit(Guid txId);
        void Rollback(Guid txId);
    	byte[] PromoteTransaction(Guid fromTxId);
		void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation);
		
		IDatabaseCommands With(ICredentials credentialsForSession);
    	bool SupportsPromotableTransactions { get;  }

    	void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale);
		void UpdateByIndex(string indexName, IndexQuery queryToDelete, PatchRequest[] patchRequests, bool allowStale);
	}
}