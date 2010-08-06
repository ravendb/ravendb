using System;
using System.Collections.Specialized;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Client.Document;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    public interface IDatabaseCommands
	{
		NameValueCollection OperationsHeaders { get; set; }
		
		JsonDocument Get(string key);
		PutResult Put(string key, Guid? etag, JObject document, JObject metadata);
		void Delete(string key, Guid? etag);

    	void PutAttachment(string key, Guid? etag, byte[] data, JObject metadata);
    	Attachment GetAttachment(string key);
    	void DeleteAttachment(string key, Guid? etag);

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
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale);
	}
}