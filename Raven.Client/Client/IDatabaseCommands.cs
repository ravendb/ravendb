using System;
using System.Collections.Specialized;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    ///<summary>
    /// Expose the set of operations by the RavenDB server
    ///</summary>
    public interface IDatabaseCommands
	{
		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		NameValueCollection OperationsHeaders { get; set; }

		/// <summary>
		/// Gets the docuent for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		JsonDocument Get(string key);
		/// <summary>
		/// Gets the results for the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		MultiLoadResult Get(string[] ids, string[] includes);

		PutResult Put(string key, Guid? etag, JObject document, JObject metadata);
		void Delete(string key, Guid? etag);

    	void PutAttachment(string key, Guid? etag, byte[] data, JObject metadata);
    	Attachment GetAttachment(string key);
    	void DeleteAttachment(string key, Guid? etag);

    	string[] GetIndexNames(int start, int pageSize);

    	void ResetIndex(string name);
    	IndexDefinition GetIndex(string name);
		string PutIndex(string name, IndexDefinition indexDef);
		string PutIndex<TDocument,TReduceResult>(string name, IndexDefinition<TDocument,TReduceResult> indexDef);

        string PutIndex(string name, IndexDefinition indexDef, bool overwrite);
        string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite);
		
        QueryResult Query(string index, IndexQuery query, string [] includes);
		void DeleteIndex(string name);

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