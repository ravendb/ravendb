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

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		PutResult Put(string key, Guid? etag, JObject document, JObject metadata);
		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		void Delete(string key, Guid? etag);

		/// <summary>
		/// Puts the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
    	void PutAttachment(string key, Guid? etag, byte[] data, JObject metadata);
		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
    	Attachment GetAttachment(string key);
		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
    	void DeleteAttachment(string key, Guid? etag);

		/// <summary>
		/// Gets the index names from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
    	string[] GetIndexNames(int start, int pageSize);

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
    	void ResetIndex(string name);
		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
    	IndexDefinition GetIndex(string name);
		/// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		string PutIndex(string name, IndexDefinition indexDef);
		/// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <returns></returns>
		string PutIndex<TDocument,TReduceResult>(string name, IndexDefinition<TDocument,TReduceResult> indexDef);

		/// <summary>
		/// Puts the index for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        string PutIndex(string name, IndexDefinition indexDef, bool overwrite);
		/// <summary>
		/// Puts the index for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
        string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite);

		/// <summary>
		/// Queries the specified index.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes.</param>
        QueryResult Query(string index, IndexQuery query, string [] includes);
		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		void DeleteIndex(string name);

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command datas.</param>
		BatchResult[] Batch(ICommandData[] commandDatas);

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
        void Commit(Guid txId);
		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
        void Rollback(Guid txId);
		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
    	byte[] PromoteTransaction(Guid fromTxId);
		/// <summary>
		/// Stores the recovery information.
		/// </summary>
        /// <param name="resourceManagerId">The resource manager Id for this transaction</param>
        /// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
        void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation);

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		IDatabaseCommands With(ICredentials credentialsForSession);
		/// <summary>
		/// Gets a value indicating whether [supports promotable transactions].
		/// </summary>
		/// <value>
		/// 	<c>true</c> if [supports promotable transactions]; otherwise, <c>false</c>.
		/// </value>
    	bool SupportsPromotableTransactions { get;  }

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
    	void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale);
		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale);
	}
}