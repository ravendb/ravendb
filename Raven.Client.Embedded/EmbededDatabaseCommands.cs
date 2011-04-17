//-----------------------------------------------------------------------
// <copyright file="EmbededDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Client.Embedded
{
	///<summary>
	/// Expose the set of operations by the RavenDB server
	///</summary>
	public class EmbeddedDatabaseCommands : IDatabaseCommands
	{
		private readonly DocumentDatabase database;
		private readonly DocumentConvention convention;

		///<summary>
		/// Create a new instance
		///</summary>
		public EmbeddedDatabaseCommands(DocumentDatabase database, DocumentConvention convention)
		{
			this.database = database;
			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
		}

		/// <summary>
		/// Access the database statistics
		/// </summary>
		public DatabaseStatistics Statistics
		{
			get { return database.Statistics; }
		}

		/// <summary>
		/// Provide direct access to the database transactional storage
		/// </summary>
		public ITransactionalStorage TransactionalStorage
		{
			get { return database.TransactionalStorage; }
		}


		/// <summary>
		/// Provide direct access to the database index definition storage
		/// </summary>
		public IndexDefinitionStorage IndexDefinitionStorage
		{
			get { return database.IndexDefinitionStorage; }
		}

		/// <summary>
		/// Provide direct access to the database index storage
		/// </summary>
		public IndexStorage IndexStorage
		{
			get { return database.IndexStorage; }
		}

		#region IDatabaseCommands Members

		/// <summary>
		/// Gets or sets the operations headers.
		/// </summary>
		/// <value>The operations headers.</value>
		public NameValueCollection OperationsHeaders { get; set; }

		/// <summary>
		/// Gets documents for the specified key prefix
		/// </summary>
		public JsonDocument[] StartsWith(string keyPrefix, int start, int pageSize)
		{
			var documentsWithIdStartingWith = database.GetDocumentsWithIdStartingWith(keyPrefix, start, pageSize);
			return SerializationHelper.RavenJObjectsToJsonDocuments(documentsWithIdStartingWith.OfType<RavenJObject>()).ToArray();
		}

		/// <summary>
		/// Gets the document for the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public JsonDocument Get(string key)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.Get(key, RavenTransactionAccessor.GetTransactionInformation());
		}

		/// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		public PutResult Put(string key, Guid? etag, RavenJObject document, RavenJObject metadata)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.Put(key, etag, document, metadata, RavenTransactionAccessor.GetTransactionInformation());
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Guid? etag)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.Delete(key, etag, RavenTransactionAccessor.GetTransactionInformation());
		}

		/// <summary>
		/// Puts the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public void PutAttachment(string key, Guid? etag, byte[] data, RavenJObject metadata)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			// we filter out content length, because getting it wrong will cause errors 
			// in the server side when serving the wrong value for this header.
			// worse, if we are using http compression, this value is known to be wrong
			// instead, we rely on the actual size of the data provided for us
			metadata.Remove("Content-Length");
			database.PutStatic(key, etag, data, metadata);
		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.GetStatic(key);
		}

		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void DeleteAttachment(string key, Guid? etag)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.DeleteStatic(key, etag);
		}

		/// <summary>
		/// Gets the index names from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <returns></returns>
		public string[] GetIndexNames(int start, int pageSize)
		{
			pageSize = Math.Min(pageSize, database.Configuration.MaxPageSize);
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.GetIndexNames(start, pageSize)
				.Select(x => x.Value<string>()).ToArray();
		}

		/// <summary>
		/// Resets the specified index
		/// </summary>
		/// <param name="name">The name.</param>
		public void ResetIndex(string name)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.ResetIndex(name);
		}

		/// <summary>
		/// Gets the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		public IndexDefinition GetIndex(string name)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.GetIndexDefinition(name);
		}

		/// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The index def.</param>
		public string PutIndex(string name, IndexDefinition definition)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return PutIndex(name, definition, false);
		}

		/// <summary>
		/// Puts the index for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="definition">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		public string PutIndex(string name, IndexDefinition definition, bool overwrite)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			if (overwrite == false && database.IndexStorage.Indexes.Contains(name))
				throw new InvalidOperationException("Cannot put index: " + name + ", index already exists"); 
			return database.PutIndex(name, definition);
		}

		/// <summary>
		/// Puts the index definition for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <returns></returns>
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention));
		}

		/// <summary>
		/// Puts the index for the specified name
		/// </summary>
		/// <typeparam name="TDocument">The type of the document.</typeparam>
		/// <typeparam name="TReduceResult">The type of the reduce result.</typeparam>
		/// <param name="name">The name.</param>
		/// <param name="indexDef">The index def.</param>
		/// <param name="overwrite">if set to <c>true</c> [overwrite].</param>
		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention), overwrite);
		}

		/// <summary>
		/// Queries the specified index.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <param name="query">The query.</param>
		/// <param name="includes">The includes are ignored for this implementation.</param>
		public QueryResult Query(string index, IndexQuery query, string[] includes)
		{
			query.PageSize = Math.Min(query.PageSize, database.Configuration.MaxPageSize);
			CurrentOperationContext.Headers.Value = OperationsHeaders;

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

		/// <summary>
		/// Deletes the index.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteIndex(string name)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders; 
			database.DeleteIndex(name);
		}

		/// <summary>
		/// Gets the results for the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		/// <param name="includes">The includes.</param>
		/// <returns></returns>
		public MultiLoadResult Get(string[] ids, string[] includes)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return new MultiLoadResult
			{
				Results = ids
					.Select(id => database.Get(id, RavenTransactionAccessor.GetTransactionInformation()))
					.Where(document => document != null)
					.Select(x => x.ToJson())
					.ToList()
			};
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			foreach (var commandData in commandDatas)
			{
				commandData.TransactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			}
			CurrentOperationContext.Headers.Value = OperationsHeaders; 
			return database.Batch(commandDatas);
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Commit(Guid txId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.Commit(txId);
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void Rollback(Guid txId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders; 
			database.Rollback(txId);
		}

		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public byte[] PromoteTransaction(Guid fromTxId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders; 
			return database.PromoteTransaction(fromTxId);
		}

		/// <summary>
		/// Stores the recovery information.
		/// </summary>
		/// <param name="resourceManagerId">The resource manager Id for this transaction</param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId,Guid txId, byte[] recoveryInformation)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var jObject = new RavenJObject {{"Resource-Manager-Id", resourceManagerId.ToString()}};
			database.PutStatic("transactions/recoveryInformation/" + txId, null, recoveryInformation, jObject);
		}

		/// <summary>
		/// Returns a new <see cref="IDatabaseCommands "/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
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

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var databaseBulkOperations = new DatabaseBulkOperations(database, RavenTransactionAccessor.GetTransactionInformation());
			databaseBulkOperations.DeleteByIndex(indexName, queryToDelete, allowStale);
		}

		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
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


		/// <summary>
		/// Create a new instance of <see cref="IDatabaseCommands"/> that will interact
		/// with the root database. Useful if the database has works against a tenant database.
		/// </summary>
		public IDatabaseCommands GetRootDatabase()
		{
			return this;
		}

		/// <summary>
		/// Returns a list of suggestions based on the specified suggestion query.
		/// </summary>
		/// <param name="index">The index to query for suggestions</param>
		/// <param name="suggestionQuery">The suggestion query.</param>
		public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.ExecuteSuggestionQuery(index, suggestionQuery);
		}

		///<summary>
		/// Get the possible terms for the specified field in the index 
		/// You can page through the results by use fromValue parameter as the 
		/// starting point for the next query
		///</summary>
		///<returns></returns>
		public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.ExecuteGetTermsQuery(index, field, fromValue, pageSize);
	 
		}

		#endregion

		/// <summary>
		/// Spin the background worker for indexing
		/// </summary>
		public void SpinBackgroundWorkers()
		{
			database.SpinBackgroundWorkers();
		}
	}
}
