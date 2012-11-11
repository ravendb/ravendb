//-----------------------------------------------------------------------
// <copyright file="EmbededDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using Raven.Database.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Queries;
using Raven.Database.Server;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;
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
		private readonly ProfilingInformation profilingInformation;
		private TransactionInformation TransactionInformation
		{
			get { return convention.EnlistInDistributedTransactions ? RavenTransactionAccessor.GetTransactionInformation() : null; }
		}

		///<summary>
		/// Create a new instance
		///</summary>
		public EmbeddedDatabaseCommands(DocumentDatabase database, DocumentConvention convention, Guid? sessionId)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.database = database;
			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
			if(database.Configuration.IsSystemDatabase() == false)
				throw new InvalidOperationException("Database must be a system database");
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
		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize, bool metadataOnly = false)
		{
			pageSize = Math.Min(pageSize, database.Configuration.MaxPageSize);

			// metadata only is NOT supported for embedded, nothing to save on the data transfers, so not supporting 
			// this

			var documentsWithIdStartingWith = database.GetDocumentsWithIdStartingWith(keyPrefix, matches, start, pageSize);
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
			var jsonDocument = database.Get(key, TransactionInformation);
			return EnsureLocalDate(jsonDocument);
		}

		private JsonDocument EnsureLocalDate(JsonDocument jsonDocument)
		{
			if (jsonDocument == null)
				return null;
			if (jsonDocument.LastModified != null)
				jsonDocument.LastModified = jsonDocument.LastModified.Value.ToLocalTime();
			return jsonDocument;
		}

		private JsonDocumentMetadata EnsureLocalDate(JsonDocumentMetadata jsonDocumentMetadata)
		{
			if (jsonDocumentMetadata == null)
				return null;
			if (jsonDocumentMetadata.LastModified != null)
				jsonDocumentMetadata.LastModified = jsonDocumentMetadata.LastModified.Value.ToLocalTime();
			return jsonDocumentMetadata;
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
			return database.Put(key, etag, document, metadata, TransactionInformation);
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Guid? etag)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.Delete(key, etag, TransactionInformation);
		}

		/// <summary>
		/// Puts the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="data">The data.</param>
		/// <param name="metadata">The metadata.</param>
		public void PutAttachment(string key, Guid? etag, Stream data, RavenJObject metadata)
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
		/// Updates just the attachment with the specified key's metadata
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="metadata">The metadata.</param>
		public void UpdateAttachmentMetadata(string key, Guid? etag, RavenJObject metadata)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			// we filter out content length, because getting it wrong will cause errors 
			// in the server side when serving the wrong value for this header.
			// worse, if we are using http compression, this value is known to be wrong
			// instead, we rely on the actual size of the data provided for us
			metadata.Remove("Content-Length");
			database.PutStatic(key, etag, null, metadata);
		}

		/// <summary>
		/// Gets the attachment by the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment GetAttachment(string key)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			Attachment attachment = database.GetStatic(key);
			if (attachment == null)
				return null;
			Func<Stream> data = attachment.Data;
			attachment.Data = () =>
			{
				var memoryStream = new MemoryStream();
				database.TransactionalStorage.Batch(accessor => data().CopyTo(memoryStream));
				memoryStream.Position = 0;
				return memoryStream;
			};
			return attachment;
		}

		/// <summary>
		/// Get the attachment information for the attachments with the same idprefix
		/// </summary>
		public IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize)
		{
			pageSize = Math.Min(pageSize, database.Configuration.MaxPageSize);

			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.GetStaticsStartingWith(idPrefix, start, pageSize)
				.Select(x => new Attachment
				{
					Etag = x.Etag,
					Metadata = x.Metadata,
					Size = x.Size,
					Key = x.Key,
					Data = () =>
					{
						throw new InvalidOperationException("Cannot get attachment data from an attachment header");
					}
				});
		}

		/// <summary>
		/// Retrieves the attachment metadata with the specified key, not the actual attachmet
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns></returns>
		public Attachment HeadAttachment(string key)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			Attachment attachment = database.GetStatic(key);
			if (attachment == null)
				return null;
			attachment.Data = () =>
			{
				throw new InvalidOperationException("Cannot get attachment data from an attachment header");
			};
			return attachment;
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
		/// Get tenant database names (Server/Client mode only)
		/// </summary>
		/// <returns></returns>
		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
			throw new InvalidOperationException("Embedded mode does not support multi-tenancy");
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
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <param name="indexEntriesOnly">Include index entries</param>
		public QueryResult Query(string index, IndexQuery query, string[] includes, bool metadataOnly = false, bool indexEntriesOnly = false)
		{
			query.PageSize = Math.Min(query.PageSize, database.Configuration.MaxPageSize);
			CurrentOperationContext.Headers.Value = OperationsHeaders;

			// metadataOnly is not supported for embedded

			// indexEntriesOnly is not supported for embedded

			QueryResultWithIncludes queryResult;
			if (index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase) || index.Equals("dynamic", StringComparison.InvariantCultureIgnoreCase))
			{
				string entityName = null;
				if (index.StartsWith("dynamic/"))
					entityName = index.Substring("dynamic/".Length);
				queryResult = database.ExecuteDynamicQuery(entityName, query.Clone());
			}
			else
			{
				queryResult = database.Query(index, query.Clone());
			}
			EnsureLocalDate(queryResult.Results);

			var loadedIds = new HashSet<string>(
					queryResult.Results
						.Where(x => x["@metadata"] != null)
						.Select(x => x["@metadata"].Value<string>("@id"))
						.Where(x => x != null)
					); 

			if (includes != null)
			{
			var includeCmd = new AddIncludesCommand(database, TransactionInformation,
			                                        (etag, doc) => queryResult.Includes.Add(doc), includes, loadedIds);

			foreach (var result in queryResult.Results)
			{
				includeCmd.Execute(result);
			}

			includeCmd.AlsoInclude(queryResult.IdsToInclude);

			EnsureLocalDate(queryResult.Includes);
			}

			return queryResult;
		}

		private static void EnsureLocalDate(List<RavenJObject> docs)
		{
			foreach (var doc in docs)
			{
				RavenJToken metadata;
				if (doc.TryGetValue(Constants.Metadata, out metadata) == false || metadata.Type != JTokenType.Object)
					continue;
				var lastModified = metadata.Value<DateTime?>(Constants.LastModified);
				if (lastModified == null || lastModified.Value.Kind == DateTimeKind.Local)
					continue;

				((RavenJObject)metadata)[Constants.LastModified] = lastModified.Value.ToLocalTime();
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
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <returns></returns>
		public MultiLoadResult Get(string[] ids, string[] includes, bool metadataOnly = false)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;

			// metadata only is not supported for embedded

			var multiLoadResult = new MultiLoadResult
			{
				Results = ids
					.Select(id => database.Get(id, TransactionInformation))
					.ToArray()
					.Select(x => x == null ? null : EnsureLocalDate(x).ToJson())
					.ToList(),
			};

			if (includes != null)
			{
			var includeCmd = new AddIncludesCommand(database, TransactionInformation, (etag, doc) => multiLoadResult.Includes.Add(doc), includes, new HashSet<string>(ids));
			foreach (var jsonDocument in multiLoadResult.Results)
			{
				includeCmd.Execute(jsonDocument);
			}
			}

			return multiLoadResult;
		}

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders; 
			return database.Batch(commandDatas.Select(cmd=>
			{
				cmd.TransactionInformation = TransactionInformation;
				return cmd;
			}));
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
		/// Returns a new <see cref="IDatabaseCommands"/> using the specified credentials
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		/// <returns></returns>
		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return this;
		}

		/// <summary>
		/// Force the database commands to read directly from the master, unless there has been a failover.
		/// </summary>
		public void ForceReadFromMaster()
		{
			// nothing to do, there is no replication for embedded 
		}

		/// <summary>
		/// It seems that we can't promote a transaction inside the same process
		/// </summary>
		public bool SupportsPromotableTransactions
		{
			get { return false; }
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)
		{
			UpdateByIndex(indexName, queryToUpdate, patchRequests, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
		{
			UpdateByIndex(indexName, queryToUpdate, patch, false);
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
			var databaseBulkOperations = new DatabaseBulkOperations(database, TransactionInformation);
			databaseBulkOperations.UpdateByIndex(indexName, queryToUpdate, patchRequests, allowStale);
		}

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> [allow stale].</param>
		public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var databaseBulkOperations = new DatabaseBulkOperations(database, RavenTransactionAccessor.GetTransactionInformation());
			databaseBulkOperations.UpdateByIndex(indexName, queryToUpdate, patch, allowStale);
		}

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		public void DeleteByIndex(string indexName, IndexQuery queryToDelete)
		{
			DeleteByIndex(indexName, queryToDelete, false);
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
			var databaseBulkOperations = new DatabaseBulkOperations(database, TransactionInformation);
			databaseBulkOperations.DeleteByIndex(indexName, queryToDelete, allowStale);
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
		public IDatabaseCommands ForDefaultDatabase()
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

		/// <summary>
		/// Return a list of documents that based on the MoreLikeThisQuery.
		/// </summary>
		/// <param name="query">The more like this query parameters</param>
		/// <returns></returns>
		public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var result = database.ExecuteMoreLikeThisQuery(query, TransactionInformation);
			return result.Result;
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

	    /// <summary>
	    /// Using the given Index, calculate the facets as per the specified doc
	    /// </summary>
	    /// <param name="index"></param>
	    /// <param name="query"></param>
	    /// <param name="facetSetupDoc"></param>
	    /// <returns></returns>
		public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.ExecuteGetTermsQuery(index, query, facetSetupDoc);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public void Patch(string key, PatchRequest[] patches)
		{
			Patch(key, patches, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public void Patch(string key, ScriptedPatchRequest patch)
		{
			Patch(key, patch, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public void Patch(string key, PatchRequest[] patches, Guid? etag)
		{
			Batch(new[]
					{
						new PatchCommandData
							{
								Key = key,
								Patches = patches,
								Etag = etag
							}
					});
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public void Patch(string key, ScriptedPatchRequest patch, Guid? etag)
		{
			Batch(new[]
					{
						new ScriptedPatchCommandData 
								{ 
									Key = key,  
									Patch = patch,
									Etag = etag
								}
					});
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			// nothing to do here, embedded doesn't support caching
			return new DisposableAction(() => { });
		}

		/// <summary>
		/// Retrieve the statistics for the database
		/// </summary>
		public DatabaseStatistics GetStatistics()
		{
			return database.Statistics;
		}

		/// <summary>
		/// Generate the next identity value from the server
		/// </summary>
		public long NextIdentityFor(string name)
		{
			long nextIdentityValue = -1;
			database.TransactionalStorage.Batch(accessor =>
			{
				nextIdentityValue = accessor.General.GetNextIdentityValue(name);
			});
			return nextIdentityValue;
		}

		/// <summary>
		/// Get the full URL for the given document key. This is not supported for embedded database.
		/// </summary>
		public string UrlFor(string documentKey)
		{
			throw new NotSupportedException("Could not get url for embedded database");
		}

		/// <summary>
		/// Retrieves the document metadata for the specified document key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <returns>
		/// The document metadata for the specified document, or null if the document does not exist
		/// </returns>
		public JsonDocumentMetadata Head(string key)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var jsonDocumentMetadata = database.GetDocumentMetadata(key, TransactionInformation);
			return EnsureLocalDate(jsonDocumentMetadata);
		}

		/// <summary>
		/// Perform a single POST request containing multiple nested GET requests
		/// </summary>
		public GetResponse[] MultiGet(GetRequest[] requests)
		{
			throw new NotSupportedException("Multi GET is only support for Server/Client, not embedded");
		}

		#endregion

		/// <summary>
		/// Spin the background worker for indexing
		/// </summary>
		public void SpinBackgroundWorkers()
		{
			database.SpinBackgroundWorkers();
		}

		/// <summary>
		/// The profiling information
		/// </summary>
		public ProfilingInformation ProfilingInformation
		{
			get { return profilingInformation; }
		}
	}
}
