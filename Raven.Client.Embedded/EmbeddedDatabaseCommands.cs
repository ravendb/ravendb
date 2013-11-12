//-----------------------------------------------------------------------
// <copyright file="EmbededDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Client.Changes;
using Raven.Client.Exceptions;
using Raven.Client.Listeners;
using Raven.Database.Data;
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
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
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
		private readonly IDocumentConflictListener[] conflictListeners;
		private readonly ProfilingInformation profilingInformation;
		private bool resolvingConflict;
		private bool resolvingConflictRetries;
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private TransactionInformation TransactionInformation
		{
			get { return convention.EnlistInDistributedTransactions ? RavenTransactionAccessor.GetTransactionInformation() : null; }
		}

		///<summary>
		/// Create a new instance
		///</summary>
		public EmbeddedDatabaseCommands(DocumentDatabase database, DocumentConvention convention, Guid? sessionId, IDocumentConflictListener[] conflictListeners)
		{
			profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
			this.database = database;
			this.convention = convention;
			this.conflictListeners = conflictListeners;
			OperationsHeaders = new NameValueCollection();
			if (database.Configuration.IsSystemDatabase() == false)
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
		/// Admin operations, like create/delete database.
		/// </summary>
		public IAdminDatabaseCommands Admin
		{
			get { throw new NotSupportedException("Multiple databases are not supported in the embedded API currently"); }
		}

		/// <summary>
		/// Admin operations, like create/delete database.
		/// </summary>
		public IGlobalAdminDatabaseCommands GlobalAdmin
		{
			get { throw new NotSupportedException("Multiple databases are not supported in the embedded API currently"); }
		}

		/// <summary>
		/// Gets documents for the specified key prefix
		/// </summary>
		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize, bool metadataOnly = false, string exclude = null)
		{
			pageSize = Math.Min(pageSize, database.Configuration.MaxPageSize);

			// metadata only is NOT supported for embedded, nothing to save on the data transfers, so not supporting 
			// this

			var documentsWithIdStartingWith = database.GetDocumentsWithIdStartingWith(keyPrefix, matches, exclude, start, pageSize);
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

			if (AssertNonConflictedDocumentAndCheckIfNeedToReload(jsonDocument))
			{
				if (resolvingConflictRetries)
					throw new InvalidOperationException("Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");

				resolvingConflictRetries = true;
				try
				{
					return Get(key);
				}
				finally
				{
					resolvingConflictRetries = false;
				}
			}

			return jsonDocument;
		}

		/// <summary>
	    /// Retrieves the document with the specified key and performs the transform operation specified on that document
	    /// </summary>
	    /// <param name="key">The key</param>
	    /// <param name="transformer">The transformer to use</param>
	    /// <param name="queryInputs">Inputs to the transformer</param>
	    /// <returns></returns>
	    public JsonDocument Get(string key, string transformer, Dictionary<string, RavenJToken> queryInputs = null)
	    {
            return database.GetWithTransformer(key, transformer, TransactionInformation, queryInputs);
	    }

	    /// <summary>
		/// Puts the document with the specified key in the database
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		/// <param name="document">The document.</param>
		/// <param name="metadata">The metadata.</param>
		/// <returns></returns>
		public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.Put(key, etag, document, metadata, TransactionInformation);
		}

		/// <summary>
		/// Deletes the document with the specified key.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void Delete(string key, Etag etag)
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
		public void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata)
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
		public void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata)
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

			AssertNonConflictedAttachement(attachment, false);

			return attachment;
		}

        /// <summary>
        /// Gets the attachment by the specified key
        /// </summary>
        /// <returns></returns>
        public AttachmentInformation[] GetAttachments(Etag startEtag, int pageSize)
        {
            CurrentOperationContext.Headers.Value = OperationsHeaders;
            return database.GetAttachments(0, pageSize, startEtag, null, long.MaxValue);
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

			AssertNonConflictedAttachement(attachment, true);

			return attachment;
		}

		/// <summary>
		/// Deletes the attachment with the specified key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="etag">The etag.</param>
		public void DeleteAttachment(string key, Etag etag)
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
		/// Gets the indexes from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public IndexDefinition[] GetIndexes(int start, int pageSize)
		{
			//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
			return database
				.GetIndexes(start, pageSize)
				.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter()))
				.ToArray();
		}

		/// <summary>
		/// Gets the transformers from the server
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		public TransformerDefinition[] GetTransformers(int start, int pageSize)
		{
			return database.GetTransformers(start, pageSize)
				.Select(x =>JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject) x)["definition"].ToString(),new JsonToJsonConverter()))
				.ToArray();

		}

		/// <summary>
		/// Gets the transformer definition for the specified name
		/// </summary>
		/// <param name="name">The name.</param>
		public TransformerDefinition GetTransformer(string name)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.GetTransformerDefinition(name);
		}

		/// <summary>
		/// Deletes the transformer.
		/// </summary>
		/// <param name="name">The name.</param>
		public void DeleteTransformer(string name)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.DeleteTransfom(name);
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
		/// Creates a transformer with the specified name, based on an transformer definition
		/// </summary>
		public string PutTransformer(string name, TransformerDefinition indexDef)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.PutTransform(name, indexDef);
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
			if (overwrite == false && database.IndexStorage.HasIndex(name))
				throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
			return database.PutIndex(name, definition.Clone());
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
            if(query.PageSizeSet)
			    query.PageSize = Math.Min(query.PageSize, database.Configuration.MaxPageSize);

			UpdateQueryFromHeaders(query, OperationsHeaders);
			// metadataOnly is not supported for embedded

			// indexEntriesOnly is not supported for embedded

			QueryResultWithIncludes queryResult;
			if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
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
			}

			var docResults = queryResult.Results.Concat(queryResult.Includes);
			return RetryOperationBecauseOfConflict(docResults, queryResult,
			                                       () => Query(index, query, includes, metadataOnly, indexEntriesOnly));
		}

		private void UpdateQueryFromHeaders(IndexQuery query, NameValueCollection headers)
		{
			query.SortHints = new Dictionary<string, SortOptions>();

			foreach (var header in headers.AllKeys.Where(key => key.StartsWith("SortHint-")))
			{
				var value = headers[header];
				if (string.IsNullOrEmpty(value))
					continue;
				SortOptions sort;
				Enum.TryParse(value, true, out sort);

				var key = header;

				if(DateTime.Now > new DateTime(2013,11,30))
					throw new Exception("This is an ugly code that was supposed to be fixed by this time");
				if (sort == SortOptions.Long && key.EndsWith("_Range"))
				{
					key = key.Substring(0, key.Length - "_Range".Length);
				}

				query.SortHints.Add(key, sort);
			}
		}

		/// <summary>
		/// Queries the specified index in the Raven flavored Lucene query syntax. Will return *all* results, regardless
		/// of the number of items that might be returned.
		/// </summary>
		public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query, out QueryHeaderInformation queryHeaderInfo)
		{
			if (query.PageSizeSet == false)
				query.PageSize = int.MaxValue;
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var items = new BlockingCollection<RavenJObject>(1024);
			using (var waitForHeaders = new ManualResetEventSlim(false))
			{
				QueryHeaderInformation localQueryHeaderInfo = null;
				var task = Task.Factory.StartNew(() =>
				{
					bool setWaitHandle = true;
					try
					{
						// we may be sending a LOT of documents to the user, and most 
						// of them aren't going to be relevant for other ops, so we are going to skip
						// the cache for that, to avoid filling it up very quickly
						using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
						{
							database.TransactionalStorage.Batch(accessor =>
							{
								using (var op = new DocumentDatabase.DatabaseQueryOperation(database, index, query, accessor))
								{
									op.Init();
									localQueryHeaderInfo = op.Header;
									waitForHeaders.Set();
									setWaitHandle = false;
									op.Execute(items.Add);
								}	
							});
							
						}
					}
					catch (Exception e)
					{
						if (setWaitHandle)
							waitForHeaders.Set();

						if (index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase) &&
							e is IndexDoesNotExistsException)
						{
							throw new InvalidOperationException(@"StreamQuery() does not support querying dynamic indexes. It is designed to be used with large data-sets and is unlikely to return all data-set after 15 sec of indexing, like Query() does.",e);
						}

						throw;
					}
				}, TaskCreationOptions.LongRunning);
				waitForHeaders.Wait();
				queryHeaderInfo = localQueryHeaderInfo;
				return new DisposableEnumerator<RavenJObject>(YieldUntilDone(items, task), items.Dispose);
			}
		}

		/// <summary>
		/// Streams the documents by etag OR starts with the prefix and match the matches
		/// Will return *all* results, regardless of the number of itmes that might be returned.
		/// </summary>
        public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag, string startsWith, string matches, int start, int pageSize, string exclude)
		{
			if(fromEtag != null && startsWith != null)
				throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

			var items = new BlockingCollection<RavenJObject>(1024);
			var task = Task.Factory.StartNew(() =>
			{
				// we may be sending a LOT of documents to the user, and most 
				// of them aren't going to be relevant for other ops, so we are going to skip
				// the cache for that, to avoid filling it up very quickly
				using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
				{
					try
					{
						if (string.IsNullOrEmpty(startsWith))
						{
							database.GetDocuments(start, pageSize, fromEtag,
							                      items.Add);
						}
						else
						{
							database.GetDocumentsWithIdStartingWith(
								startsWith,
								matches,
                                exclude,
								start,
								pageSize,
								items.Add);
						}
					}
					catch (ObjectDisposedException)
					{
					}
				}
			});
			return new DisposableEnumerator<RavenJObject>(YieldUntilDone(items, task), items.Dispose);
		}

		private IEnumerator<RavenJObject> YieldUntilDone(BlockingCollection<RavenJObject> items, Task task)
		{
			try
			{
				task.ContinueWith(_ => items.Add(null));
				while (true)
				{
					var ravenJObject = items.Take();
					if (ravenJObject == null)
						break;
					yield return ravenJObject;
				}
			}
			finally
			{
				try
				{
					task.Wait();
				}
				catch (AggregateException ae)
				{
//log all exceptions, so no errror information gets lost
					ae.Handle(e =>
					{
						logger.Error("{0}",e);					
						return true;
					});

					if (ae.InnerExceptions.Count > 0)
						throw ae.InnerExceptions.First();
				}
				catch (ObjectDisposedException)
				{
				}
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
	    /// <param name="transformer"></param>
	    /// <param name="queryInputs"></param>
	    /// <param name="metadataOnly">Load just the document metadata</param>
	    /// <returns></returns>
	    public MultiLoadResult Get(string[] ids, string[] includes, string transformer = null, Dictionary<string, RavenJToken> queryInputs = null, bool metadataOnly = false)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;

			// metadata only is not supported for embedded

			var multiLoadResult = new MultiLoadResult
			                      {
				                      Results = ids
					                      .Select(id =>
					                      {
					                          if (string.IsNullOrEmpty(transformer))
					                              return database.Get(id, TransactionInformation);
                                              return database.GetWithTransformer(id, transformer, TransactionInformation,  queryInputs);
					                      })
					                      .ToArray()
					                      .Select(x => x == null ? null : x.ToJson())
					                      .ToList(),
			                      };

			if (includes != null)
			{
				var includeCmd = new AddIncludesCommand(database, TransactionInformation, (etag, doc) => multiLoadResult.Includes.Add(doc), includes,
				                                        new HashSet<string>(ids));
				foreach (var jsonDocument in multiLoadResult.Results)
				{
					includeCmd.Execute(jsonDocument);
				}
			}

			var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

		    return RetryOperationBecauseOfConflict(docResults, multiLoadResult,
		                                           () => Get(ids, includes, transformer, queryInputs, metadataOnly));
		}

		/// <summary>
		/// Begins an async get operation for documents
		/// </summary>
		/// <param name="start">Paging start</param>
		/// <param name="pageSize">Size of the page.</param>
		/// <param name="metadataOnly">Load just the document metadata</param>
		/// <remarks>
		/// This is primarily useful for administration of a database
		/// </remarks>
		public JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false)
		{
			// As this is embedded we don't care for the metadata only value
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database
				.GetDocuments(start, pageSize, null)
				.Cast<RavenJObject>()
				.ToJsonDocuments()
				.ToArray();
		}

        /// <summary>
        /// Begins an async get operation for documents
        /// </summary>
        /// <param name="fromEtag">The ETag of the first document to start with</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <param name="metadataOnly">Load just the document metadata</param>
        /// <remarks>
        /// This is primarily useful for administration of a database
        /// </remarks>
        public JsonDocument[] GetDocuments(Etag fromEtag, int pageSize, bool metadataOnly = false)
        {
            // As this is embedded we don't care for the metadata only value
            CurrentOperationContext.Headers.Value = OperationsHeaders;
            return database
                .GetDocuments(0, pageSize, fromEtag)
                .Cast<RavenJObject>()
                .ToJsonDocuments()
                .ToArray();
        }

		/// <summary>
		/// Executed the specified commands as a single batch
		/// </summary>
		/// <param name="commandDatas">The command data.</param>
		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var batchResults = database.Batch(commandDatas.Select(cmd =>
			{
				cmd.TransactionInformation = TransactionInformation;
				return cmd;
			}).ToList());
			if (batchResults != null)
			{
				foreach (var batchResult in batchResults.Where(batchResult => batchResult != null && batchResult.Metadata != null && batchResult.Metadata.IsSnapshot))
				{
					batchResult.Metadata = (RavenJObject) batchResult.Metadata.CreateSnapshot();
				}
			}
			return batchResults;
		}

	    /// <summary>
	    /// Commits the specified tx id.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    public void Commit(string txId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.Commit(txId);
		}

	    /// <summary>
	    /// Rollbacks the specified tx id.
	    /// </summary>
	    /// <param name="txId">The tx id.</param>
	    public void Rollback(string txId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.Rollback(txId);
		}

		/// <summary>
		/// Prepares the transaction on the server.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public void PrepareTransaction(string txId)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			database.PrepareTransaction(txId);
		}

		/// <summary>
		/// Gets the build number
		/// </summary>
		public BuildNumber GetBuildNumber()
		{
		    return new BuildNumber
		    {
		        BuildVersion = DocumentDatabase.BuildVersion,
		        ProductVersion = DocumentDatabase.ProductVersion
		    };
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
		public IDisposable ForceReadFromMaster()
		{
			// nothing to do, there is no replication for embedded 
			return null;
		}

		/// <summary>
		/// Get the low level  bulk insert operation
		/// </summary>
		public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
		{
			return new EmbeddedBulkInsertOperation(database, options, changes);
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests)
		{
			return UpdateByIndex(indexName, queryToUpdate, patchRequests, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
		{
			return UpdateByIndex(indexName, queryToUpdate, patch, false);
		}

		/// <summary>
		/// Perform a set based update using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patchRequests">The patch requests.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var databaseBulkOperations = new DatabaseBulkOperations(database, TransactionInformation);
			var state = databaseBulkOperations.UpdateByIndex(indexName, queryToUpdate, patchRequests, allowStale);
			return new Operation(0, state);
		}

		/// <summary>
		/// Perform a set based update using the specified index
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToUpdate">The query to update.</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation while the index is stale.</param>
		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var databaseBulkOperations = new DatabaseBulkOperations(database, TransactionInformation);
			var state = databaseBulkOperations.UpdateByIndex(indexName, queryToUpdate, patch, allowStale);
			return new Operation(0, state);
		}

		/// <summary>
		/// Perform a set based deletes using the specified index, not allowing the operation
		/// if the index is stale
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete)
		{
			return DeleteByIndex(indexName, queryToDelete, false);
		}

		/// <summary>
		/// Perform a set based deletes using the specified index.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="queryToDelete">The query to delete.</param>
		/// <param name="allowStale">if set to <c>true</c> allow the operation even if the index is stale. Otherwise, not allowing the operation.</param>
		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			var databaseBulkOperations = new DatabaseBulkOperations(database, TransactionInformation);
			var state = databaseBulkOperations.DeleteByIndex(indexName, queryToDelete, allowStale);
			return new Operation(0, state);
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
		public IDatabaseCommands ForSystemDatabase()
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
		/// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
		/// </summary>
		/// <param name="index">Name of the index</param>
		/// <param name="query">Query to build facet results</param>
		/// <param name="facetSetupDoc">Name of the FacetSetup document</param>
		/// <param name="start">Start index for paging</param>
		/// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
		public FacetResults GetFacets( string index, IndexQuery query, string facetSetupDoc, int start = 0, int? pageSize = null ) {
			CurrentOperationContext.Headers.Value = OperationsHeaders;
			return database.ExecuteGetTermsQuery( index, query, facetSetupDoc, start, pageSize );
		}

        /// <summary>
        /// Using the given Index, calculate the facets as per the specified doc with the given start and pageSize
        /// </summary>
        /// <param name="index">Name of the index</param>
        /// <param name="query">Query to build facet results</param>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start = 0, int? pageSize = null)
        {
            CurrentOperationContext.Headers.Value = OperationsHeaders;
            return database.ExecuteGetTermsQuery(index, query, facets, start, pageSize);
        }

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		public RavenJObject Patch(string key, PatchRequest[] patches)
		{
			return Patch(key, patches, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			var batchResults = Batch(new[]
			{
				new PatchCommandData
				{
					Key = key,
					Patches = patches
				}
			});
			if (!ignoreMissing && batchResults[0].PatchResult != null && batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch)
		{
			return Patch(key, patch, null);
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="ignoreMissing">true if the patch request should ignore a missing document, false to throw DocumentDoesNotExistException</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			var batchResults = Batch(new[]
				  {
					  new ScriptedPatchCommandData
					  {
						  Key = key,
						  Patch = patch
					  }
				  });
			if (!ignoreMissing && batchResults[0].PatchResult != null && batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
				throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patches">Array of patch requests</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)
		{
			var batchResults = Batch(new[]
			{
				new PatchCommandData
				{
					Key = key, Patches = patches, Etag = etag
				}
			});

			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchesToExisting">Array of patch requests to apply to an existing document</param>
		/// <param name="patchesToDefault">Array of patch requests to apply to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault, RavenJObject defaultMetadata)
		{
			var batchResults = Batch(new[]
			{
				new PatchCommandData
				{
					Key = key,
					Patches = patchesToExisting,
					PatchesIfMissing = patchesToDefault,
					Metadata = defaultMetadata
				}
			});

			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document, ignoring the document's Etag
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patch">The patch request to use (using JavaScript)</param>
		/// <param name="etag">Require specific Etag [null to ignore]</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)
		{
			var batchResults = Batch(new[]
			      {
				      new ScriptedPatchCommandData
				      {
					      Key = key,
					      Patch = patch,
					      Etag = etag
				      }
			      });
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Sends a patch request for a specific document which may or may not currently exist
		/// </summary>
		/// <param name="key">Id of the document to patch</param>
		/// <param name="patchExisting">The patch request to use (using JavaScript) to an existing document</param>
		/// <param name="patchDefault">The patch request to use (using JavaScript)  to a default document when the document is missing</param>
		/// <param name="defaultMetadata">The metadata for the default document when the document is missing</param>
		public RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata)
		{
			var batchResults = Batch(new[]
				  {
					  new ScriptedPatchCommandData
					  {
						  Key = key,
						  Patch = patchExisting,
						  PatchIfMissing = patchDefault,
						  Metadata = defaultMetadata
					  }
				  });
			return batchResults[0].AdditionalData;
		}

		/// <summary>
		/// Disable all caching within the given scope
		/// </summary>
		public IDisposable DisableAllCaching()
		{
			// nothing to do here, embedded doesn't support caching
			return new DisposableAction(() =>
			{
			});
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
		/// Seeds the next identity value on the server
		/// </summary>
		public long SeedIdentityFor(string name, long value)
		{
			database.TransactionalStorage.Batch(accessor => accessor.General.SetIdentityValue(name, value));
			return value;
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
			var metadata = database.GetDocumentMetadata(key, TransactionInformation);

			AssertNonConflictedDocumentForHead(metadata);

			return metadata;
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

		private T RetryOperationBecauseOfConflict<T>(IEnumerable<RavenJObject> docResults, T currentResult, Func<T> nextTry)
		{
			bool requiresRetry = docResults.Aggregate(false, (current, docResult) => current | AssertNonConflictedDocumentAndCheckIfNeedToReload(docResult));
			if (!requiresRetry)
				return currentResult;

			if (resolvingConflictRetries)
				throw new InvalidOperationException(
					"Encountered another conflict after already resolving a conflict. Conflict resultion cannot recurse.");
			resolvingConflictRetries = true;
			try
			{
				return nextTry();
			}
			finally
			{
				resolvingConflictRetries = false;
			}
		}

		private void AssertNonConflictedAttachement(Attachment attachment, bool headRequest)
		{
			if (attachment == null)
				return;

			if (attachment.Metadata == null)
				return;

			if (attachment.Metadata.Value<int>("@Http-Status-Code") == 409)
			{
				if (headRequest)
				{
					throw new ConflictException("Conflict detected on " + attachment.Key +
												", conflict must be resolved before the attachment will be accessible", true)
					{
						Etag = attachment.Etag
					};
				}

				var conflictsDoc = RavenJObject.Load(new BsonReader(attachment.Data()));
				var conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

				throw new ConflictException("Conflict detected on " + attachment.Key +
											", conflict must be resolved before the attachement will be accessible", true)
				{
					Etag = attachment.Etag,
					ConflictedVersionIds = conflictIds
				};
			}
		}

		private void AssertNonConflictedDocumentForHead(JsonDocumentMetadata metadata)
		{
			if (metadata == null)
				return;

			if (metadata.Metadata.Value<int>("@Http-Status-Code") == 409)
			{
				throw new ConflictException("Conflict detected on " + metadata.Key +
												", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict", true)
				{
					Etag = metadata.Etag
				};
			}
		}

		private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(JsonDocument jsonDocument)
		{
			if (jsonDocument == null)
				return false;

			if (jsonDocument.DataAsJson == null)
				return false;

			if (jsonDocument.Metadata == null)
				return false;

			if (jsonDocument.Metadata.Value<int>("@Http-Status-Code") == 409)
			{
				var conflictException = TryResolveConflictOrCreateConflictException(jsonDocument.Key, jsonDocument.DataAsJson,
				                                                                    jsonDocument.Etag);
				if (conflictException == null)
					return true;
				throw conflictException;
			}
			return false;
		}

		private bool AssertNonConflictedDocumentAndCheckIfNeedToReload(RavenJObject docResult)
		{
			if (docResult == null)
				return false;
			var metadata = docResult[Constants.Metadata];
			if (metadata == null)
				return false;

			if (metadata.Value<int>("@Http-Status-Code") == 409)
			{
				var concurrencyException = TryResolveConflictOrCreateConflictException(metadata.Value<string>("@id"), docResult, metadata.Value<string>("@etag"));
				if (concurrencyException == null)
					return true;
				throw concurrencyException;
			}
			return false;
		}

		private ConflictException TryResolveConflictOrCreateConflictException(string key, RavenJObject conflictsDoc, Etag etag)
		{
			var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
			if (ravenJArray == null)
				throw new InvalidOperationException("Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

			var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();

			if (TryResolveConflictByUsingRegisteredListeners(key, etag, conflictIds))
				return null;

			return new ConflictException("Conflict detected on " + key +
										", conflict must be resolved before the document will be accessible", true)
			{
				ConflictedVersionIds = conflictIds,
				Etag = etag
			};
		}

		internal bool TryResolveConflictByUsingRegisteredListeners(string key, Etag etag, string[] conflictIds)
		{
			if (conflictListeners.Length > 0 && resolvingConflict == false)
			{
				resolvingConflict = true;
				try
				{
					var multiLoadResult = Get(conflictIds, null);

					var results = multiLoadResult.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

					foreach (var conflictListener in conflictListeners)
					{
						JsonDocument resolvedDocument;
						if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
						{
							Put(key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata);

							return true;
						}
					}
				}
				finally
				{
					resolvingConflict = false;
				}
			}

			return false;
		}
	}
}
