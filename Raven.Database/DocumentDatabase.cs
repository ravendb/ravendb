//-----------------------------------------------------------------------
// <copyright file="DocumentDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using log4net;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Backup;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Http;
using Raven.Http.Exceptions;
using Constants = Raven.Abstractions.Data.Constants;
using Raven.Json.Linq;
using Index = Raven.Database.Indexing.Index;
using Task = Raven.Database.Tasks.Task;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Database
{
	public class DocumentDatabase : IResourceStore, IUuidGenerator
	{
		[ImportMany]
		public OrderedPartCollection<AbstractAttachmentPutTrigger> AttachmentPutTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractAttachmentDeleteTrigger> AttachmentDeleteTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractAttachmentReadTrigger> AttachmentReadTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractPutTrigger> PutTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractDeleteTrigger> DeleteTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractDynamicCompilationExtension> Extensions { get; set; }

		private readonly WorkContext workContext;

		/// <summary>
		/// This is used to hold state associated with this instance by external extensions
		/// </summary>
		public ConcurrentDictionary<object, object> ExtensionsState { get; private set; }


		private readonly ThreadLocal<bool> disableAllTriggers = new ThreadLocal<bool>(() => false);
        private System.Threading.Tasks.Task indexingBackgroundTask;
        private System.Threading.Tasks.Task reducingBackgroundTask;
		private System.Threading.Tasks.Task tasksBackgroundTask;
	    private readonly TaskScheduler backgroundTaskScheduler;

		private readonly ILog log = LogManager.GetLogger(typeof(DocumentDatabase));

		private long currentEtagBase;

		public DocumentDatabase(InMemoryRavenConfiguration configuration)
		{
			ExternalState = new ConcurrentDictionary<string, object>();

			if (configuration.BackgroundTasksPriority != ThreadPriority.Normal)
			{
				backgroundTaskScheduler = new TaskSchedulerWithCustomPriority(
					// we need a minimum of four task threads - one for indexing dispatch, one for reducing dispatch, one for tasks, one for indexing/reducing ops
					Math.Max(4, configuration.MaxNumberOfParallelIndexTasks + 2),
					configuration.BackgroundTasksPriority);
			}
			else
			{
				backgroundTaskScheduler = TaskScheduler.Current;
			}

			ExtensionsState = new ConcurrentDictionary<object, object>();
			Configuration = configuration;

			configuration.Container.SatisfyImportsOnce(this);

			workContext = new WorkContext
			{
				IndexUpdateTriggers = IndexUpdateTriggers,
				ReadTriggers = ReadTriggers
			};

			TransactionalStorage = configuration.CreateTransactionalStorage(workContext.HandleWorkNotifications);
			configuration.Container.SatisfyImportsOnce(TransactionalStorage);

			try
			{
				TransactionalStorage.Initialize(this);
			}
			catch (Exception)
			{
				TransactionalStorage.Dispose();
				throw;
			}

			TransactionalStorage.Batch(actions => currentEtagBase = actions.General.GetNextIdentityValue("Raven/Etag"));

			IndexDefinitionStorage = new IndexDefinitionStorage(
				configuration,
				TransactionalStorage,
				configuration.DataDirectory,
				configuration.Container.GetExportedValues<AbstractViewGenerator>(),
				Extensions);
			IndexStorage = new IndexStorage(IndexDefinitionStorage, configuration);

			workContext.Configuration = configuration;
			workContext.IndexStorage = IndexStorage;
			workContext.TransactionaStorage = TransactionalStorage;
			workContext.IndexDefinitionStorage = IndexDefinitionStorage;


			try
			{
				InitializeTriggers();
				ExecuteStartupTasks();
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}
		}

		private void InitializeTriggers()
		{
			PutTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			DeleteTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			ReadTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

			AttachmentPutTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			AttachmentDeleteTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			AttachmentReadTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

			IndexUpdateTriggers
				.Init(disableAllTriggers)
				.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
		}

		private void ExecuteStartupTasks()
		{
			foreach (var task in Configuration.Container.GetExportedValues<IStartupTask>())
			{
				task.Execute(this);
			}
		}

		public DatabaseStatistics Statistics
		{
			get
			{
				var result = new DatabaseStatistics
				{
					CountOfIndexes = IndexStorage.Indexes.Length,
					Errors = workContext.Errors,
					Triggers = PutTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Put" })
								.Concat(DeleteTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Delete" }))
								.Concat(ReadTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Read" }))
								.Concat(IndexUpdateTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Index Update" }))
								.ToArray()
				};

				TransactionalStorage.Batch(actions =>
				{
					result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
					result.CountOfDocuments = actions.Documents.GetDocumentsCount();
					result.StaleIndexes = IndexStorage.Indexes
						.Where(s =>
						{
							string entityName = null;
							var abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(s);
							if (abstractViewGenerator != null)
								entityName = abstractViewGenerator.ForEntityName;

							return actions.Staleness.IsIndexStale(s, null, entityName);
						}).ToArray();
					result.Indexes = actions.Indexing.GetIndexesStats().ToArray();
				});
				return result;
			}
		}

		IRaveHttpnConfiguration IResourceStore.Configuration
		{
			get { return Configuration; }
		}

		public ConcurrentDictionary<string, object> ExternalState { get; set; }

		public InMemoryRavenConfiguration Configuration
		{
			get;
			private set;
		}

		public ITransactionalStorage TransactionalStorage { get; private set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

		public IndexStorage IndexStorage { get; private set; }

		#region IDisposable Members

		public void Dispose()
		{
			workContext.StopWork();
			foreach (var value in ExtensionsState.Values.OfType<IDisposable>())
			{
				value.Dispose();
			}
			TransactionalStorage.Dispose();
			IndexStorage.Dispose();

			if (tasksBackgroundTask != null)
				tasksBackgroundTask.Wait(); 
			if (indexingBackgroundTask != null)
				indexingBackgroundTask.Wait();
            if (reducingBackgroundTask != null)
                reducingBackgroundTask.Wait();

			var disposable = backgroundTaskScheduler as IDisposable;
			if (disposable != null)
				disposable.Dispose();
		}

		public void StopBackgroundWokers()
		{
			workContext.StopWork();
			tasksBackgroundTask.Wait();
			indexingBackgroundTask.Wait();
		    reducingBackgroundTask.Wait();
		}

		public WorkContext WorkContext
		{
			get { return workContext; }
		}

		#endregion

		public void SpinBackgroundWorkers()
		{
			workContext.StartWork();
            indexingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
		        new IndexingExecuter(TransactionalStorage, workContext, backgroundTaskScheduler).Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
            reducingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
                new ReducingExecuter(TransactionalStorage, workContext, backgroundTaskScheduler).Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
            tasksBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
                new TasksExecuter(TransactionalStorage, workContext).Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
		}

		private long sequentialUuidCounter;

		public Guid CreateSequentialUuid()
		{
			var ticksAsBytes = BitConverter.GetBytes(currentEtagBase);
			Array.Reverse(ticksAsBytes);
			var increment = Interlocked.Increment(ref sequentialUuidCounter);
			var currentAsBytes = BitConverter.GetBytes(increment);
			Array.Reverse(currentAsBytes);
			var bytes = new byte[16];
			Array.Copy(ticksAsBytes, 0, bytes, 0, ticksAsBytes.Length);
			Array.Copy(currentAsBytes, 0, bytes, 8, currentAsBytes.Length);
			return bytes.TransfromToGuidWithProperSorting();
		}


		public JsonDocument Get(string key, TransactionInformation transactionInformation)
		{
			JsonDocument document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.Documents.DocumentByKey(key, transactionInformation);
			});

			DocumentRetriever.EnsureIdInMetadata(document);
			return new DocumentRetriever(null, ReadTriggers)
				.ExecuteReadTriggers(document, transactionInformation,ReadOperation.Load);
		}

		public JsonDocumentMetadata GetDocumentMetadata(string key, TransactionInformation transactionInformation)
		{
			JsonDocumentMetadata document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.Documents.DocumentMetadataByKey(key, transactionInformation);
			});

			DocumentRetriever.EnsureIdInMetadata(document);
			return new DocumentRetriever(null, ReadTriggers)
				.ProcessReadVetoes(document, transactionInformation, ReadOperation.Load);
		}

        public PutResult Put(string key, Guid? etag, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key != null && Encoding.Unicode.GetByteCount(key) >= 255)
				throw new ArgumentException("The key must be a maximum of 255 bytes in unicode, 127 characters", "key");

            log.DebugFormat("Putting a document with key: {0} and etag {1}", key, etag);

			if (string.IsNullOrEmpty(key))
			{
				// we no longer sort by the key, so it doesn't matter
				// that the key is no longer sequential
				key = Guid.NewGuid().ToString();
			}
			RemoveReservedProperties(document);
			RemoveReservedProperties(metadata);
			Guid newEtag = Guid.Empty;
			lock (this)
			{
				TransactionalStorage.Batch(actions =>
				{
					if (key.EndsWith("/"))
					{
						key += actions.General.GetNextIdentityValue(key);
					}
					if (transactionInformation == null)
					{
						AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
						PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, transactionInformation));

						newEtag = actions.Documents.AddDocument(key, etag, document, metadata);
						// We detect this by using the etags
						// AddIndexingTask(actions, metadata, () => new IndexDocumentsTask { Keys = new[] { key } });
						PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, newEtag, transactionInformation));
					}
					else
					{
						newEtag = actions.Transactions.AddDocumentInTransaction(key, etag,
						                                                        document, metadata, transactionInformation);
					}
					workContext.ShouldNotifyAboutWork();
				});
			}
			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSyncronization(() => PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata, newEtag)));

			return new PutResult
			{
				Key = key,
				ETag = newEtag
			};
		}

		private void AddIndexingTask(IStorageActionsAccessor actions, RavenJToken metadata, Func<Task> taskGenerator)
		{
			foreach (var indexName in IndexDefinitionStorage.IndexNames)
			{
				var viewGenerator = IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					continue;
				var entityName = metadata.Value<string>(Constants.RavenEntityName);
				if (viewGenerator.ForEntityName != null &&
						viewGenerator.ForEntityName != entityName)
					continue;
				var task = taskGenerator();
				task.Index = indexName;
				actions.Tasks.AddTask(task, DateTime.UtcNow);
			}
		}

		private void AssertPutOperationNotVetoed(string key, RavenJObject metadata, RavenJObject document, TransactionInformation transactionInformation)
		{
			var vetoResult = PutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, document, metadata, transactionInformation) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertAttachmentPutOperationNotVetoed(string key, RavenJObject metadata, byte[] data)
		{
			var vetoResult = AttachmentPutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, data, metadata) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertAttachmentDeleteOperationNotVetoed(string key)
		{
			var vetoResult = AttachmentDeleteTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("DELETE vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertDeleteOperationNotVetoed(string key, TransactionInformation transactionInformation)
		{
			var vetoResult = DeleteTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key, transactionInformation) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("DELETE vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

        private static void RemoveReservedProperties(RavenJObject document)
		{
			var toRemove = new HashSet<string>();
			foreach (var propertyName in document.Keys.Where(propertyName => propertyName.StartsWith("@")))
			{
			    toRemove.Add(propertyName);
			}
			foreach (var propertyName in toRemove)
			{
				document.Remove(propertyName);
			}
		}

		public void Delete(string key, Guid? etag, TransactionInformation transactionInformation)
		{
            log.DebugFormat("Delete a document with key: {0} and etag {1}", key, etag);
			TransactionalStorage.Batch(actions =>
			{
				if (transactionInformation == null)
				{
					AssertDeleteOperationNotVetoed(key, transactionInformation);

					DeleteTriggers.Apply(trigger => trigger.OnDelete(key, transactionInformation));

					RavenJObject metadata;
					if (actions.Documents.DeleteDocument(key, etag, out metadata))
					{
						AddIndexingTask(actions, metadata, () => new RemoveFromIndexTask { Keys = new[] { key } });
						DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, transactionInformation));
					}
				}
				else
				{
					actions.Transactions.DeleteDocumentInTransaction(transactionInformation, key, etag);
				}
				workContext.ShouldNotifyAboutWork();
			});
			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSyncronization(() => DeleteTriggers.Apply(trigger => trigger.AfterCommit(key)));
		}

		public void Commit(Guid txId)
		{
			try
			{
				TransactionalStorage.Batch(actions =>
				{
					actions.Transactions.CompleteTransaction(txId, doc =>
					{
						// doc.Etag - represent the _modified_ document etag, and we already
						// checked etags on previous PUT/DELETE, so we don't pass it here
						if (doc.Delete)
							Delete(doc.Key, null, null);
						else
							Put(doc.Key, null,
								doc.Data,
								doc.Metadata, null);
					});
					actions.Attachments.DeleteAttachment("transactions/recoveryInformation/" + txId, null);
					workContext.ShouldNotifyAboutWork();
				});
			}
			catch (Exception e)
			{
				if (TransactionalStorage.HandleException(e))
					return;
				throw;
			}
		}

		public void Rollback(Guid txId)
		{
			try
			{
				TransactionalStorage.Batch(actions =>
				{
					actions.Transactions.RollbackTransaction(txId);
					actions.Attachments.DeleteAttachment("transactions/recoveryInformation/" + txId, null);
					workContext.ShouldNotifyAboutWork();
				});
			}
			catch (Exception e)
			{
				if (TransactionalStorage.HandleException(e))
					return;

				throw;
			}
		}

		public string PutIndex(string name, IndexDefinition definition)
		{
			definition.Name = name = IndexDefinitionStorage.FixupIndexName(name);
			switch (IndexDefinitionStorage.FindIndexCreationOptions(definition))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					// ensure that the code can compile
					new DynamicViewCompiler(name, definition, Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Configuration).GenerateInstance();
					DeleteIndex(name);
					break;
			}
			IndexDefinitionStorage.AddIndex(definition);
			IndexStorage.CreateIndexImplementation(definition);
		    TransactionalStorage.Batch(actions =>
		    {
		        actions.Indexing.AddIndex(name, definition.IsMapReduce);
		        workContext.ShouldNotifyAboutWork();
		    });
			return name;
		}

	    public QueryResult Query(string index, IndexQuery query)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			var list = new List<RavenJObject>();
			var stale = false;
			Tuple<DateTime, Guid> indexTimestamp = null;
			TransactionalStorage.Batch(
				actions =>
				{
					string entityName = null;


					var viewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
					if (viewGenerator == null)
						throw new InvalidOperationException("Could not find index named: " + index);

					entityName = viewGenerator.ForEntityName;

					stale = actions.Staleness.IsIndexStale(index, query.Cutoff, entityName);
					indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index);
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					var docRetriever = new DocumentRetriever(actions, ReadTriggers);
					var indexDefinition = GetIndexDefinition(index);
					var fieldsToFetch = new FieldsToFetch(query.FieldsToFetch, query.AggregationOperation,
					                                      viewGenerator.ReduceDefinition == null
					                                      	? Constants.DocumentIdFieldName
					                                      	: Constants.ReduceKeyFieldName);
					var collection = from queryResult in IndexStorage.Query(index, query, result => docRetriever.ShouldIncludeResultInQuery(result, indexDefinition, fieldsToFetch), fieldsToFetch)
									 select docRetriever.RetrieveDocumentForQuery(queryResult, indexDefinition, fieldsToFetch)
										 into doc
										 where doc != null
										 select doc;

					var transformerErrors = new List<string>();
					IEnumerable<RavenJObject> results;
					if (query.SkipTransformResults == false && 
						query.PageSize > 0 && // maybe they just want the stats?
						viewGenerator.TransformResultsDefinition != null)
					{
						var dynamicJsonObjects = collection.Select(x => new DynamicJsonObject(x.ToJson())).ToArray();
						var robustEnumerator = new RobustEnumerator(dynamicJsonObjects.Length)
						{
							OnError =
								(exception, o) =>
								transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o),
														 exception.Message))
						};
						results =
							robustEnumerator.RobustEnumeration(
								dynamicJsonObjects,
								source => viewGenerator.TransformResultsDefinition(docRetriever, source))
								.Select(JsonExtensions.ToJObject);
					}
					else
					{
						results = collection.Select(x => x.ToJson());
					}

					if (query.PageSize > 0) // maybe they just want the query stats?
						list.AddRange(results);

					if (transformerErrors.Count > 0)
					{
						throw new InvalidOperationException("The transform results function failed.\r\n" + string.Join("\r\n", transformerErrors));
					}

				});
			return new QueryResult
			{
				IndexName = index,
				Results = list,
				IsStale = stale,
				SkippedResults = query.SkippedResults.Value,
				TotalResults = query.TotalSize.Value,
				IndexTimestamp = indexTimestamp.Item1,
				IndexEtag = indexTimestamp.Item2
			};
		}

		public IEnumerable<string> QueryDocumentIds(string index, IndexQuery query, out bool stale)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			bool isStale = false;
			HashSet<string> loadedIds = null;
			TransactionalStorage.Batch(
				actions =>
				{
					isStale = actions.Staleness.IsIndexStale(index, query.Cutoff, null);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index)
;
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					loadedIds = new HashSet<string>(from queryResult in IndexStorage.Query(index, query, result => true, new FieldsToFetch(null, AggregationOperation.None, Raven.Abstractions.Data.Constants.DocumentIdFieldName))
													select queryResult.Key);
				});
			stale = isStale;
			return loadedIds;
		}

		public void DeleteIndex(string name)
		{
			name = IndexDefinitionStorage.FixupIndexName(name);
			IndexDefinitionStorage.RemoveIndex(name);
			IndexStorage.DeleteIndex(name);
			//we may run into a conflict when trying to delete if the index is currently
			//busy indexing documents, worst case scenario, we will have an orphaned index
			//row which will get cleaned up on next db restart.
			for (var i = 0; i < 10; i++)
			{
				try
				{
					TransactionalStorage.Batch(action =>
					{
						action.Indexing.DeleteIndex(name);

						workContext.ShouldNotifyAboutWork();
					});
					return;
				}
				catch (ConcurrencyException)
				{
					Thread.Sleep(100);
				}
			}
		}

		public Attachment GetStatic(string name)
		{
			Attachment attachment = null;
			TransactionalStorage.Batch(actions =>
			{
				attachment = actions.Attachments.GetAttachment(name);

				attachment = ProcessAttachmentReadVetoes(name, attachment);

				ExecuteAttachmentReadTriggers(name, attachment);
			});
			return attachment;
		}

		private Attachment ProcessAttachmentReadVetoes(string name, Attachment attachment)
		{
			if (attachment == null)
				return attachment;

			var foundResult = false;
			foreach (var attachmentReadTriggerLazy in AttachmentReadTriggers)
			{
				if (foundResult)
					break;
				var attachmentReadTrigger = attachmentReadTriggerLazy.Value;
				var readVetoResult = attachmentReadTrigger.AllowRead(name, attachment.Data, attachment.Metadata,
																	 ReadOperation.Load);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						attachment.Data = new byte[0];
						attachment.Metadata = new RavenJObject
						                      	{
						                      		{
						                      			"Raven-Read-Veto",
						                      			new RavenJObject
						                      				{
						                      					{"Reason", readVetoResult.Reason},
						                      					{"Trigger", attachmentReadTrigger.ToString()}
						                      				}
						                      			}
						                      	};
						foundResult = true;
						break;
					case ReadVetoResult.ReadAllow.Ignore:
						attachment = null;
						foundResult = true;
						break;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}
			return attachment;
		}

		private void ExecuteAttachmentReadTriggers(string name, Attachment attachment)
		{
			if (attachment == null)
				return;

			foreach (var attachmentReadTrigger in AttachmentReadTriggers)
			{
				attachment.Data = attachmentReadTrigger.Value.OnRead(name, attachment.Data, attachment.Metadata, ReadOperation.Load);
			}
		}

		public void PutStatic(string name, Guid? etag, byte[] data, RavenJObject metadata)
		{
			if (name == null) throw new ArgumentNullException("name");
			if (Encoding.Unicode.GetByteCount(name) >= 255)
				throw new ArgumentException("The key must be a maximum of 255 bytes in unicode, 127 characters", "name");

			Guid newEtag = Guid.Empty;
			TransactionalStorage.Batch(actions =>
			{
				AssertAttachmentPutOperationNotVetoed(name, metadata, data);

				AttachmentPutTriggers.Apply(trigger => trigger.OnPut(name, data, metadata));

				newEtag = actions.Attachments.AddAttachment(name, etag, data, metadata);

				AttachmentPutTriggers.Apply(trigger => trigger.AfterPut(name, data, metadata, newEtag));

				workContext.ShouldNotifyAboutWork();
			});

			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSyncronization(() => AttachmentPutTriggers.Apply(trigger => trigger.AfterCommit(name, data, metadata, newEtag)));

		}

		public void DeleteStatic(string name, Guid? etag)
		{
			TransactionalStorage.Batch(actions =>
			{
				AssertAttachmentDeleteOperationNotVetoed(name);

				AttachmentDeleteTriggers.Apply(x => x.OnDelete(name));

				actions.Attachments.DeleteAttachment(name, etag);

				AttachmentDeleteTriggers.Apply(x => x.AfterDelete(name));

				workContext.ShouldNotifyAboutWork();
			});

			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSyncronization(
					() => AttachmentDeleteTriggers.Apply(trigger => trigger.AfterCommit(name)));

		}

		public RavenJArray GetDocumentsWithIdStartingWith(string idPrefix, int start, int pageSize)
		{
			var list = new RavenJArray();
			TransactionalStorage.Batch(actions =>
			{
				var documents = actions.Documents.GetDocumentsWithIdStartingWith(idPrefix, start)
					.Take(pageSize);
				var documentRetriever = new DocumentRetriever(actions, ReadTriggers);
				foreach (var doc in documents)
				{
					DocumentRetriever.EnsureIdInMetadata(doc);
					var document = documentRetriever
						.ExecuteReadTriggers(doc, null, ReadOperation.Load);
					if (document == null)
						continue;

					list.Add(document.ToJson());
				}
			});
			return list;
		}

		public RavenJArray GetDocuments(int start, int pageSize, Guid? etag)
		{
			var list = new RavenJArray();
			TransactionalStorage.Batch(actions =>
			{
				IEnumerable<JsonDocument> documents;
				if (etag == null)
					documents = actions.Documents.GetDocumentsByReverseUpdateOrder(start);
				else
					documents = actions.Documents.GetDocumentsAfter(etag.Value);
				var documentRetriever = new DocumentRetriever(actions, ReadTriggers);
				foreach (var doc in documents.Take(pageSize))
				{
					DocumentRetriever.EnsureIdInMetadata(doc);
					var document = documentRetriever
						.ExecuteReadTriggers(doc, null, ReadOperation.Load);
					if (document == null)
						continue;

					list.Add(document.ToJson());
				}
			});
			return list;
		}

		public AttachmentInformation[] GetAttachments(int start, int pageSize, Guid? etag)
		{
			AttachmentInformation[] documents = null;

			TransactionalStorage.Batch(actions =>
			{
				if (etag == null)
					documents = actions.Attachments.GetAttachmentsByReverseUpdateOrder(start).Take(pageSize).ToArray();
				else
					documents = actions.Attachments.GetAttachmentsAfter(etag.Value).Take(pageSize).ToArray();

			});
			return documents;
		}

		public RavenJArray GetIndexNames(int start, int pageSize)
		{
			return new RavenJArray(
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(s => new RavenJValue(s))
				);
		}

		public RavenJArray GetIndexes(int start, int pageSize)
		{
			return new RavenJArray(
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(
						indexName => new RavenJObject
							{
								{"name", new RavenJValue(indexName) },
								{"definition", RavenJObject.FromObject(IndexDefinitionStorage.GetIndexDefinition(indexName))}
							}));
		}

		public PatchResult ApplyPatch(string docId, Guid? etag, PatchRequest[] patchDoc, TransactionInformation transactionInformation)
		{
			var result = PatchResult.Patched;
			TransactionalStorage.Batch(actions =>
			{
				var doc = actions.Documents.DocumentByKey(docId, transactionInformation);
				if (doc == null)
				{
					result = PatchResult.DocumentDoesNotExists;
				}
				else if (etag != null && doc.Etag != etag.Value)
				{
					Debug.Assert(doc.Etag != null);
					throw new ConcurrencyException("Could not patch document '" + docId + "' because non current etag was used")
					{
						ActualETag = doc.Etag.Value,
						ExpectedETag = etag.Value,
					};
				}
				else
				{
					var jsonDoc = doc.ToJson();
					new JsonPatcher(jsonDoc).Apply(patchDoc);
					Put(doc.Key, doc.Etag, jsonDoc, jsonDoc.Value<RavenJObject>("@metadata"), transactionInformation);
					result = PatchResult.Patched;
				}

				workContext.ShouldNotifyAboutWork();
			});

			return result;
		}

		public BatchResult[] Batch(IEnumerable<ICommandData> commands)
		{
			var results = new List<BatchResult>();

			var commandDatas = commands.ToArray();
			var shouldLock = commandDatas.Any(x=>x is PutCommandData);

			if(shouldLock)
				Monitor.Enter(this);
			try
			{
				log.DebugFormat("Executing batched commands in a single transaction");
				TransactionalStorage.Batch(actions =>
				{
					foreach (var command in commandDatas)
					{
						command.Execute(this);
						results.Add(new BatchResult
						{
							Method = command.Method,
							Key = command.Key,
							Etag = command.Etag,
							Metadata = command.Metadata
						});
					}
					workContext.ShouldNotifyAboutWork();
				});
				log.DebugFormat("Successfully executed {0} commands", results.Count);
			}
			finally
			{
				if(shouldLock)
					Monitor.Exit(this);
			}
			return results.ToArray();
		}

		public bool HasTasks
		{
			get
			{
				bool hasTasks = false;
				TransactionalStorage.Batch(actions =>
				{
					hasTasks = actions.Tasks.HasTasks;
				});
				return hasTasks;
			}
		}

		public long ApproximateTaskCount
		{
			get
			{
				long approximateTaskCount = 0;
				TransactionalStorage.Batch(actions =>
				{
					approximateTaskCount = actions.Tasks.ApproximateTaskCount;
				});
				return approximateTaskCount;
			}
		}

		public void StartBackup(string backupDestinationDirectory)
		{
			var document = Get(BackupStatus.RavenBackupStatusDocumentKey, null);
			if (document != null)
			{
				var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning)
				{
					throw new InvalidOperationException("Backup is already running");
				}
			}
			Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
			{
				Started = DateTime.UtcNow,
				IsRunning = true,
			}), new RavenJObject(), null);
			IndexStorage.FlushAllIndexes();
			TransactionalStorage.StartBackupOperation(this, backupDestinationDirectory);
		}

		public static void Restore(RavenConfiguration configuration, string backupLocation, string databaseLocation)
		{
			using (var transactionalStorage = configuration.CreateTransactionalStorage(() => { }))
			{
				transactionalStorage.Restore(backupLocation, databaseLocation);
			}
		}

		public byte[] PromoteTransaction(Guid fromTxId)
		{
			var committableTransaction = new CommittableTransaction();
			var transmitterPropagationToken = TransactionInterop.GetTransmitterPropagationToken(committableTransaction);
			TransactionalStorage.Batch(
				actions =>
					actions.Transactions.ModifyTransactionId(fromTxId, committableTransaction.TransactionInformation.DistributedIdentifier,
												TransactionManager.DefaultTimeout));
			return transmitterPropagationToken;
		}

		public void ResetIndex(string index)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
			if (indexDefinition == null)
				throw new InvalidOperationException("There is no index named: " + index);
			IndexStorage.DeleteIndex(index);
			IndexStorage.CreateIndexImplementation(indexDefinition);
			TransactionalStorage.Batch(actions =>
			{
				actions.Indexing.DeleteIndex(index);
			    actions.Indexing.AddIndex(index, indexDefinition.IsMapReduce);
			    workContext.ShouldNotifyAboutWork();
			});
		}

		public IndexDefinition GetIndexDefinition(string index)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			return IndexDefinitionStorage.GetIndexDefinition(index);
		}

		static string buildVersion;
		public static string BuildVersion
		{
			get
			{
				if (buildVersion == null)
					buildVersion = FileVersionInfo.GetVersionInfo(typeof(DocumentDatabase).Assembly.Location).FilePrivatePart.ToString();
				return buildVersion;
			}
		}

		static string productVersion;

		public static string ProductVersion
		{
			get
			{
				if (productVersion == null)
					productVersion = FileVersionInfo.GetVersionInfo(typeof(DocumentDatabase).Assembly.Location).ProductVersion.ToString();
				return productVersion;
			}
		}

		public string[] GetIndexFields(string index)
		{
			var abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
			if(abstractViewGenerator == null)
				return new string[0];
			return abstractViewGenerator.Fields;
		}

		/// <summary>
		/// This API is provided solely for the use of bundles that might need to run
		/// without any other bundle interfering. Specifically, the replication bundle
		/// need to be able to run without interference from any other bundle.
		/// </summary>
		/// <returns></returns>
		public IDisposable DisableAllTriggersForCurrentThread()
		{
			var old = disableAllTriggers.Value;
			disableAllTriggers.Value = true;
			return new DisposableAction(() => disableAllTriggers.Value = old);
		}

		/// <summary>
		/// Get the total size taken by the database on the disk.
		/// This explicitly does NOT include in memory indexes or in memory database.
		/// It does include any reserved space on the file system, which may significantly increase
		/// the database size.
		/// </summary>
		/// <remarks>
		/// This is a potentially a very expensive call, avoid making it if possible.
		/// </remarks>
		public long GetTotalSizeOnDisk()
		{
			if (Configuration.RunInMemory)
				return 0;
			var allFiles = Directory.GetFiles(Configuration.DataDirectory, "*.*", SearchOption.AllDirectories);
			var totalSize = allFiles.Sum(file => new FileInfo(file).Length);

			return totalSize;
		}
	}
}
