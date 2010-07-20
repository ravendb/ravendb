using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using System.Transactions;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Backup;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Raven.Database.Tasks;

namespace Raven.Database
{
	public class DocumentDatabase : IDisposable
	{
		[ImportMany]
		public IEnumerable<AbstractPutTrigger> PutTriggers { get; set; }

		[ImportMany]
		public IEnumerable<AbstractDeleteTrigger> DeleteTriggers { get; set; }


        [ImportMany]
        public IEnumerable<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }

		[ImportMany]
		public IEnumerable<AbstractReadTrigger> ReadTriggers { get; set; }

		[ImportMany]
		public AbstractDynamicCompilationExtension[] Extensions { get; set; }

		private readonly WorkContext workContext;

		private Thread[] backgroundWorkers = new Thread[0];

		private readonly ILog log = LogManager.GetLogger(typeof (DocumentDatabase));

		public DocumentDatabase(RavenConfiguration configuration)
		{
			Configuration = configuration;
			
			configuration.Container.SatisfyImportsOnce(this);

		    workContext = new WorkContext {IndexUpdateTriggers = IndexUpdateTriggers};

			TransactionalStorage = configuration.CreateTransactionalStorage(workContext.NotifyAboutWork);
			configuration.Container.SatisfyImportsOnce(TransactionalStorage);
			
            bool newDb;
			try
			{
				newDb = TransactionalStorage.Initialize();
			}
			catch (Exception)
			{
				TransactionalStorage.Dispose();
				throw;
			}

			IndexDefinitionStorage = new IndexDefinitionStorage(
                TransactionalStorage,
                configuration.DataDirectory, 
                configuration.Container.GetExportedValues<AbstractViewGenerator>(),
				Extensions);
			IndexStorage = new IndexStorage(IndexDefinitionStorage, configuration);

			workContext.PerformanceCounters = new PerformanceCounters("Instance @ " + configuration.Port);
			workContext.IndexStorage = IndexStorage;
			workContext.TransactionaStorage = TransactionalStorage;
			workContext.IndexDefinitionStorage = IndexDefinitionStorage;


			InitializeTriggers();
			ExecuteStartupTasks();

			if (!newDb) 
				return;

			OnNewlyCreatedDatabase();
		}

		private void InitializeTriggers()
		{
			PutTriggers.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			DeleteTriggers.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
			ReadTriggers.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
            IndexUpdateTriggers.OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
		}

		private void ExecuteStartupTasks()
		{
			foreach (var task in Configuration.Container.GetExportedValues<IStartupTask>())
			{
				task.Execute(this);
			}
		}

		private void OnNewlyCreatedDatabase()
		{
		    PutIndex("Raven/DocumentsByEntityName",
		             new IndexDefinition
		             {
		                 Map =
						 @"from doc in docs 
where doc[""@metadata""][""Raven-Entity-Name""] != null 
select new { Tag = doc[""@metadata""][""Raven-Entity-Name""] };
",
		                 Indexes = {{"Tag", FieldIndexing.NotAnalyzed}},
		                 Stores = {{"Tag", FieldStorage.No}}
		             });
		}

	    public DatabaseStatistics Statistics
		{
			get
			{
				var result = new DatabaseStatistics
				{
					CountOfIndexes = IndexStorage.Indexes.Length,
					Errors = workContext.Errors
				};
				TransactionalStorage.Batch(actions =>
				{
					result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
					result.CountOfDocuments = actions.Documents.GetDocumentsCount();
					result.StaleIndexes = IndexStorage.Indexes
                        .Where(s => actions.Tasks.DoesTasksExistsForIndex(s, null))
						.ToArray();
					result.Indexes = actions.Indexing.GetIndexesStats().ToArray();
				});
				return result;
			}
		}

		public RavenConfiguration Configuration
		{
			get; private set;
		}

		public ITransactionalStorage TransactionalStorage { get; private set; }

		public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

		public IndexStorage IndexStorage { get; private set; }

		#region IDisposable Members

		public void Dispose()
		{
			workContext.StopWork();
			TransactionalStorage.Dispose();
			IndexStorage.Dispose();
			foreach (var backgroundWorker in backgroundWorkers)
			{
				backgroundWorker.Join();
			}
		}

		public WorkContext WorkContext
		{
			get { return workContext; }
		}

		#endregion

		public void SpinBackgroundWorkers()
		{
			const int threadCount = 1; // Environment.ProcessorCount;
			backgroundWorkers = new Thread[threadCount];
			for (var i = 0; i < threadCount; i++)
			{
				backgroundWorkers[i] = new Thread(new TaskExecuter(TransactionalStorage, workContext).Execute)
				{
					IsBackground = true,
					Name = "RavenDB Background Worker #" + i,
				};
				backgroundWorkers[i].Start();
			}
		}

		[SuppressUnmanagedCodeSecurity]
		[DllImport("rpcrt4.dll", SetLastError = true)]
		private static extern int UuidCreateSequential(out Guid value);

        public static Guid CreateSequentialUuid()
        {
            Guid value;
            UuidCreateSequential(out value);
            var byteArray = value.ToByteArray();
            Array.Reverse(byteArray);
            return new Guid(byteArray);
        }

	    public JsonDocument Get(string key, TransactionInformation transactionInformation)
		{
			JsonDocument document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.Documents.DocumentByKey(key, transactionInformation);
			});

			return ExecuteReadTriggersOnRead(ProcessReadVetoes(document, transactionInformation, ReadOperation.Load), transactionInformation, ReadOperation.Load);
		}

		private JsonDocument ExecuteReadTriggersOnRead(JsonDocument resultingDocument, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if (resultingDocument == null)
				return null;

			foreach (var readTrigger in ReadTriggers)
			{
				readTrigger.OnRead(resultingDocument.Key, resultingDocument.DataAsJson, resultingDocument.Metadata, operation, transactionInformation);
			}
			return resultingDocument;
		}

		private JsonDocument ProcessReadVetoes(JsonDocument document, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if (document == null)
				return document;
			foreach (var readTrigger in ReadTriggers)
			{
				var readVetoResult = readTrigger.AllowRead(document.Key, document.DataAsJson, document.Metadata, operation, transactionInformation);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						return new JsonDocument
						{
							DataAsJson = 
								JObject.FromObject(new
								{
									Message = "The document exists, but it is hidden by a read trigger",
									DocumentHidden = true,
									readVetoResult.Reason
								}),
							Metadata = JObject.FromObject(
								new
								{
									ReadVeto = true,
									VetoingTrigger = readTrigger.ToString()
								}
								)
						};
					case ReadVetoResult.ReadAllow.Ignore:
						return null;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}

			return document;
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata, TransactionInformation transactionInformation)
		{
			if (string.IsNullOrEmpty(key))
			{
				Guid value;
				UuidCreateSequential(out value);
				key = value.ToString();
			}
			RemoveReservedProperties(document);
			RemoveReservedProperties(metadata);
			TransactionalStorage.Batch(actions =>
			{
			    metadata["Last-Modified"] = JToken.FromObject(DateTime.UtcNow.ToString("r"));
				if (key.EndsWith("/"))
				{
					key += actions.General.GetNextIdentityValue(key);
				}
				metadata.Add("@id", new JValue(key));
				if (transactionInformation == null)
                {
                	AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
                	PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, transactionInformation));

					etag = actions.Documents.AddDocument(key, etag, document, metadata);
					AddIndexingTask(actions, metadata, () => new IndexDocumentsTask { Keys = new[] { key } });
                    PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, transactionInformation));
                }
                else
                {
                    etag = actions.Transactions.AddDocumentInTransaction(key, etag,
                                                     document, metadata, transactionInformation);
                }
				workContext.ShouldNotifyAboutWork();
			});

			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSyncronization(() => PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata)));
	
		    return new PutResult
		    {
		        Key = key,
		        ETag = (Guid)etag
		    };
		}

		private void AddIndexingTask(IStorageActionsAccessor actions, JToken metadata, Func<Task> taskGenerator)
		{
			foreach (var indexName in IndexDefinitionStorage.IndexNames)
			{
				var viewGenerator = IndexDefinitionStorage.GetViewGenerator(indexName);
				if(viewGenerator==null)
					continue;
				var entityName = metadata.Value<string>("Raven-Entity-Name");
				if(viewGenerator.ForEntityName != null && 
						viewGenerator.ForEntityName != entityName)
					continue;
				var task = taskGenerator();
				task.Index = indexName;
				actions.Tasks.AddTask(task);
			}
		}

		private void AssertPutOperationNotVetoed(string key, JObject metadata, JObject document, TransactionInformation transactionInformation)
		{
			var vetoResult = PutTriggers
				.Select(trigger => new{Trigger = trigger, VetoResult = trigger.AllowPut(key, document,metadata, transactionInformation)})
				.FirstOrDefault(x=>x.VetoResult.IsAllowed == false);
			if(vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
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

		private static void RemoveReservedProperties(JObject document)
		{
			var toRemove = new HashSet<string>();
			foreach (var property in document.Properties())
			{
				if (property.Name.StartsWith("@"))
					toRemove.Add(property.Name);
			}
			foreach (var propertyName in toRemove)
			{
				document.Remove(propertyName);
			}
		}

        public void Delete(string key, Guid? etag, TransactionInformation transactionInformation)
		{
			TransactionalStorage.Batch(actions =>
			{
                if (transactionInformation == null)
                {
					AssertDeleteOperationNotVetoed(key, transactionInformation);

                	DeleteTriggers.Apply(trigger => trigger.OnDelete(key, transactionInformation));

                	JObject metadata;
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
			catch (EsentErrorException e)
			{
				// we need to protect ourselve from rollbacks happening in an async manner
				// after the database was already shut down.
				if (e.Error != JET_err.InvalidInstance)
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
        			workContext.ShouldNotifyAboutWork();
        		});
        	}
        	catch (EsentErrorException e)
        	{
				// we need to protect ourselve from rollbacks happening in an async manner
				// after the database was already shut down.
				if (e.Error != JET_err.InvalidInstance)
					throw;
        	}
        }

		public string PutIndex(string name, IndexDefinition definition)
		{
			switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(name, definition))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					// ensure that the code can compile
					new DynamicViewCompiler(name, definition, Extensions).GenerateInstance();
					DeleteIndex(name);
					break;
			}
			IndexDefinitionStorage.AddIndex(name, definition);
			IndexStorage.CreateIndexImplementation(name, definition);
			TransactionalStorage.Batch(actions => AddIndexAndEnqueueIndexingTasks(actions, name));
			return name;
		}

		private void AddIndexAndEnqueueIndexingTasks(IStorageActionsAccessor actions, string indexName)
		{
			actions.Indexing.AddIndex(indexName);
			var firstAndLast = actions.Documents.FirstAndLastDocumentIds();
			if (firstAndLast.Item1 != 0 && firstAndLast.Item2 != 0)
			{
				for (var i = firstAndLast.Item1; i <= firstAndLast.Item2; i += Configuration.IndexingBatchSize)
				{
					actions.Tasks.AddTask(new IndexDocumentRangeTask
					{
						FromId = i,
						ToId = Math.Min(i + Configuration.IndexingBatchSize, firstAndLast.Item2),
						Index = indexName
					});
				}
			}
			workContext.ShouldNotifyAboutWork();
		}

		public QueryResult Query(string index, IndexQuery query)
		{
			var list = new List<JObject>();
			var stale = false;
			TransactionalStorage.Batch(
				actions =>
				{
					stale = actions.Tasks.DoesTasksExistsForIndex(index, query.Cutoff);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index);
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					var loadedIds = new HashSet<string>();
					var collection = from queryResult in IndexStorage.Query(index, query)
					                 select RetrieveDocument(actions, queryResult, loadedIds)
					                 into doc
										 let processedDoc = ExecuteReadTriggersOnRead(ProcessReadVetoes(doc, null, ReadOperation.Query), null, ReadOperation.Query)
										 where processedDoc != null
										 select processedDoc.ToJson();
					list.AddRange(collection);
				});
			return new QueryResult
			{
				Results = list.ToArray(),
				IsStale = stale,
				TotalResults = query.TotalSize.Value
			};
		}

		public IEnumerable<string> QueryDocumentIds(string index, IndexQuery query, out bool stale)
		{
			bool isStale = false;
			HashSet<string> loadedIds = null;
			TransactionalStorage.Batch(
				actions =>
				{
					isStale = actions.Tasks.DoesTasksExistsForIndex(index, query.Cutoff);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index)
;
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					loadedIds = new HashSet<string>(from queryResult in IndexStorage.Query(index, query)
					                                select queryResult.Key);
				});
			stale = isStale;
			return loadedIds;
		}

		private static JsonDocument RetrieveDocument(IStorageActionsAccessor actions, IndexQueryResult queryResult,
		                                             HashSet<string> loadedIds)
		{
			if (queryResult.Projection == null)
			{
				if (loadedIds.Add(queryResult.Key))
					return actions.Documents.DocumentByKey(queryResult.Key, null);
				return null;
			}

			return new JsonDocument
			{
				Key = queryResult.Key,
				Projection = queryResult.Projection,
			};
		}

		public void DeleteIndex(string name)
		{
			IndexDefinitionStorage.RemoveIndex(name);
			IndexStorage.DeleteIndex(name);
			//we may run into a conflict when trying to delete if the index is currently
			//busy indexing documents
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
				catch (EsentErrorException e)
				{
					if(e.Error==JET_err.WriteConflict)
					{
						Thread.Sleep(100);
						continue;
					}
					throw;
				}
			}
		}

		public Attachment GetStatic(string name)
		{
			Attachment attachment = null;
			TransactionalStorage.Batch(actions =>
			{
				attachment = actions.Attachments.GetAttachment(name);
			});
			return attachment;
		}

		public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
		{
			TransactionalStorage.Batch(actions => actions.Attachments.AddAttachment(name, etag, data, metadata));
		}

		public void DeleteStatic(string name, Guid? etag)
		{
			TransactionalStorage.Batch(actions => actions.Attachments.DeleteAttachment(name, etag));
		}

		public JArray GetDocuments(int start, int pageSize, Guid? etag)
		{
			var list = new JArray();
			TransactionalStorage.Batch(actions =>
			{
			    IEnumerable<JsonDocument> documents;
                if (etag == null)
                    documents = actions.Documents.GetDocumentsByReverseUpdateOrder(start);
                else
                    documents = actions.Documents.GetDocumentsAfter(etag.Value);
			    foreach (var doc in  documents
                    .Take(pageSize))
				{
					var document = ExecuteReadTriggersOnRead(ProcessReadVetoes(doc, null, ReadOperation.Query), null, ReadOperation.Query);
					if(document == null)
						continue;
					if (document.Metadata.Property("@id") == null)
						document.Metadata.Add("@id", new JValue(doc.Key));

					list.Add(document.ToJson());
				}
			});
			return list;
		}

		public JArray GetIndexNames(int start, int pageSize)
		{
			return new JArray(
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(s => new JValue(s))
				);
		}

		public JArray GetIndexes(int start, int pageSize)
		{
			return new JArray(
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(
						indexName => new JObject
						{
							{"name", new JValue(indexName)},
							{"definition", JObject.FromObject(IndexDefinitionStorage.GetIndexDefinition(indexName))}
						})
				);
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
					throw new ConcurrencyException("Could not patch document '" + docId+ "' because non current etag was used")
					{
						ActualETag = doc.Etag,
						ExpectedETag = etag.Value,
					};
				}
				else
				{
					var jsonDoc = doc.ToJson();
					new JsonPatcher(jsonDoc).Apply(patchDoc);
					Put(doc.Key, doc.Etag, jsonDoc, doc.Metadata, transactionInformation);
					result = PatchResult.Patched;
				}

				workContext.ShouldNotifyAboutWork();
			});

			return result;
		}

		public BatchResult[] Batch(ICollection<ICommandData> commands)
        {
			var results = new List<BatchResult>();

			log.DebugFormat("Executing {0} batched commands in a single transaction", commands.Count);
            TransactionalStorage.Batch(actions =>
            {
                foreach(var command in commands)
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
			log.DebugFormat("Successfully executed {0} commands", commands.Count);
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
			var document = Get(BackupStatus.RavenBackupStatusDocumentKey,null);
			if(document!=null)
			{
				var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
				if(backupStatus.IsRunning)
				{
					throw new InvalidOperationException("Backup is already running");
				}
			}
			Put(BackupStatus.RavenBackupStatusDocumentKey, null, JObject.FromObject(new BackupStatus
			{
				Started = DateTime.Now,
				IsRunning = true,
			}), new JObject(), null);

			TransactionalStorage.StartBackupOperation(this,backupDestinationDirectory);
		}

		public static void Restore(RavenConfiguration configuration, string backupLocation, string databaseLocation)
		{
			using(var transactionalStorage = configuration.CreateTransactionalStorage(() => { }))
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
			var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
			if(indexDefinition == null)
				throw new InvalidOperationException("There is no index named: " + index);
			IndexStorage.DeleteIndex(index);
			IndexStorage.CreateIndexImplementation(index, indexDefinition);
			TransactionalStorage.Batch(actions =>
			{
				actions.Indexing.DeleteIndex(index);
				AddIndexAndEnqueueIndexingTasks(actions, index);
			});
		}

		public IndexDefinition GetIndexDefinition(string index)
		{
			return IndexDefinitionStorage.GetIndexDefinition(index);
		}
	}
}