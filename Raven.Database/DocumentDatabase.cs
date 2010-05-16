using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
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

		private readonly WorkContext workContext;

		private Thread[] backgroundWorkers = new Thread[0];

		private readonly ILog log = LogManager.GetLogger(typeof (DocumentDatabase));

		public DocumentDatabase(RavenConfiguration configuration)
		{
			Configuration = configuration;
			
			configuration.Container.SatisfyImportsOnce(this);

		    workContext = new WorkContext {IndexUpdateTriggers = IndexUpdateTriggers};

			TransactionalStorage = new TransactionalStorage(configuration, workContext.NotifyAboutWork);
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
                configuration.Container.GetExportedValues<AbstractViewGenerator>());
			IndexStorage = new IndexStorage(IndexDefinitionStorage, configuration);

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
					result.CountOfDocuments = actions.GetDocumentsCount();
					result.StaleIndexes = IndexStorage.Indexes
                        .Where(s => actions.DoesTasksExistsForIndex(s, null))
						.ToArray();
					result.Indexes = actions.GetIndexesStats().ToArray();
				});
				return result;
			}
		}

		public RavenConfiguration Configuration
		{
			get; private set;
		}

		public TransactionalStorage TransactionalStorage { get; private set; }

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
					Name = "RDB Background Worker #" + i,
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
				document = actions.DocumentByKey(key, transactionInformation);
			});

			return ExecuteReadTriggersOnRead(ProcessReadVetoes(document, transactionInformation), transactionInformation);
		}

		private JsonDocument ExecuteReadTriggersOnRead(JsonDocument resultingDocument, TransactionInformation transactionInformation)
		{
			if (resultingDocument == null)
				return null;

			foreach (var readTrigger in ReadTriggers)
			{
				readTrigger.OnRead(resultingDocument.Key, resultingDocument.DataAsJson, resultingDocument.Metadata, ReadOperation.Load, transactionInformation);
			}
			return resultingDocument;
		}

		private JsonDocument ProcessReadVetoes(JsonDocument document, TransactionInformation transactionInformation)
		{
			if (document == null)
				return document;
			foreach (var readTrigger in ReadTriggers)
			{
				var readVetoResult = readTrigger.AllowRead(document.Key, document.DataAsJson, document.Metadata, ReadOperation.Load, transactionInformation);
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
					key += actions.GetNextIdentityValue(key);
				}
				metadata.Add("@id", new JValue(key));
				if (transactionInformation == null)
                {
                	AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
                	PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, transactionInformation));

					etag = actions.AddDocument(key, etag, document, metadata);
					actions.AddTask(new IndexDocumentsTask { Index = "*", Keys = new[] { key } });
                    PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, transactionInformation));
                }
                else
                {
                    etag = actions.AddDocumentInTransaction(transactionInformation, key, document.ToString(), etag,
                                                     metadata.ToString());
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

                    if (actions.DeleteDocument(key, etag))
                    {
                        actions.AddTask(new RemoveFromIndexTask {Index = "*", Keys = new[] {key}});
                        DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, transactionInformation));
                    }
                }
                else
                {
                    actions.DeleteDocumentInTransaction(transactionInformation, key, etag);
                }
				workContext.ShouldNotifyAboutWork();
			});
        	TransactionalStorage
        		.ExecuteImmediatelyOrRegisterForSyncronization(() => DeleteTriggers.Apply(trigger => trigger.AfterCommit(key)));
		}

        public void Commit(Guid txId)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.CompleteTransaction(txId, doc =>
                {
                    // doc.Etag - represent the _modified_ document etag, and we already
                    // checked etags on previous PUT/DELETE, so we don't pass it here
                    if (doc.Delete)
                        Delete(doc.Key, null, null);
                    else
                        Put(doc.Key, null,
							doc.Data.ToJObject(),
							doc.Metadata.ToJObject(), null);
                });
				workContext.ShouldNotifyAboutWork();
            });
        }

        public void Rollback(Guid txId)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.RollbackTransaction(txId);
				workContext.ShouldNotifyAboutWork();
            });
        }

		public string PutIndex(string name, IndexDefinition definition)
		{
			switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(name, definition))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					// ensure that the code can compile
					new DynamicViewCompiler(name, definition).GenerateInstance();
					DeleteIndex(name);
					break;
			}
			IndexDefinitionStorage.AddIndex(name, definition);
			IndexStorage.CreateIndexImplementation(name, definition);
			TransactionalStorage.Batch(actions =>
			{
				actions.AddIndex(name);
				var firstAndLast = actions.FirstAndLastDocumentIds();
				if (firstAndLast.Item1 != 0 && firstAndLast.Item2 != 0)
				{
					for (var i = firstAndLast.Item1; i <= firstAndLast.Item2; i += Configuration.IndexingBatchSize)
					{
						actions.AddTask(new IndexDocumentRangeTask
						{
							FromId = i,
							ToId = Math.Min(i + Configuration.IndexingBatchSize, firstAndLast.Item2),
							Index = name
						});
					}
				}
				workContext.ShouldNotifyAboutWork();
			});
			return name;
		}

		public QueryResult Query(string index, IndexQuery query)
		{
			var list = new List<JObject>();
			var stale = false;
			TransactionalStorage.Batch(
				actions =>
				{
					stale = actions.DoesTasksExistsForIndex(index, query.Cutoff);
					var indexFailureInformation = actions.GetFailureRate(index)
;
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					var loadedIds = new HashSet<string>();
					var collection = from queryResult in IndexStorage.Query(index, query)
					                 select RetrieveDocument(actions, queryResult, loadedIds)
					                 into doc
										 let processedDoc = ExecuteReadTriggersOnRead(ProcessReadVetoes(doc, null), null)
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

		private static JsonDocument RetrieveDocument(DocumentStorageActions actions, IndexQueryResult queryResult,
		                                             HashSet<string> loadedIds)
		{
			if (queryResult.Projection == null)
			{
				if (loadedIds.Add(queryResult.Key))
					return actions.DocumentByKey(queryResult.Key, null);
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
						action.DeleteIndex(name);

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
				attachment = actions.GetAttachment(name);
			});
			return attachment;
		}

		public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
		{
			TransactionalStorage.Batch(actions => actions.AddAttachment(name, etag, data, metadata.ToString(Formatting.None)));
		}

		public void DeleteStatic(string name, Guid? etag)
		{
			TransactionalStorage.Batch(actions => actions.DeleteAttachment(name, etag));
		}

		public JArray GetDocuments(int start, int pageSize, Guid? etag)
		{
			var list = new JArray();
			TransactionalStorage.Batch(actions =>
			{
			    IEnumerable<JsonDocument> documents;
                if (etag == null)
                    documents = actions.GetDocumentsByReverseUpdateOrder(start);
                else
                    documents = actions.GetDocumentsAfter(etag.Value);
			    foreach (var doc in  documents
                    .Take(pageSize))
				{
					var document = ExecuteReadTriggersOnRead(ProcessReadVetoes(doc, null), null);
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
				var doc = actions.DocumentByKey(docId, transactionInformation);
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
                		Etag = command.Etag
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
					hasTasks = actions.HasTasks;
				});
				return hasTasks;
			}
		}

		public int ApproximateTaskCount
		{
			get
			{
				int approximateTaskCount = 0;
				TransactionalStorage.Batch(actions =>
				{
					approximateTaskCount = actions.ApproximateTaskCount;
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

			var backupOperation = new BackupOperation(this, Configuration.DataDirectory, backupDestinationDirectory);
			ThreadPool.QueueUserWorkItem(backupOperation.Execute);
		}

		public static void Restore(string backupLocation, string databaseLocation)
		{
			new RestoreOperation(backupLocation, databaseLocation).Execute();
		}
	}
}