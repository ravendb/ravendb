using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database
{
	public class DocumentDatabase : IDisposable
	{
		private readonly RavenConfiguration configuration;
		private readonly WorkContext workContext;
		private Thread[] backgroundWorkers = new Thread[0];

		public DocumentDatabase(RavenConfiguration configuration)
		{
			this.configuration = configuration;
			TransactionalStorage = new TransactionalStorage(configuration.DataDirectory);
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

			IndexDefinitionStorage = new IndexDefinitionStorage(configuration.DataDirectory);
			IndexStorage = new IndexStorage(configuration.DataDirectory, IndexDefinitionStorage);
			workContext = new WorkContext
			{
				IndexStorage = IndexStorage,
				TransactionaStorage = TransactionalStorage,
				IndexDefinitionStorage = IndexDefinitionStorage
			};

			if (!newDb) 
				return;

			if(configuration.ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch)
			{
				PutIndex("Raven/DocumentsByEntityName",
				         new IndexDefinition
				         {
				         	Map =
				         		@"
	from doc in docs 
	where doc[""@metadata""][""Raven-Entity-Name""] != null 
	select new { Tag = doc[""@metadata""][""Raven-Entity-Name""] };
"
				         });
			}
	
			configuration.RaiseDatabaseCreatedFromScratch(this);
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
						.Where(actions.DoesTasksExistsForIndex)
						.ToArray();
					result.Indexes = actions.GetIndexesStats().ToArray();
					actions.Commit();
				});
				return result;
			}
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
		public static extern int UuidCreateSequential(out Guid value);

		public JsonDocument Get(string key, TransactionInformation transactionInformation)
		{
			JsonDocument document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.DocumentByKey(key, transactionInformation);
				actions.Commit();
			});
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
			metadata.Add("@id", new JValue(key));
			TransactionalStorage.Batch(actions =>
			{
                if (transactionInformation == null)
                {
                    etag = actions.AddDocument(key, document.ToString(), etag, metadata.ToString());
                    actions.AddTask(new IndexDocumentTask {Index = "*", Key = key});
                }
                else
                {
                    etag = actions.AddDocumentInTransaction(transactionInformation, key, document.ToString(), etag,
                                                     metadata.ToString());
                }
			    actions.Commit();
			});
			workContext.NotifyAboutWork();
		    return new PutResult
		    {
		        Key = key,
		        ETag = (Guid)etag
		    };
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
                    actions.DeleteDocument(key, etag);
                    actions.AddTask(new RemoveFromIndexTask {Index = "*", Keys = new[] {key}});
                }
                else
                {
                    actions.DeleteDocumentInTransaction(transactionInformation, key, etag);
                }
			    actions.Commit();
			});
			workContext.NotifyAboutWork();
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
                        Put(doc.Key, null, JObject.Parse(doc.Data), JObject.Parse(doc.Metadata), null);
                });
                actions.Commit();
            });
            workContext.NotifyAboutWork();
        }

        public void Rollback(Guid txId)
        {
            TransactionalStorage.Batch(actions =>
            {
                actions.RollbackTransaction(txId);
                actions.Commit();
            });
            workContext.NotifyAboutWork();
        }

		public string PutIndex(string name, IndexDefinition definition)
		{
			switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(name, definition))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
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
					for (var i = firstAndLast.Item1; i <= firstAndLast.Item2; i += configuration.IndexingBatchSize)
					{
						actions.AddTask(new IndexDocumentRangeTask
						{
							FromId = i,
							ToId = Math.Min(i + configuration.IndexingBatchSize, firstAndLast.Item2),
							Index = name
						});
					}
				}
				actions.Commit();
			});
			workContext.NotifyAboutWork();
			return name;
		}

		public QueryResult Query(string index, IndexQuery query)
		{
			var list = new List<JObject>();
			var stale = false;
			TransactionalStorage.Batch(
				actions =>
				{
					stale = actions.DoesTasksExistsForIndex(index);
					var indexFailureInformation = actions.GetFailureRate(index);
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					var loadedIds = new HashSet<string>();
					var collection = from queryResult in IndexStorage.Query(index, query)
					                 select RetrieveDocument(actions, queryResult, loadedIds)
					                 into doc
					                 where doc != null
					                 select doc.ToJson();
					list.AddRange(collection);
					actions.Commit();
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
				DataAsJosn = queryResult.Projection,
			};
		}

		public void DeleteIndex(string name)
		{
			IndexDefinitionStorage.RemoveIndex(name);
			IndexStorage.DeleteIndex(name);
			TransactionalStorage.Batch(action =>
			{
				action.DeleteIndex(name);

				action.Commit();
			});
			workContext.NotifyAboutWork();
		}

		public Attachment GetStatic(string name)
		{
			Attachment attachment = null;
			TransactionalStorage.Batch(actions =>
			{
				attachment = actions.GetAttachment(name);
				actions.Commit();
			});
			return attachment;
		}

		public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
		{
			TransactionalStorage.Batch(actions =>
			{
				actions.AddAttachment(name, etag, data, metadata.ToString(Formatting.None));
				actions.Commit();
			});
		}

		public void DeleteStatic(string name, Guid? etag)
		{
			TransactionalStorage.Batch(actions =>
			{
				actions.DeleteAttachment(name, etag);
				actions.Commit();
			});
		}

		public JArray GetDocuments(int start, int pageSize)
		{
			var list = new JArray();
			TransactionalStorage.Batch(actions =>
			{
				foreach (
					var documentAndId in actions.DocumentsById(new Reference<bool>(), start, int.MaxValue, pageSize))
				{
					var doc = documentAndId.Item1;
					doc.Metadata.Add("@docNum", new JValue(documentAndId.Item2));

					list.Add(doc.ToJson());
				}
				actions.Commit();
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

		public PatchResult ApplyPatch(string docId, Guid? etag, JArray patchDoc, TransactionInformation transactionInformation)
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
					result = PatchResult.WriteConflict;
				}
				else
				{
					var jsonDoc = doc.ToJson();
					new JsonPatcher(jsonDoc).Apply(patchDoc);
					Put(doc.Key, doc.Etag, jsonDoc, doc.Metadata, transactionInformation);
					result = PatchResult.Patched;
				}

				actions.Commit();
			});

			return result;
		}

        public object[] Batch(IEnumerable<ICommandData> commands)
        {
        	var results = new List<object>();

            TransactionalStorage.Batch(actions =>
            {
                foreach(var command in commands)
                {
                	command.Execute(this);
                	results.Add(new {command.Method, command.Key});
                }
                actions.Commit();
            });

            workContext.NotifyAboutWork();
            return results.ToArray();
        }
	}
}