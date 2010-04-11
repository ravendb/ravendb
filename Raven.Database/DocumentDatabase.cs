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
			IndexStorage = new IndexStorage(configuration.DataDirectory);
			workContext = new WorkContext
			{
				IndexStorage = IndexStorage,
				TransactionaStorage = TransactionalStorage,
				IndexDefinitionStorage = IndexDefinitionStorage
			};

			if (newDb)
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

		public JsonDocument Get(string key)
		{
			JsonDocument document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.DocumentByKey(key);
				actions.Commit();
			});
			return document;
		}

		public string Put(string key, Guid? etag, JObject document, JObject metadata)
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
				actions.AddDocument(key, document.ToString(), etag, metadata.ToString());
				actions.AddTask(new IndexDocumentTask {Index = "*", Key = key});
				actions.Commit();
			});
			workContext.NotifyAboutWork();
			return key;
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

		public void Delete(string key, Guid? etag)
		{
			TransactionalStorage.Batch(actions =>
			{
				actions.DeleteDocument(key, etag);
				actions.AddTask(new RemoveFromIndexTask {Index = "*", Keys = new[] {key}});
				actions.Commit();
			});
			workContext.NotifyAboutWork();
		}

		public string PutIndex(string name, string mapDef)
		{
			return PutIndex(name, mapDef, null);
		}

		public string PutIndex(string name, string mapDef, string reduceDef)
		{
			switch (IndexDefinitionStorage.FindIndexCreationOptionsOptions(name, mapDef))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					DeleteIndex(name);
					break;
			}
			IndexDefinitionStorage.AddIndex(name, mapDef, reduceDef);
			IndexStorage.CreateIndex(name, isMapReduce: reduceDef != null);
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

		public QueryResult Query(string index, string query, int start, int pageSize)
		{
			return Query(index, query, start, pageSize, new string[0]);
		}

		public QueryResult Query(string index, string query, int start, int pageSize, string[] fieldsToFetch)
		{
			var list = new List<JObject>();
			var stale = false;
			var totalSize = new Reference<int>();
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
					list.AddRange(from queryResult in IndexStorage.Query(index, query, start, pageSize, totalSize, fieldsToFetch)
					select RetrieveDocument(actions, queryResult, loadedIds)
					into doc
					              	where doc != null
					              	select doc.ToJson());
					actions.Commit();
				});
			return new QueryResult
			{
				Results = list.ToArray(),
				IsStale = stale,
				TotalResults = totalSize.Value
			};
		}

		private static JsonDocument RetrieveDocument(DocumentStorageActions actions, IndexQueryResult queryResult,
		                                             HashSet<string> loadedIds)
		{
			if (queryResult.Projection == null)
			{
				if (loadedIds.Add(queryResult.Key))
					return actions.DocumentByKey(queryResult.Key);
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
							{"definition", new JValue(IndexDefinitionStorage.GetIndexDefinition(indexName))}
						})
				);
		}

		public PatchResult ApplyPatch(string docId, Guid? etag, JArray patchDoc)
		{
			var result = PatchResult.Patched;
			TransactionalStorage.Batch(actions =>
			{
				var doc = actions.DocumentByKey(docId);
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
					Put(doc.Key, doc.Etag, jsonDoc, doc.Metadata);
					result = PatchResult.Patched;
				}

				actions.Commit();
			});

			return result;
		}

        public object[] Batch(IEnumerable<ICommandData> commandData)
        {
            object[] result = new object[commandData.Count()];

            TransactionalStorage.Batch(actions =>
            {
                for (int commandIndex = 0; commandIndex < commandData.Count(); commandIndex++)
                {
                    if (commandData[commandIndex] is PutCommandData)
                    {
                        PutCommandData putData = commandData[commandIndex] as PutCommandData;
                        if (string.IsNullOrEmpty(putData.Key))
                        {
                            Guid value;
                            UuidCreateSequential(out value);
                            putData.Key = value.ToString();
                        }
                        RemoveReservedProperties(putData.Document);
                        RemoveReservedProperties(putData.Metadata);
                        putData.Metadata.Add("@id", new JValue(putData.Key));

                        actions.AddDocument(putData.Key, putData.Document.ToString(), putData.Etag, putData.Metadata.ToString());
                        actions.AddTask(new IndexDocumentTask { Index = "*", Key = putData.Key });

                        result[commandIndex] = new { Method = "PUT", Key = putData.Key };
                        continue;
                    }

                    if (commandData[commandIndex] is DeleteCommandData)
                    {
                        DeleteCommandData deleteData = commandData[commandIndex] as DeleteCommandData;
                        actions.DeleteDocument(deleteData.Key, deleteData.Etag);
                        actions.AddTask(new RemoveFromIndexTask { Index = "*", Keys = new[] { deleteData.Key } });
                        result[commandIndex] = new { Method = "DELETE", Key = deleteData.Key };
                        continue;
                    }
                }

                actions.Commit();
            });

            workContext.NotifyAboutWork();
            return result;
        }
	}
}