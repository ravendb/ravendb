//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Task = Raven.Database.Tasks.Task;

namespace Raven.Database.Indexing
{
	public class IndexingExecuter : AbstractIndexingExecuter
	{
		public IndexingExecuter(WorkContext context)
			: base(context)
		{
			autoTuner = new IndexBatchSizeAutoTuner(context);
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions)
		{
			return actions.Staleness.IsMapStale(indexesStat.Name);
		}

		protected override Task GetApplicableTask(IStorageActionsAccessor actions)
		{
			return actions.Tasks.GetMergedTask<RemoveFromIndexTask>();
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushMapIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexName = indexesStat.Name,
				LastIndexedEtag = indexesStat.LastIndexedEtag
			};
		}

		private class FutureIndexBatch
		{
			public Guid StartingEtag;
			public Task<JsonDocument[]> Task;
			public int Age;
		}

		private int currentIndexingAge;

		private readonly ConcurrentSet<FutureIndexBatch> futureIndexBatches = new ConcurrentSet<FutureIndexBatch>();

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			currentIndexingAge++;

			indexesToWorkOn = context.Configuration.IndexingScheduler.FilterMapIndexes(indexesToWorkOn);

			var lastIndexedGuidForAllIndexes = indexesToWorkOn.Min(x => new ComparableByteArray(x.LastIndexedEtag.ToByteArray())).ToGuid();

			context.CancellationToken.ThrowIfCancellationRequested();

			var operationCancelled = false;
			TimeSpan indexingDuration = TimeSpan.Zero;
			JsonDocument[] jsonDocs = null;
			try
			{
				jsonDocs = GetJsonDocuments(lastIndexedGuidForAllIndexes);

				Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}",
										  jsonDocs.Length, lastIndexedGuidForAllIndexes);

				context.ReportIndexingActualBatchSize(jsonDocs.Length);
				context.CancellationToken.ThrowIfCancellationRequested();

				MaybeAddFutureBatch(jsonDocs);

				if (jsonDocs.Length > 0)
				{
					context.IndexedPerSecIncreaseBy(jsonDocs.Length);
					var result = FilterIndexes(indexesToWorkOn, jsonDocs).ToList();
					indexesToWorkOn = result.Select(x => x.Item1).ToList();
					var sw = Stopwatch.StartNew();
					BackgroundTaskExecuter.Instance.ExecuteAll(context, result, (indexToWorkOn,_) =>
					{
						var index = indexToWorkOn.Item1;
						var docs = indexToWorkOn.Item2;
						transactionalStorage.Batch(
							actions => IndexDocuments(actions, index.IndexName, docs));
					
					});
					indexingDuration = sw.Elapsed;
				}
			}
			catch(OperationCanceledException)
			{
				operationCancelled = true;
			}
			finally
			{
				if (operationCancelled == false && jsonDocs != null && jsonDocs.Length > 0)
				{
					var lastByEtag = GetHighestEtag(jsonDocs);
					var lastModified = lastByEtag.LastModified.Value;
					var lastEtag = lastByEtag.Etag.Value;

					if(Log.IsDebugEnabled)
					{
						Log.Debug("Aftering indexing {0} documents, the new last etag for is: {1} for {2}",
						          jsonDocs.Length,
						          lastEtag,
								  string.Join(", ", indexesToWorkOn.Select(x => x.IndexName))
							);
					}

					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed documents
					transactionalStorage.Batch(actions =>
					{
						foreach (var indexToWorkOn in indexesToWorkOn)
						{
							actions.Indexing.UpdateLastIndexed(indexToWorkOn.IndexName, lastEtag, lastModified);
						}
					});

					UpdateAutoThrottler(jsonDocs, indexingDuration);
				}

				// make sure that we don't have too much "future cache" items
				foreach (var source in futureIndexBatches.Where(x => x.Age + 16 < currentIndexingAge).ToList())
				{
					ObserveDiscardedTask(source);
					futureIndexBatches.TryRemove(source);
				}
			}
		}

		private void UpdateAutoThrottler(JsonDocument[] jsonDocs, TimeSpan indexingDuration)
		{
			var futureLen = futureIndexBatches.Sum(x =>
			{
				if (x.Task.IsCompleted)
					return x.Task.Result.Length;
				return autoTuner.NumberOfItemsToIndexInSingleBatch/15;
			});

			var futureSize = futureIndexBatches.Sum(x =>
			{
				if (x.Task.IsCompleted)
					return x.Task.Result.Sum(s=>s.SerializedSizeOnDisk);
				return autoTuner.NumberOfItemsToIndexInSingleBatch * 256;
		
			});
			autoTuner.AutoThrottleBatchSize(jsonDocs.Length + futureLen, futureSize + jsonDocs.Sum(x => x.SerializedSizeOnDisk), indexingDuration);
		}

		private JsonDocument[] GetJsonDocuments(Guid lastIndexedGuidForAllIndexes)
		{
			var futureResults = GetFutureJsonDocuments(lastIndexedGuidForAllIndexes, Timeout.Infinite);
			if (futureResults != null)
				return futureResults;
			return GetJsonDocs(lastIndexedGuidForAllIndexes);
		}

		private JsonDocument[] GetFutureJsonDocuments(Guid lastIndexedGuidForAllIndexes, int timeToWait)
		{
			var nextBatch = futureIndexBatches.FirstOrDefault(x => x.StartingEtag == lastIndexedGuidForAllIndexes);
			if (nextBatch == null)
				return null;
			
			try
			{
				if (nextBatch.Task.IsCompleted == false)
				{
					if (nextBatch.Task.Wait(timeToWait) == false)
						return null;
				}
				if (timeToWait == Timeout.Infinite)
					timeToWait = 500;

				var documents = nextBatch.Task.Result;

				var items = new List<JsonDocument[]>
				{
					documents
				};
				while (true)
				{
					var highestEtag = GetHighestEtag(documents);
					var current = GetFutureJsonDocuments(highestEtag.Etag.Value, timeToWait/2);
					if(current == null)
					{
						if (items.Count == 1)
							return items[0];

						var result = new JsonDocument[items.Sum(x => x.Length)];
						int pos = 0;
						foreach (var docs in items)
						{
							Array.Copy(docs, 0, result, pos, docs.Length);
							pos += docs.Length;
						}
						return result;
					}
					else
					{
						items.Add(current);
					}
				}
			}
			catch (Exception e)
			{
				Log.WarnException("Error when getting next batch value asyncronously, will try in sync manner", e);
				return null;
			}
			finally
			{
				futureIndexBatches.TryRemove(nextBatch);
			}
		}

		private void MaybeAddFutureBatch(JsonDocument[] past)
		{
			if(context.Configuration.MaxNumberOfParallelIndexTasks == 1)
				return;
			if(past.Length == 0)
				return;
			if(futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				var alreadyLoaded = futureIndexBatches.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Length;
					return 0;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToIndexInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future cachings
			if( MemoryStatistics.AvailableMemory < context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit )
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			var lastByEtag = GetHighestEtag(past);

			var lastEtag = lastByEtag.Etag.Value;
			var nextBatch = futureIndexBatches.FirstOrDefault(x => x.StartingEtag == lastEtag);

			if(nextBatch != null)
				return;

			var futureBatchStat = new FutureBatchStats
			{
				Timestamp = SystemTime.UtcNow,
			};
			var sp = Stopwatch.StartNew();
			context.AddFutureBatch(futureBatchStat);
			futureIndexBatches.Add(new FutureIndexBatch
			{
				StartingEtag = lastEtag,
				Age = currentIndexingAge,
				Task = System.Threading.Tasks.Task.Factory.StartNew(() =>
				{
					var jsonDocuments = GetJsonDocs(lastEtag);
					int localWork = workCounter;
					while (jsonDocuments.Length == 0  && context.DoWork)
					{
						futureBatchStat.Retries++;

						if(context.WaitForWork(TimeSpan.FromMinutes(1), ref localWork, "PreFetching") == false)
							continue;

						jsonDocuments = GetJsonDocs(lastEtag);
					}
					futureBatchStat.Duration = sp.Elapsed;
					futureBatchStat.Size = jsonDocuments.Length;
					MaybeAddFutureBatch(jsonDocuments);
					return jsonDocuments;
				})
			});
		}

		private static JsonDocument GetHighestEtag(JsonDocument[] past)
		{
			var highest = new ComparableByteArray(Guid.Empty);
			JsonDocument highestDoc = null;
			for (int i = past.Length-1; i >= 0; i--)
			{
				var etag = past[i].Etag.Value;
				if (highest.CompareTo(etag) > 0)
				{
					continue;
				}
				highest = new ComparableByteArray(etag);
				highestDoc = past[i];
			}
			return highestDoc;
		}

		private static System.Threading.Tasks.Task ObserveDiscardedTask(FutureIndexBatch source)
		{
			return source.Task.ContinueWith(task =>
			{
				if (task.Exception != null)
				{
					Log.WarnException("Error happened on discarded future work batch", task.Exception);
				}
				else
				{
					Log.Warn("WASTE: Discarding future work item without using it, to reduce memory usage");
				}
			});
		}

		protected override void Dispose()
		{
			System.Threading.Tasks.Task.WaitAll(futureIndexBatches.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		private JsonDocument[] GetJsonDocs(Guid lastIndexed)
		{
			JsonDocument[] jsonDocs = null;
			transactionalStorage.Batch(actions =>
			{
				jsonDocs = actions.Documents
					.GetDocumentsAfter(
						lastIndexed,
						autoTuner.NumberOfItemsToIndexInSingleBatch,
						autoTuner.MaximumSizeAllowedToFetchFromStorage)
					.Where(x => x != null)
					.Select(doc =>
					{
						DocumentRetriever.EnsureIdInMetadata(doc);
						return doc;
					})
					.ToArray();
			});
			return jsonDocs;
		}

		private IEnumerable<Tuple<IndexToWorkOn, IndexingBatch>> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, JsonDocument[] jsonDocs)
		{
			var last = jsonDocs.Last();

			Debug.Assert(last.Etag != null);
			Debug.Assert(last.LastModified != null);

			var lastEtag = last.Etag.Value;
			var lastModified = last.LastModified.Value;

			var lastIndexedEtag = new ComparableByteArray(lastEtag.ToByteArray());

			var documentRetriever = new DocumentRetriever(null, context.ReadTriggers);

			var filteredDocs =
				BackgroundTaskExecuter.Instance.Apply(context, jsonDocs, doc =>
				{
					var filteredDoc = documentRetriever.ExecuteReadTriggers(doc, null, ReadOperation.Index);
					return filteredDoc == null ? new
					{
						Doc = doc,
						Json = (object)new FilteredDocument(doc)
					} : new
					{
						Doc = filteredDoc, 
						Json = JsonToExpando.Convert(doc.ToJson())
					};
				});

			Log.Debug("After read triggers executed, {0} documents remained", filteredDocs.Count);

			var results = new Tuple<IndexToWorkOn, IndexingBatch>[indexesToWorkOn.Count];
			var actions = new Action<IStorageActionsAccessor>[indexesToWorkOn.Count];

			BackgroundTaskExecuter.Instance.ExecuteAll(context, indexesToWorkOn, (indexToWorkOn, i) =>
			{
				var indexLastInedexEtag = new ComparableByteArray(indexToWorkOn.LastIndexedEtag.ToByteArray());
				if (indexLastInedexEtag.CompareTo(lastIndexedEtag) >= 0)
					return;

				var indexName = indexToWorkOn.IndexName;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch();

				foreach (var item in filteredDocs)
				{
					// did we already indexed this document in this index?
					var etag = item.Doc.Etag;
					if(etag == null)
						continue;

					if (indexLastInedexEtag.CompareTo(new ComparableByteArray(etag.Value.ToByteArray())) >= 0)
						continue;


					// is the Raven-Entity-Name a match for the things the index executes on?
					if (viewGenerator.ForEntityNames.Count != 0 &&
					    viewGenerator.ForEntityNames.Contains(item.Doc.Metadata.Value<string>(Constants.RavenEntityName)) == false)
					{
						continue;
					}

					batch.Add(item.Doc, item.Json);

					if (batch.DateTime == null)
						batch.DateTime = item.Doc.LastModified;
					else
						batch.DateTime = batch.DateTime > item.Doc.LastModified
						                 	? item.Doc.LastModified
						                 	: batch.DateTime;
				}

				if (batch.Docs.Count == 0)
				{
					Log.Debug("All documents have been filtered for {0}, no indexing will be performed, updating to {1}, {2}", indexName,
						lastEtag, lastModified);
					// we use it this way to batch all the updates together
					actions[i] = accessor => accessor.Indexing.UpdateLastIndexed(indexName, lastEtag, lastModified);
					return;
				}
				Log.Debug("Going to index {0} documents in {1}", batch.Ids.Count, indexToWorkOn);
				results[i] = Tuple.Create(indexToWorkOn, batch);

			});

			transactionalStorage.Batch(actionsAccessor =>
			{
				foreach (var action in actions)
				{
					if (action != null)
						action(actionsAccessor);
				}
			});

			return results.Where(x => x != null);
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			return true;
		}

		private class IndexingBatch
		{
			public IndexingBatch()
			{
				Ids = new List<string>();
				Docs = new List<dynamic>();
			}

			public readonly List<string> Ids;
			public readonly List<dynamic> Docs;
			public DateTime? DateTime;

			public void Add(JsonDocument doc, object asJson)
			{
				Ids.Add(doc.Key);
				Docs.Add(asJson);
			}
		}

		private void IndexDocuments(IStorageActionsAccessor actions, string index, IndexingBatch batch)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
				return; // index was deleted, probably
			try
			{
				if(Log.IsDebugEnabled)
				{
					string ids;
					if (batch.Ids.Count < 256) 
						ids = string.Join(",", batch.Ids);
					else
					{
						ids = string.Join(", ", batch.Ids.Take(128)) + " ... " + string.Join(", ", batch.Ids.Skip(batch.Ids.Count - 128));
					}
					Log.Debug("Indexing {0} documents for index: {1}. ({2})", batch.Docs.Count, index, ids);
				}
				context.CancellationToken.ThrowIfCancellationRequested();

				context.IndexStorage.Index(index, viewGenerator, batch.Docs, context, actions, batch.DateTime ?? DateTime.MinValue);
			}
			catch(OperationCanceledException)
			{
				throw;
			}
			catch (Exception e)
			{
				if (actions.IsWriteConflict(e))
					return;
				Log.WarnException(
					string.Format("Failed to index documents for index: {0}", index),
					e);
			}
		}

	}
}
