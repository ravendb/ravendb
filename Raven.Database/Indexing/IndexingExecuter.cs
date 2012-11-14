//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
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

		[DebuggerDisplay("{DebugDisplay}")]
		private class FutureIndexBatch
		{
			public Guid StartingEtag;
			public Task<JsonResults> Task;
			public int Age;

			public string DebugDisplay
			{
				get
				{
					if (Task.IsCompleted == false)
						return "Etag: " + StartingEtag + ", Age: " + Age + " Results: Pending";

					return "Etag: " + StartingEtag + ", Age: " + Age + " Results: " + Task.Result.Results.Length.ToString("#,#");
				}
			}
		}

		public class JsonResults
		{
			public JsonDocument[] Results;
			public bool LoadedFromDisk;

			public override string ToString()
			{
				if (Results == null)
					return "0";
				return Results.Length.ToString("#,#", CultureInfo.InvariantCulture);
			}
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
			JsonResults jsonDocs = null;
			try
			{
				jsonDocs = GetJsonDocuments(lastIndexedGuidForAllIndexes);

				if (Log.IsDebugEnabled)
				{
					Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
							  jsonDocs.Results.Length, lastIndexedGuidForAllIndexes, string.Join(", ", jsonDocs.Results.Select(x => x.Key)));
				}

				context.ReportIndexingActualBatchSize(jsonDocs.Results.Length);
				context.CancellationToken.ThrowIfCancellationRequested();

				MaybeAddFutureBatch(jsonDocs);

				if (jsonDocs.Results.Length > 0)
				{
					context.IndexedPerSecIncreaseBy(jsonDocs.Results.Length);
					var result = FilterIndexes(indexesToWorkOn, jsonDocs.Results).ToList();
					indexesToWorkOn = result.Select(x => x.Item1).ToList();
					var sw = Stopwatch.StartNew();
					BackgroundTaskExecuter.Instance.ExecuteAll(context, result, (indexToWorkOn, _) =>
					{
						var index = indexToWorkOn.Item1;
						var docs = indexToWorkOn.Item2;
						transactionalStorage.Batch(
							actions => IndexDocuments(actions, index.IndexName, docs));

					});
					indexingDuration = sw.Elapsed;
				}
			}
			catch (OperationCanceledException)
			{
				operationCancelled = true;
			}
			finally
			{
				if (operationCancelled == false && jsonDocs != null && jsonDocs.Results.Length > 0)
				{
					var lastByEtag = GetHighestEtag(jsonDocs.Results);
					var lastModified = lastByEtag.LastModified.Value;
					var lastEtag = lastByEtag.Etag.Value;

					if (Log.IsDebugEnabled)
					{
						Log.Debug("Aftering indexing {0} documents, the new last etag for is: {1} for {2}",
								  jsonDocs.Results.Length,
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

					UpdateAutoThrottler(jsonDocs.Results, indexingDuration);
				}

				// make sure that we don't have too much "future cache" items
				foreach (var source in futureIndexBatches.Where(x => (currentIndexingAge - x.Age) > 25).ToList())
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
				{
					var jsonResults = x.Task.Result;
					return jsonResults.LoadedFromDisk ? jsonResults.Results.Length : 0;
				}
				return autoTuner.NumberOfItemsToIndexInSingleBatch / 15;
			});

			var futureSize = futureIndexBatches.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					var jsonResults = x.Task.Result;
					return jsonResults.LoadedFromDisk ? jsonResults.Results.Sum(s => s.SerializedSizeOnDisk) : 0;
				}
				return autoTuner.NumberOfItemsToIndexInSingleBatch * 256;

			});
			autoTuner.AutoThrottleBatchSize(jsonDocs.Length + futureLen, futureSize + jsonDocs.Sum(x => x.SerializedSizeOnDisk), indexingDuration);
		}

		public JsonResults GetJsonDocuments(Guid lastIndexedGuidForAllIndexes)
		{
			var futureResults = GetFutureJsonDocuments(lastIndexedGuidForAllIndexes);
			if (futureResults != null)
				return futureResults;
			return GetJsonDocsFromDisk(lastIndexedGuidForAllIndexes);
		}

		private JsonResults GetFutureJsonDocuments(Guid lastIndexedGuidForAllIndexes)
		{
			var nextBatch = futureIndexBatches.FirstOrDefault(x => x.StartingEtag == lastIndexedGuidForAllIndexes);
			if (nextBatch == null)
				return null;

			if (System.Threading.Tasks.Task.CurrentId == nextBatch.Task.Id)
				return null;
			try
			{
				futureIndexBatches.TryRemove(nextBatch);
				if (nextBatch.Task.IsCompleted == false)
				{
					if (nextBatch.Task.Wait(Timeout.Infinite) == false)
						return null;
				}
				var timeToWait = 500;

				var items = new List<JsonResults>
				{
					nextBatch.Task.Result
				};
				while (true)
				{
					var nextHighestEtag = GetNextHighestEtag(nextBatch.Task.Result.Results);
					nextBatch = futureIndexBatches.FirstOrDefault(x => x.StartingEtag == nextHighestEtag);
					if (nextBatch == null)
					{
						break;
					}
					futureIndexBatches.TryRemove(nextBatch);
					timeToWait /= 2;
					if (nextBatch.Task.Wait(timeToWait) == false)
						break;

					items.Add(nextBatch.Task.Result);
				}

				if (items.Count == 1)
					return items[0];

				// a single doc may appear multiple times, if it was updated
				// while we were fetching things, so we have several versions
				// of the same doc loaded, this will make sure that we will only 
				// take one of them.
				return new JsonResults
				{
					Results = items.SelectMany(x => x.Results).GroupBy(x => x.Key)
						.Select(g => g.OrderBy(x => x.Etag).First())
						.ToArray(),
					LoadedFromDisk = items.Aggregate(false, (prev, results) => prev | results.LoadedFromDisk)
				};
			}
			catch (Exception e)
			{
				Log.WarnException("Error when getting next batch value asyncronously, will try in sync manner", e);
				return null;
			}
		}



		private void MaybeAddFutureBatch(JsonResults past)
		{
			if (context.Configuration.MaxNumberOfParallelIndexTasks == 1)
				return;
			if (past.Results.Length == 0 || past.LoadedFromDisk == false)
				return;
			if (futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				var alreadyLoaded = futureIndexBatches.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Results.Length;
					return 0;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToIndexInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future cachings
			if (MemoryStatistics.AvailableMemory < context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			var nextEtag = GetNextHighestEtag(past.Results);

			var nextBatch = futureIndexBatches.FirstOrDefault(x => x.StartingEtag == nextEtag);

			if (nextBatch != null)
				return;

			var futureBatchStat = new FutureBatchStats
			{
				Timestamp = SystemTime.UtcNow,
			};
			var sp = Stopwatch.StartNew();
			context.AddFutureBatch(futureBatchStat);
			futureIndexBatches.Add(new FutureIndexBatch
			{
				StartingEtag = nextEtag,
				Age = currentIndexingAge,
				Task = System.Threading.Tasks.Task.Factory.StartNew(() =>
				{
					var jsonDocuments = GetJsonDocuments(nextEtag);
					int localWork = workCounter;
					while (jsonDocuments.Results.Length == 0 && context.DoWork)
					{
						futureBatchStat.Retries++;

						if (context.WaitForWork(TimeSpan.FromMinutes(10), ref localWork, "PreFetching") == false)
							continue;

						jsonDocuments = GetJsonDocuments(nextEtag);
					}
					futureBatchStat.Duration = sp.Elapsed;
					futureBatchStat.Size = jsonDocuments.Results.Length;
					MaybeAddFutureBatch(jsonDocuments);
					return jsonDocuments;
				})
			});
		}

		private static Guid GetNextHighestEtag(JsonDocument[] past)
		{
			return GetHighestEtag(past).Etag.Value;
		}


		private static JsonDocument GetHighestEtag(JsonDocument[] past)
		{
			var highest = new ComparableByteArray(Guid.Empty);
			JsonDocument highestDoc = null;
			for (int i = past.Length - 1; i >= 0; i--)
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

		private JsonResults GetJsonDocsFromDisk(Guid lastIndexed)
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
			return new JsonResults
			{
				Results = jsonDocs,
				LoadedFromDisk = true
			};
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
					if (etag == null)
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
				if (Log.IsDebugEnabled)
				{
					Log.Debug("Going to index {0} documents in {1}: ({2})", batch.Ids.Count, indexToWorkOn, string.Join(", ", batch.Ids));
				}
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

		private void IndexDocuments(IStorageActionsAccessor actions, string index, IndexingBatch batch)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(index);
			if (viewGenerator == null)
				return; // index was deleted, probably
			try
			{
				if (Log.IsDebugEnabled)
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

				context.IndexStorage.Index(index, viewGenerator, batch, context, actions, batch.DateTime ?? DateTime.MinValue);
			}
			catch (OperationCanceledException)
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

		public void AfterCommit(JsonDocument[] docs)
		{
			if (docs.Length == 0)
				return;

			foreach (var doc in docs)
			{
				DocumentRetriever.EnsureIdInMetadata(doc);
			}

			futureIndexBatches.Add(new FutureIndexBatch
			{
				StartingEtag = DecrementEtag(GetLowestEtag(docs)),
				Task = new CompletedTask<JsonResults>(new JsonResults
				{
					Results = docs,
					LoadedFromDisk = false
				}),
				Age = currentIndexingAge
			});
		}

		private Guid DecrementEtag(Guid etag)
		{
			var bytes = etag.ToByteArray();
			var changes = BitConverter.ToInt64(bytes.Skip(8).Reverse().ToArray(), 0) - 1;
			var restarts = BitConverter.ToInt64(bytes.Take(8).Reverse().ToArray(), 0);
			if (restarts == 1 && changes == 0)// very first item, we start from 0-0
				restarts = 0;
			return new Guid(BitConverter.GetBytes(restarts).Reverse().Concat(BitConverter.GetBytes(changes).Reverse()).ToArray());
		}


		private static Guid GetLowestEtag(JsonDocument[] past)
		{
			var lowest = new ComparableByteArray(past[0].Etag.Value);
			for (int i = 1; i < past.Length; i++)
			{
				var etag = past[i].Etag.Value;
				if (lowest.CompareTo(etag) < 0)
				{
					continue;
				}
				lowest = new ComparableByteArray(etag);
			}
			return lowest.ToGuid();
		}
	}
}
