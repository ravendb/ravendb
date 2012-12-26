// -----------------------------------------------------------------------
//  <copyright file="PrefetchingBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Impl;

namespace Raven.Database.Indexing
{
	public class PrefetchingBehavior : IDisposable
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();
		private readonly WorkContext context;
		private readonly BaseBatchSizeAutoTuner autoTuner;
		private readonly ConcurrentDictionary<string, HashSet<Guid>> documentsToRemove =
			new ConcurrentDictionary<string, HashSet<Guid>>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentDictionary<Guid, FutureIndexBatch> futureIndexBatches =
			new ConcurrentDictionary<Guid, FutureIndexBatch>();
		private int currentIndexingAge;

		public PrefetchingBehavior(WorkContext context, BaseBatchSizeAutoTuner autoTuner)
		{
			this.context = context;
			this.autoTuner = autoTuner;
		}

		public void Dispose()
		{
			Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		public JsonDocument[] GetDocumentsBatchFrom(Guid etag)
		{
			var jsonDocuments = GetJsonDocuments(etag);
			MaybeAddFutureBatch(jsonDocuments);
			return jsonDocuments.Results;
		}

		private JsonResults GetJsonDocuments(Guid etag)
		{
			JsonResults futureResults = GetFutureJsonDocuments(etag);
			if (futureResults != null)
				return futureResults;
			JsonResults results = GetJsonDocsFromDisk(etag);
			return results.Results.Length > 0
					   ? MergeWithOtherFutureResults(results)
					   : results;
		}

		private JsonResults GetFutureJsonDocuments(Guid lastIndexedGuidForAllIndexes)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return null;
			try
			{
				Guid nextDocEtag = GetNextDocEtag(lastIndexedGuidForAllIndexes);
				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false)
					return null;

				if (Task.CurrentId == nextBatch.Task.Id)
					return null;
				return nextBatch.Task.Result;
			}
			catch (Exception e)
			{
				Log.WarnException("Error when getting next batch value asynchronously, will try in sync manner", e);
				return null;
			}
		}


		private JsonResults GetJsonDocsFromDisk(Guid lastIndexed)
		{
			JsonDocument[] jsonDocs = null;
			context.TransactionaStorage.Batch(actions =>
			{
				Guid? untilEtag = null;

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

		private JsonResults MergeWithOtherFutureResults(JsonResults results, int timeToWait = 0)
		{
			var items = new List<JsonResults>
			{
				results
			};
			while (true)
			{
				Guid nextDocEtag = GetNextDocEtag(GetNextHighestEtag(results.Results));
				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryGetValue(nextDocEtag, out nextBatch) == false)
				{
					break;
				}
				if (nextBatch.Task.IsCompleted == false)
				{
					if (nextBatch.Task.Wait(timeToWait) == false)
						break;
					timeToWait /= 2;
				}

				FutureIndexBatch _;
				futureIndexBatches.TryRemove(nextBatch.StartingEtag, out _);

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
				Results = items.SelectMany(x => x.Results)
							   .GroupBy(x => x.Key)
							   .Select(g => g.OrderBy(x => x.Etag, ByteArrayComparer.Instance).First())
							   .ToArray(),
				LoadedFromDisk = items.Aggregate(false, (prev, r) => prev | r.LoadedFromDisk)
			};
		}

		private void MaybeAddFutureBatch(JsonResults past)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || context.RunIndexing == false)
				return;
			if (context.Configuration.MaxNumberOfParallelIndexTasks == 1)
				return;
			if (past.Results.Length == 0 || past.LoadedFromDisk == false)
				return;
			if (futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				var alreadyLoaded = futureIndexBatches.Values.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Results.Length;
					return 0;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToIndexInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future caching
			if (MemoryStatistics.AvailableMemory <
				context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			Guid highestLoadedEtag = GetNextHighestEtag(past.Results);
			Guid nextEtag = GetNextDocEtag(highestLoadedEtag);

			if (nextEtag == highestLoadedEtag)
				return; // there is nothing newer to do 


			if (futureIndexBatches.ContainsKey(nextEtag)) // already loading this
				return;

			var futureBatchStat = new FutureBatchStats
			{
				Timestamp = SystemTime.UtcNow,
			};
			Stopwatch sp = Stopwatch.StartNew();
			context.AddFutureBatch(futureBatchStat);
			futureIndexBatches.TryAdd(nextEtag, new FutureIndexBatch
			{
				StartingEtag = nextEtag,
				Age = Interlocked.Increment(ref currentIndexingAge),
				Task = Task.Factory.StartNew(() =>
				{
					JsonResults jsonDocuments = null;
					int localWork = 0;
					while (context.RunIndexing)
					{
						jsonDocuments = GetJsonDocuments(nextEtag);
						if (jsonDocuments.Results.Length > 0)
							break;

						futureBatchStat.Retries++;

						context.WaitForWork(TimeSpan.FromMinutes(10), ref localWork, "PreFetching");
					}
					futureBatchStat.Duration = sp.Elapsed;
					futureBatchStat.Size = jsonDocuments == null ? 0 : jsonDocuments.Results.Length;
					if (jsonDocuments != null)
					{
						MaybeAddFutureBatch(jsonDocuments);
					}
					return jsonDocuments;
				})
			});
		}


		private Guid GetNextDocEtag(Guid highestEtag)
		{
			Guid nextDocEtag = highestEtag;
			context.TransactionaStorage.Batch(
				accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(highestEtag); });
			return nextDocEtag;
		}

		private static Guid GetNextHighestEtag(JsonDocument[] past)
		{
			JsonDocument jsonDocument = GetHighestEtag(past);
			if (jsonDocument == null)
				return Guid.Empty;
			return jsonDocument.Etag ?? Guid.Empty;
		}

		public static JsonDocument GetHighestEtag(JsonDocument[] past)
		{
			var highest = new ComparableByteArray(Guid.Empty);
			JsonDocument highestDoc = null;
			for (int i = past.Length - 1; i >= 0; i--)
			{
				Guid etag = past[i].Etag.Value;
				if (highest.CompareTo(etag) > 0)
				{
					continue;
				}
				highest = new ComparableByteArray(etag);
				highestDoc = past[i];
			}
			return highestDoc;
		}

		private static Task ObserveDiscardedTask(FutureIndexBatch source)
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

		public void BatchProcessingComplete()
		{
			int indexingAge = Interlocked.Increment(ref currentIndexingAge);

			// make sure that we don't have too much "future cache" items
			foreach (var source in futureIndexBatches.Values.Where(x => (indexingAge - x.Age) > 64).ToList())
			{
				ObserveDiscardedTask(source);
				FutureIndexBatch _;
				futureIndexBatches.TryRemove(source.StartingEtag, out _);
			}
		}

		public void GetFutureStats(int currentBatchSize, out int futureLen, out int futureSize)
		{
			futureLen = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					JsonResults jsonResults = x.Task.Result;
					return jsonResults.LoadedFromDisk ? jsonResults.Results.Length : 0;
				}
				return currentBatchSize / 15;
			});

			futureSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					JsonResults jsonResults = x.Task.Result;
					return jsonResults.LoadedFromDisk ? jsonResults.Results.Sum(s => s.SerializedSizeOnDisk) : 0;
				}
				return currentBatchSize * 256;
			});
		}

		public void AfterCommit(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || docs.Length == 0)
				return;

			var countOfLoadedItems = futureIndexBatches.Values.Sum(x => x.Task.IsCompleted ? 1 : x.Task.Result.Results.Length);
			if (countOfLoadedItems >
				context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
				return;

			foreach (JsonDocument doc in docs)
			{
				DocumentRetriever.EnsureIdInMetadata(doc);
			}

			var startingEtag = GetLowestEtag(docs);
			futureIndexBatches.TryAdd(startingEtag, new FutureIndexBatch
			{
				StartingEtag = startingEtag,
				Task = new CompletedTask<JsonResults>(new JsonResults
				{
					Results = docs,
					LoadedFromDisk = false
				}),
				Age = Interlocked.Increment(ref currentIndexingAge)
			});
		}

		private static Guid GetLowestEtag(JsonDocument[] past)
		{
			var lowest = new ComparableByteArray(past[0].Etag.Value);
			for (int i = 1; i < past.Length; i++)
			{
				Guid etag = past[i].Etag.Value;
				if (lowest.CompareTo(etag) < 0)
				{
					continue;
				}
				lowest = new ComparableByteArray(etag);
			}
			return lowest.ToGuid();
		}

		public void CleanupDocumentsToRemove(Guid lastIndexedEtag)
		{
			var highest = new ComparableByteArray(lastIndexedEtag);

			foreach (var docToRemove in documentsToRemove)
			{
				if (docToRemove.Value.All(etag => highest.CompareTo(etag) > 0) == false)
					continue;

				HashSet<Guid> _;
				documentsToRemove.TryRemove(docToRemove.Key, out _);
			}
		}

		public bool FilterDocuments(JsonDocument document)
		{
			HashSet<Guid> etags;
			return documentsToRemove.TryGetValue(document.Key, out etags) && etags.Contains(document.Etag.Value);
		}

		public void AfterDelete(string key, Guid lastDocumentEtag)
		{
			documentsToRemove.AddOrUpdate(key, s => new HashSet<Guid> { lastDocumentEtag },
										  (s, set) => new HashSet<Guid>(set) { lastDocumentEtag });
		}

		[DebuggerDisplay("{DebugDisplay}")]
		private class FutureIndexBatch
		{
			public int Age;
			public Guid StartingEtag;
			public Task<JsonResults> Task;

			// ReSharper disable UnusedMember.Local
			public string DebugDisplay
			// ReSharper restore UnusedMember.Local
			{
				get
				{
					if (Task.IsCompleted == false)
						return "Etag: " + StartingEtag + ", Age: " + Age + " Results: Pending";

					return "Etag: " + StartingEtag + ", Age: " + Age + " Results: " + Task.Result.Results.Length.ToString("#,#");
				}
			}
		}

		private class JsonResults
		{
			public bool LoadedFromDisk;
			public JsonDocument[] Results;

			public override string ToString()
			{
				if (Results == null)
					return "0";
				return Results.Length.ToString("#,#", CultureInfo.InvariantCulture);
			}
		}

		public bool ShouldSkipDeleteFromIndex(JsonDocument item)
		{
			if (item.SkipDeleteFromIndex == false)
				return false;
			return documentsToRemove.ContainsKey(item.Key) == false;
		}
	}
}