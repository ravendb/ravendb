// -----------------------------------------------------------------------
//  <copyright file="PrefetchingBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly BaseBatchSizeAutoTuner autoTuner;
		private readonly WorkContext context;
		private readonly ConcurrentDictionary<string, HashSet<Guid>> documentsToRemove =
			new ConcurrentDictionary<string, HashSet<Guid>>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentDictionary<Guid, FutureIndexBatch> futureIndexBatches =
			new ConcurrentDictionary<Guid, FutureIndexBatch>();

		private readonly ConcurrentQueue<JsonDocument> inMemoryDocs =
			new ConcurrentQueue<JsonDocument>();

		private int currentIndexingAge;

		public PrefetchingBehavior(WorkContext context, BaseBatchSizeAutoTuner autoTuner)
		{
			this.context = context;
			this.autoTuner = autoTuner;
		}

		public int InMemoryIndexingQueueSize
		{
			get { return inMemoryDocs.Count; }
		}

		#region IDisposable Members

		public void Dispose()
		{
			Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		#endregion

		public List<JsonDocument> GetDocumentsBatchFrom(Guid etag)
		{
			var results = GetDocsFromBatchWithPossibleDuplicates(etag);
			// a single doc may appear multiple times, if it was updated while we were fetching things, 
			// so we have several versions of the same doc loaded, this will make sure that we will only  
			// take one of them.
			var ids = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			for (int i = results.Count - 1; i >= 0; i--)
			{
				if(ids.Add(results[i].Key) == false)
				{
					results.RemoveAt(i);
				}
			}
			return results;
		}

		private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicates(Guid etag)
		{
			var inMemResults = new List<JsonDocument>();
			var nextDocEtag = GetNextDocEtag(etag);
			if (TryGetInMemoryJsonDocuments(nextDocEtag, inMemResults))
				return inMemResults;

			var results =
				GetFutureJsonDocuments(nextDocEtag) ??
				GetJsonDocsFromDisk(etag); // here we _intentionally_ using the current etag, not the next one

			return MergeWithOtherFutureResults(results);
		}

		private bool TryGetInMemoryJsonDocuments(Guid nextDocEtag, List<JsonDocument> items)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return false;

			JsonDocument result;
			bool hasDocs = false;
			while (inMemoryDocs.TryPeek(out result)  &&
				ComparableByteArray.CompareTo(nextDocEtag.ToByteArray(),result.Etag.Value.ToByteArray()) >= 0)
			{
				// safe to do peek then dequeue because we are the only one doing the dequeues
				// and here we are single threaded
				inMemoryDocs.TryDequeue(out result);

				if (result.Etag.Value != nextDocEtag)
					continue;

				items.Add(result);
				hasDocs = true;
				nextDocEtag = Etag.Increment(nextDocEtag, 1);
			}
			return hasDocs;
		}

		private List<JsonDocument> GetFutureJsonDocuments(Guid nextDocEtag)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return null;
			try
			{
				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false)
					return null;

				if (Task.CurrentId == nextBatch.Task.Id)
					return null;
				return nextBatch.Task.Result;
			}
			catch (Exception e)
			{
				log.WarnException("Error when getting next batch value asynchronously, will try in sync manner", e);
				return null;
			}
		}


		private List<JsonDocument> GetJsonDocsFromDisk(Guid etag)
		{
			List<JsonDocument> jsonDocs = null;
			var untilEtag = GetNextEtagInMemory();
			context.TransactionalStorage.Batch(actions =>
			{
				jsonDocs = actions.Documents
					.GetDocumentsAfter(
						etag,
						autoTuner.NumberOfItemsToIndexInSingleBatch,
						autoTuner.MaximumSizeAllowedToFetchFromStorage,
						untilEtag: untilEtag)
					.Where(x => x != null)
					.Select(doc =>
					{
						DocumentRetriever.EnsureIdInMetadata(doc);
						return doc;
					})
					.ToList();
			});
			if (untilEtag == null)
			{
				MaybeAddFutureBatch(jsonDocs);
			}
			return jsonDocs;
		}

		private Guid? GetNextEtagInMemory()
		{
			JsonDocument result;
			if (inMemoryDocs.TryPeek(out result) == false)
				return null;
			return result.Etag;
		}

		private List<JsonDocument> MergeWithOtherFutureResults(List<JsonDocument> results, int timeToWait = 0)
		{
			if (results == null || results.Count == 0)
				return results;

			var nextDocEtag = GetNextDocEtag(GetHighestEtag(results));
			while (results.Count < (autoTuner.MaximumSizeAllowedToFetchFromStorage / 4) * 3) // we won't be merging if we have more than 3/4 of max already
			{

				if (TryGetInMemoryJsonDocuments(nextDocEtag, results))
				{
					nextDocEtag = GetNextDocEtag(results.Last().Etag.Value);
					continue;
				}

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

				results.AddRange(nextBatch.Task.Result);
				nextDocEtag = GetNextDocEtag(nextBatch.Task.Result.Last().Etag.Value);
			}
			return results;
		}

		private void MaybeAddFutureBatch(List<JsonDocument> past)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || context.RunIndexing == false)
				return;
			if (context.Configuration.MaxNumberOfParallelIndexTasks == 1)
				return;
			if (past.Count == 0)
				return;
			if (futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				int alreadyLoaded = futureIndexBatches.Values.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Count;
					return autoTuner.NumberOfItemsToIndexInSingleBatch / 4 * 3;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToIndexInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future caching
			if (MemoryStatistics.AvailableMemory <
				context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			Guid highestLoadedEtag = GetHighestEtag(past);
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
					List<JsonDocument> jsonDocuments = null;
					int localWork = 0;
					while (context.RunIndexing)
					{
						jsonDocuments = GetJsonDocsFromDisk(highestLoadedEtag);
						if (jsonDocuments.Count > 0)
							break;

						futureBatchStat.Retries++;

						context.WaitForWork(TimeSpan.FromMinutes(10), ref localWork, "PreFetching");
					}
					futureBatchStat.Duration = sp.Elapsed;
					futureBatchStat.Size = jsonDocuments == null ? 0 : jsonDocuments.Count;
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
			var nextDocEtag = highestEtag;

			var oneUpEtag = Etag.Increment(nextDocEtag, 1);

			// no need to go to disk to find the next etag if we already have it in memory
			JsonDocument next;
			if (inMemoryDocs.TryPeek(out next) && next.Etag == oneUpEtag ||
				futureIndexBatches.ContainsKey(oneUpEtag))
				return oneUpEtag;

			context.TransactionalStorage.Batch(
				accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(highestEtag); });
			return nextDocEtag;
		}

		private static Guid GetHighestEtag(List<JsonDocument> past)
		{
			JsonDocument jsonDocument = GetHighestJsonDocumentByEtag(past);
			if (jsonDocument == null)
				return Guid.Empty;
			return jsonDocument.Etag ?? Guid.Empty;
		}

		public static JsonDocument GetHighestJsonDocumentByEtag(List<JsonDocument> past)
		{
			var highest = new ComparableByteArray(Guid.Empty);
			JsonDocument highestDoc = null;
			for (int i = past.Count - 1; i >= 0; i--)
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
					log.WarnException("Error happened on discarded future work batch", task.Exception);
				}
				else
				{
					log.Warn("WASTE: Discarding future work item without using it, to reduce memory usage");
				}
			});
		}

		public void BatchProcessingComplete()
		{
			int indexingAge = Interlocked.Increment(ref currentIndexingAge);

			// make sure that we don't have too much "future cache" items
			const int numberOfIndexingGenerationsAllowed = 64;
			foreach (FutureIndexBatch source in futureIndexBatches.Values.Where(x => (indexingAge - x.Age) > numberOfIndexingGenerationsAllowed).ToList())
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
					return x.Task.Result.Count;
				}
				return currentBatchSize / 15;
			});

			futureSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					List<JsonDocument> jsonResults = x.Task.Result;
					return jsonResults.Sum(s => s.SerializedSizeOnDisk);
				}
				return currentBatchSize * 256;
			});
		}

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || docs.Length == 0)
				return;

			if (inMemoryDocs.Count > // don't use too much, this is an optimization and we need to be careful about using too much mem
				context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
				return;

			foreach (var jsonDocument in docs)
			{
				DocumentRetriever.EnsureIdInMetadata(jsonDocument);
				inMemoryDocs.Enqueue(jsonDocument);
			}
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

			JsonDocument result;
			while (inMemoryDocs.TryPeek(out result) && highest.CompareTo(result.Etag.Value) >= 0)
			{
				inMemoryDocs.TryDequeue(out result);
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

		public bool ShouldSkipDeleteFromIndex(JsonDocument item)
		{
			if (item.SkipDeleteFromIndex == false)
				return false;
			return documentsToRemove.ContainsKey(item.Key) == false;
		}

		#region Nested type: FutureIndexBatch

		private class FutureIndexBatch
		{
			public int Age;
			public Guid StartingEtag;
			public Task<List<JsonDocument>> Task;
		}

		#endregion

		#region Nested type: InMemoryIndexBatch

		private class InMemoryIndexBatch
		{
			public int Age;
			public List<JsonDocument> Documents;
			public Guid StartingEtag;
			public Guid EndEtag;
		}

		#endregion
	}
}