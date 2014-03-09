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
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
	using Util;

	public class PrefetchingBehavior : IDisposable
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly BaseBatchSizeAutoTuner autoTuner;
		private readonly WorkContext context;
		private readonly ConcurrentDictionary<string, HashSet<Etag>> documentsToRemove =
			new ConcurrentDictionary<string, HashSet<Etag>>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ReaderWriterLockSlim updatedDocumentsLock = new ReaderWriterLockSlim();
		private readonly SortedKeyList<Etag> updatedDocuments = new SortedKeyList<Etag>();

		private readonly ConcurrentDictionary<Etag, FutureIndexBatch> futureIndexBatches =
			new ConcurrentDictionary<Etag, FutureIndexBatch>();

		private readonly ConcurrentJsonDocumentSortedList prefetchingQueue = new ConcurrentJsonDocumentSortedList();

		private int currentIndexingAge;

		public PrefetchingBehavior(WorkContext context, BaseBatchSizeAutoTuner autoTuner)
		{
			this.context = context;
			this.autoTuner = autoTuner;
		}

		public int InMemoryIndexingQueueSize
		{
			get { return prefetchingQueue.Count; }
		}

		#region IDisposable Members

		public void Dispose()
		{
			Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		#endregion

		public List<JsonDocument> GetDocumentsBatchFrom(Etag etag)
		{
			var results = GetDocsFromBatchWithPossibleDuplicates(etag);
			// a single doc may appear multiple times, if it was updated while we were fetching things, 
			// so we have several versions of the same doc loaded, this will make sure that we will only  
			// take one of them.
			var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			for (int i = results.Count - 1; i >= 0; i--)
			{
				if(CanBeConsideredAsDuplicate(results[i]) && ids.Add(results[i].Key) == false)
				{
					results.RemoveAt(i);
				}
			}
			return results;
		}

		private bool CanBeConsideredAsDuplicate(JsonDocument document)
		{
			if (document.Metadata[Constants.RavenReplicationConflict] != null)
				return false;

			return true;
		}

		private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicates(Etag etag)
		{
			var result = new List<JsonDocument>();
			bool docsLoaded;
			do
			{
				var nextEtagToIndex = GetNextDocEtag(etag);
				var firstEtagInQueue = prefetchingQueue.NextDocumentETag();

				if (nextEtagToIndex != firstEtagInQueue)
				{
					if (TryLoadDocumentsFromFutureBatches(nextEtagToIndex) == false)
					{
						LoadDocumentsFromDisk(etag, firstEtagInQueue); // here we _intentionally_ use the current etag, not the next one
					}
				}

				docsLoaded = TryGetDocumentsFromQueue(nextEtagToIndex, ref result);

				if (docsLoaded)
					etag = result[result.Count - 1].Etag;

			} while (result.Count < autoTuner.NumberOfItemsToIndexInSingleBatch && docsLoaded);
			

			return result;
		}

		private void LoadDocumentsFromDisk(Etag etag, Etag untilEtag)
		{
			var jsonDocs = GetJsonDocsFromDisk(etag, untilEtag);
			
			foreach (var jsonDocument in jsonDocs)
			{
				prefetchingQueue.Add(jsonDocument);
			}
		}

		private bool TryGetDocumentsFromQueue(Etag nextDocEtag, ref List<JsonDocument> items)
		{
			JsonDocument result;

			nextDocEtag = HandleEtagGapsIfNeeded(nextDocEtag);
			bool hasDocs = false;

			while (items.Count < autoTuner.NumberOfItemsToIndexInSingleBatch && 
				prefetchingQueue.TryPeek(out result) && 
				nextDocEtag.CompareTo(result.Etag) >= 0)
			{
				// safe to do peek then dequeue because we are the only one doing the dequeues
				// and here we are single threaded, but still, better to check
				if (prefetchingQueue.TryDequeue(out result) == false)
					continue;

                // this shouldn't happen, but... 
                if(result == null)
                    continue;

				if (result.Etag != nextDocEtag)
					continue;

				items.Add(result);
				hasDocs = true;

				nextDocEtag = EtagUtil.Increment(nextDocEtag, 1);
				nextDocEtag = HandleEtagGapsIfNeeded(nextDocEtag);
			}

			return hasDocs;
		}

		private bool TryLoadDocumentsFromFutureBatches(Etag nextDocEtag)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return false;
			try
			{
				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false)
					return false;

				if (Task.CurrentId == nextBatch.Task.Id)
					return false;

				foreach (var jsonDocument in nextBatch.Task.Result)
				{
					prefetchingQueue.Add(jsonDocument);
				}

				return true;
			}
			catch (Exception e)
			{
				log.WarnException("Error when getting next batch value asynchronously, will try in sync manner", e);
				return false;
			}
		}

		private List<JsonDocument> GetJsonDocsFromDisk(Etag etag, Etag untilEtag)
		{
			List<JsonDocument> jsonDocs = null;

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
			Etag highestLoadedEtag = GetHighestEtag(past);
			Etag nextEtag = GetNextDocumentEtagFromDisk(highestLoadedEtag);

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
						jsonDocuments = GetJsonDocsFromDisk(EtagUtil.Increment(nextEtag, -1), null);
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

		private Etag GetNextDocEtag(Etag etag)
		{
			var oneUpEtag = EtagUtil.Increment(etag, 1);

			// no need to go to disk to find the next etag if we already have it in memory
			if (prefetchingQueue.NextDocumentETag() == oneUpEtag)
				return oneUpEtag;

			return GetNextDocumentEtagFromDisk(etag);
		}

		private Etag GetNextDocumentEtagFromDisk(Etag etag)
		{
			Etag nextDocEtag = null;
			context.TransactionalStorage.Batch(
				accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(etag); });

			return nextDocEtag;
		}

		private static Etag GetHighestEtag(List<JsonDocument> past)
		{
			JsonDocument jsonDocument = GetHighestJsonDocumentByEtag(past);
			if (jsonDocument == null)
				return Etag.Empty;
			return jsonDocument.Etag ?? Etag.Empty;
		}

		public static JsonDocument GetHighestJsonDocumentByEtag(List<JsonDocument> past)
		{
			var highest = new ComparableByteArray(Etag.Empty);
			JsonDocument highestDoc = null;
			for (int i = past.Count - 1; i >= 0; i--)
			{
				Etag etag = past[i].Etag;
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

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || docs.Length == 0)
				return;

			// don't use too much, this is an optimization and we need to be careful about using too much mem
			if (prefetchingQueue.Count >=  context.Configuration.MaxNumberOfItemsToPreFetchForIndexing)
				return;

			foreach (var jsonDocument in docs)
			{
				DocumentRetriever.EnsureIdInMetadata(jsonDocument);
				prefetchingQueue.Add(jsonDocument);
			}
		}

		public void CleanupDocuments(Etag lastIndexedEtag)
		{
			foreach (var docToRemove in documentsToRemove)
			{
				if (docToRemove.Value.All(etag => lastIndexedEtag.CompareTo(etag) > 0) == false)
					continue;

				HashSet<Etag> _;
				documentsToRemove.TryRemove(docToRemove.Key, out _);
			}

			updatedDocumentsLock.EnterWriteLock();
			try
			{
				updatedDocuments.RemoveSmallerOrEqual(lastIndexedEtag);
			}
			finally
			{
				updatedDocumentsLock.ExitWriteLock();
			}

			JsonDocument result;
			while (prefetchingQueue.TryPeek(out result) && lastIndexedEtag.CompareTo(result.Etag) >= 0)
			{
				prefetchingQueue.TryDequeue(out result);
			}
		}

		public bool FilterDocuments(JsonDocument document)
		{
			HashSet<Etag> etags;
			return (documentsToRemove.TryGetValue(document.Key, out etags) && etags.Any(x => x.CompareTo(document.Etag) >= 0)) == false;
		}

		public void AfterDelete(string key, Etag deletedEtag)
		{
			documentsToRemove.AddOrUpdate(key, s => new HashSet<Etag> { deletedEtag },
										  (s, set) => new HashSet<Etag>(set) { deletedEtag });
		}

		public void AfterUpdate(string key, Etag etagBeforeUpdate)
		{
			updatedDocumentsLock.EnterWriteLock();
			try
			{
			    updatedDocuments.Add(etagBeforeUpdate);
			}
			finally
			{
				updatedDocumentsLock.ExitWriteLock();
			}
		}

		public bool ShouldSkipDeleteFromIndex(JsonDocument item)
		{
			if (item.SkipDeleteFromIndex == false)
				return false;
			return documentsToRemove.ContainsKey(item.Key) == false;
		}

		private Etag HandleEtagGapsIfNeeded(Etag nextEtag)
		{
			if (nextEtag != prefetchingQueue.NextDocumentETag())
			{
				nextEtag = SkipDeletedEtags(nextEtag);
				nextEtag = SkipUpdatedEtags(nextEtag);
			}

			return nextEtag;
		}

		private Etag SkipDeletedEtags(Etag nextEtag)
		{
			while (documentsToRemove.Any(x => x.Value.Contains(nextEtag)))
			{
				nextEtag = EtagUtil.Increment(nextEtag, 1);
			}

			return nextEtag;
		}

		private Etag SkipUpdatedEtags(Etag nextEtag)
		{
			updatedDocumentsLock.EnterReadLock();
			try
			{
				var enumerator = updatedDocuments.GetEnumerator();

				// here we relay on the fact that the updated docs collection is sorted
				while (enumerator.MoveNext() && enumerator.Current.CompareTo(nextEtag) == 0)
				{
					nextEtag = EtagUtil.Increment(nextEtag, 1);
				}
			}
			finally
			{
				updatedDocumentsLock.ExitReadLock();
			}

			return nextEtag;
		}

		#region Nested type: FutureIndexBatch

		private class FutureIndexBatch
		{
			public int Age;
			public Etag StartingEtag;
			public Task<List<JsonDocument>> Task;
		}

		#endregion

		public void UpdateAutoThrottler(List<JsonDocument> jsonDocs, TimeSpan indexingDuration)
		{
			int currentBatchSize = autoTuner.NumberOfItemsToIndexInSingleBatch;
			int futureLen = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					return x.Task.Result.Count;
				}
				return currentBatchSize / 15;
			});

			long futureSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					var jsonResults = x.Task.Result;
					return jsonResults.Sum(s => (long)s.SerializedSizeOnDisk);
				}
				return currentBatchSize * 256;
			});
			autoTuner.AutoThrottleBatchSize(
				jsonDocs.Count + futureLen, 
				futureSize + jsonDocs.Sum(x => (long)x.SerializedSizeOnDisk),
			    indexingDuration);
		}
	}
}
