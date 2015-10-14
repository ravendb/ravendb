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
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
	public class PrefetchingBehavior : IDisposable, ILowMemoryHandler
	{
		private class DocAddedAfterCommit
		{
			public Etag Etag;
			public DateTime AddedAt;
		}

		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly BaseBatchSizeAutoTuner autoTuner;
		private readonly WorkContext context;
		private readonly ConcurrentDictionary<string, HashSet<Etag>> documentsToRemove = new ConcurrentDictionary<string, HashSet<Etag>>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentDictionary<Etag, FutureIndexBatch> futureIndexBatches = new ConcurrentDictionary<Etag, FutureIndexBatch>();

		private readonly ConcurrentJsonDocumentSortedList prefetchingQueue = new ConcurrentJsonDocumentSortedList();

		private DocAddedAfterCommit lowestInMemoryDocumentAddedAfterCommit;
		private int currentIndexingAge;
		private string userDescription;

		public Action<int> FutureBatchCompleted = delegate { };

		public PrefetchingBehavior(PrefetchingUser prefetchingUser, WorkContext context, BaseBatchSizeAutoTuner autoTuner, string prefetchingUserDescription)
		{
			this.context = context;
			this.autoTuner = autoTuner;
			PrefetchingUser = prefetchingUser;
			this.userDescription = prefetchingUserDescription;
			MemoryStatistics.RegisterLowMemoryHandler(this);
		}

		public PrefetchingUser PrefetchingUser { get; private set; }

		public string AdditionalInfo { get; set; }

		public bool DisableCollectingDocumentsAfterCommit { get; set; }
		public bool ShouldHandleUnusedDocumentsAddedAfterCommit { get; set; }

		public int InMemoryIndexingQueueSize
		{
			get { return prefetchingQueue.Count; }
		}

		public int InMemoryFutureIndexBatchesSize
		{
			get
			{
				return futureIndexBatches
					.Where(futureIndexBatch => futureIndexBatch.Value.Task.IsCompleted)
					.Sum(futureIndexBatch => futureIndexBatch.Value.Task.Result.Count);
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
			futureIndexBatches.Clear();
		}

		#endregion

		public IDisposable DocumentBatchFrom(Etag etag, out List<JsonDocument> documents)
		{
			documents = GetDocumentsBatchFrom(etag);
			return UpdateCurrentlyUsedBatches(documents);
		}

		public List<JsonDocument> GetDocumentsBatchFrom(Etag etag, int? take = null)
		{
			if (take != null && take.Value <= 0)
				throw new ArgumentException("Take must be greater than 0.");

			HandleCollectingDocumentsAfterCommit(etag);

			var results = GetDocsFromBatchWithPossibleDuplicates(etag, take);
			// a single doc may appear multiple times, if it was updated while we were fetching things, 
			// so we have several versions of the same doc loaded, this will make sure that we will only  
			// take one of them.
			var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			for (int i = results.Count - 1; i >= 0; i--)
			{
				if (CanBeConsideredAsDuplicate(results[i]) && ids.Add(results[i].Key) == false)
				{
					results.RemoveAt(i);
				}
			}
			return results;
		}

		private void HandleCollectingDocumentsAfterCommit(Etag requestedEtag)
		{
			if (ShouldHandleUnusedDocumentsAddedAfterCommit == false)
				return;

			if (DisableCollectingDocumentsAfterCommit)
			{
				if (lowestInMemoryDocumentAddedAfterCommit != null && requestedEtag.CompareTo(lowestInMemoryDocumentAddedAfterCommit.Etag) > 0)
				{
					lowestInMemoryDocumentAddedAfterCommit = null;
					DisableCollectingDocumentsAfterCommit = false;
				}
			}
			else
			{
				if (lowestInMemoryDocumentAddedAfterCommit != null && SystemTime.UtcNow - lowestInMemoryDocumentAddedAfterCommit.AddedAt > TimeSpan.FromMinutes(10))
				{
					DisableCollectingDocumentsAfterCommit = true;
				}
			}
		}

		private void HandleCleanupOfUnusedDocumentsInQueue()
		{
			if (ShouldHandleUnusedDocumentsAddedAfterCommit == false)
				return;

			if (DisableCollectingDocumentsAfterCommit == false)
				return;

			if (lowestInMemoryDocumentAddedAfterCommit == null)
				return;

			prefetchingQueue.RemoveAfter(lowestInMemoryDocumentAddedAfterCommit.Etag);
		}

		private bool CanBeConsideredAsDuplicate(JsonDocument document)
		{
			if (document.Metadata[Constants.RavenReplicationConflict] != null)
				return false;

			return true;
		}

		public bool CanUsePrefetcherToLoadFrom(Etag fromEtag)
		{
			var nextEtagToIndex = GetNextDocEtag(fromEtag);

			var firstEtagInQueue = prefetchingQueue.NextDocumentETag();
			if (firstEtagInQueue == null) // queue is empty, let it use this prefetcher
				return true;

			if (nextEtagToIndex == firstEtagInQueue) // docs for requested etag are already in queue
				return true;

			if (CanLoadDocumentsFromFutureBatches(nextEtagToIndex) != null)
				return true;

			return false;
		}

		private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicates(Etag etag, int? take)
		{
			var result = new List<JsonDocument>();
			bool docsLoaded;
			int prefetchingQueueSizeInBytes;
			var prefetchingDurationTimer = Stopwatch.StartNew();

            // We take an snapshot because the implementation of accessing Values from a ConcurrentDictionary involves a lock.
            // Taking the snapshot should be safe enough. 
            long currentlyUsedBatchSizesInBytes = autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum();
			do
			{
				var nextEtagToIndex = GetNextDocEtag(etag);
				var firstEtagInQueue = prefetchingQueue.NextDocumentETag();

				if (nextEtagToIndex != firstEtagInQueue)
				{
                    // if we have no results, and there is a future batch for it, we would wait for the results
                    // if there are no other results that have been read.
                    if (TryLoadDocumentsFromFutureBatches(nextEtagToIndex, allowWaiting: result.Count == 0) == false)
					{
                        // we don't have a something ready in the future batch, now we need to know if we
                        // have to wait for I/O, or if we can just let the caller get whatever it is that we 
                        // have right now, and schedule another background task to run it.
                        //
                        // The idea is that we'll give you whatever we have right now, and you'll be happy with it, 
                        // and next time you'll call, we'll have something ready
					    if (result.Count > 0)
					    {
                            MaybeAddFutureBatch(result);
					        return result;
					    }
                        // if there has been no results, AND no future batch that we can wait for, then we just load directly from disk
						LoadDocumentsFromDisk(etag, firstEtagInQueue); // here we _intentionally_ use the current etag, not the next one
					}
				}

				docsLoaded = TryGetDocumentsFromQueue(nextEtagToIndex, result, take);

				if (docsLoaded)
					etag = result[result.Count - 1].Etag;

				prefetchingQueueSizeInBytes = prefetchingQueue.LoadedSize;
			} 
			while (
				result.Count < autoTuner.NumberOfItemsToProcessInSingleBatch && 
				(take.HasValue == false || result.Count < take.Value) && 
				docsLoaded &&
				prefetchingDurationTimer.ElapsedMilliseconds <= context.Configuration.PrefetchingDurationLimit &&
                ((prefetchingQueueSizeInBytes + currentlyUsedBatchSizesInBytes) < (context.Configuration.DynamicMemoryLimitForProcessing)));

			return result;
		}

		private void LoadDocumentsFromDisk(Etag etag, Etag untilEtag)
		{
			var jsonDocs = GetJsonDocsFromDisk(etag, untilEtag);

			using (prefetchingQueue.EnterWriteLock())
			{
				foreach (var jsonDocument in jsonDocs)
					prefetchingQueue.Add(jsonDocument);
			}
		}

		private bool TryGetDocumentsFromQueue(Etag nextDocEtag, List<JsonDocument> items, int? take)
		{
			JsonDocument result;

			bool hasDocs = false;

			while (items.Count < autoTuner.NumberOfItemsToProcessInSingleBatch &&
				prefetchingQueue.TryPeek(out result) &&
				// we compare to current or _smaller_ so we will remove from the queue old versions
				// of documents that we have already loaded
				nextDocEtag.CompareTo(result.Etag) >= 0)
			{
				// safe to do peek then dequeue because we are the only one doing the dequeues
				// and here we are single threaded, but still, better to check
				if (prefetchingQueue.TryDequeue(out result) == false)
					continue;

				// this shouldn't happen, but... 
				if (result == null)
					continue;

				if (result.Etag != nextDocEtag)
					continue;

				items.Add(result);
				hasDocs = true;

				if (take.HasValue && items.Count >= take.Value)
					break;

				nextDocEtag = Abstractions.Util.EtagUtil.Increment(nextDocEtag, 1);
			}

			return hasDocs;
		}

		public IEnumerable<JsonDocument> DebugGetDocumentsInPrefetchingQueue()
		{
			return prefetchingQueue.Clone().Values;
		}

		public List<object> DebugGetDocumentsInFutureBatches()
		{
			var result = new List<object>();

			foreach (var futureBatch in futureIndexBatches)
			{
				if (futureBatch.Value.Task.IsCompleted == false)
				{
					result.Add(new
					{
						FromEtag = futureBatch.Key,
						Docs = "Loading documents from disk in progress"
					});

					continue;
				}

				var docs = futureBatch.Value.Task.Result;

				var take = Math.Min(5, docs.Count);

				var etagsWithKeysTail = Enumerable.Range(0, take).Select(
					i => docs[docs.Count - take + i]).ToDictionary(x => x.Etag, x => x.Key);

				result.Add(new
				{
					FromEtag = futureBatch.Key,
					EtagsWithKeysHead = docs.Take(5).ToDictionary(x => x.Etag, x => x.Key),
					EtagsWithKeysTail = etagsWithKeysTail,
					TotalDocsCount = docs.Count
				});
			}

			return result;
		}

		private TaskStatus? CanLoadDocumentsFromFutureBatches(Etag nextDocEtag)
		{
			if (context.Configuration.DisableDocumentPreFetching)
				return null;

			FutureIndexBatch batch;
			if (futureIndexBatches.TryGetValue(nextDocEtag, out batch) == false)
                return null;

		    if (Task.CurrentId == batch.Task.Id)
		        return null;

		    return batch.Task.Status;
		}

		private bool TryLoadDocumentsFromFutureBatches(Etag nextDocEtag, bool allowWaiting)
		{
			try
			{
                switch (CanLoadDocumentsFromFutureBatches(nextDocEtag))
                {
                    case TaskStatus.Created:
                    case TaskStatus.WaitingForActivation:
                    case TaskStatus.WaitingToRun:
                    case TaskStatus.Running:
                    case TaskStatus.WaitingForChildrenToComplete:
                        if (allowWaiting == false)
                            return false;
                        break;
                    case TaskStatus.RanToCompletion:
                        break;
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                    case null:
                        return false;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

			    FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false) // here we need to remove the batch
					return false;

				List<JsonDocument> jsonDocuments = nextBatch.Task.Result;
				using (prefetchingQueue.EnterWriteLock())
				{
					foreach (var jsonDocument in jsonDocuments)
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

            // We take an snapshot because the implementation of accessing Values from a ConcurrentDictionary involves a lock.
            // Taking the snapshot should be safe enough. 
            long currentlyUsedBatchSizesInBytes = autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum();

			context.TransactionalStorage.Batch(actions =>
			{
				//limit how much data we load from disk --> better adhere to memory limits
				var totalSizeAllowedToLoadInBytes =
					(context.Configuration.DynamicMemoryLimitForProcessing) -
                    (prefetchingQueue.LoadedSize + currentlyUsedBatchSizesInBytes);

				// at any rate, we will load a min of 512Kb docs
				var maxSize = Math.Max(
					Math.Min(totalSizeAllowedToLoadInBytes, autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes),
					1024 * 512);

				jsonDocs = actions.Documents
					.GetDocumentsAfter(
						etag,
						autoTuner.NumberOfItemsToProcessInSingleBatch,
						context.CancellationToken,
						maxSize,
						untilEtag,
						autoTuner.FetchingDocumentsFromDiskTimeout
					)
					.Where(x => x != null)
					.Select(doc =>
					{
						JsonDocument.EnsureIdInMetadata(doc);
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
			if (context.Configuration.DisableDocumentPreFetching || context.RunIndexing == false)
				return;
			if (context.Configuration.MaxNumberOfParallelProcessingTasks == 1)
				return;
			if (past.Count == 0)
				return;
			if (prefetchingQueue.LoadedSize > autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes)
				return; // already have too much in memory
			// don't keep _too_ much in memory
			if (prefetchingQueue.Count > context.Configuration.MaxNumberOfItemsToProcessInSingleBatch * 2)
				return;

			var size = 1024;
			var count = context.LastActualIndexingBatchInfo.Count;
			if (count > 0)
			{
				size = context.LastActualIndexingBatchInfo.Aggregate(0, (o, c) => o + c.TotalDocumentCount) / count;
			}

			var alreadyLoadedSizeInBytes = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
					return x.Task.Result.Sum(doc => doc.SerializedSizeOnDisk);

				return size;
			});

			var alreadyLoadedSizeInMb = alreadyLoadedSizeInBytes / 1024 / 1024;
			if (alreadyLoadedSizeInMb > context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
				return;

			if (MemoryStatistics.IsLowMemory)
				return;
			if (futureIndexBatches.Count > 5) // we limit the number of future calls we do
			{
				int alreadyLoaded = futureIndexBatches.Values.Sum(x =>
				{
					if (x.Task.IsCompleted)
						return x.Task.Result.Count;
					return autoTuner.NumberOfItemsToProcessInSingleBatch / 4 * 3;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToProcessInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future caching
			if (MemoryStatistics.AvailableMemoryInMb <
				context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
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
				PrefetchingUser = PrefetchingUser
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
						jsonDocuments = GetJsonDocsFromDisk(Abstractions.Util.EtagUtil.Increment(nextEtag, -1), null);
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
				}).ContinueWith(t =>
				{
					t.AssertNotFailed();

					FutureBatchCompleted(t.Result.Count);
					return t.Result;
				})
			});
		}

		private Etag GetNextDocEtag(Etag etag)
		{
			var oneUpEtag = Abstractions.Util.EtagUtil.Increment(etag, 1);

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
			var highest = Etag.Empty;
			JsonDocument highestDoc = null;
			for (int i = past.Count - 1; i >= 0; i--)
			{
				Etag etag = past[i].Etag;
				if (highest.CompareTo(etag) > 0)
				{
					continue;
				}
				highest = etag;
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
				FutureIndexBatch batch;
				futureIndexBatches.TryRemove(source.StartingEtag, out batch);
			}
		}

		public void AfterStorageCommitBeforeWorkNotifications(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetching || docs.Length == 0 || DisableCollectingDocumentsAfterCommit)
				return;

			if (prefetchingQueue.Count >= // don't use too much, this is an optimization and we need to be careful about using too much mem
				context.Configuration.MaxNumberOfItemsToPreFetch ||
				prefetchingQueue.LoadedSize > context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
				return;

			Etag lowestEtag = null;

			using (prefetchingQueue.EnterWriteLock())
			{
				foreach (var jsonDocument in docs)
				{
					JsonDocument.EnsureIdInMetadata(jsonDocument);
					prefetchingQueue.Add(jsonDocument);

					if (ShouldHandleUnusedDocumentsAddedAfterCommit && (lowestEtag == null || jsonDocument.Etag.CompareTo(lowestEtag) < 0))
					{
						lowestEtag = jsonDocument.Etag;
					}
				}
			}

			if (ShouldHandleUnusedDocumentsAddedAfterCommit && lowestEtag != null)
			{
				if (lowestInMemoryDocumentAddedAfterCommit == null || lowestEtag.CompareTo(lowestInMemoryDocumentAddedAfterCommit.Etag) < 0)
				{
					lowestInMemoryDocumentAddedAfterCommit = new DocAddedAfterCommit
					{
						Etag = lowestEtag,
						AddedAt = SystemTime.UtcNow
					};
				}
			}
		}

		public void CleanupDocuments(Etag lastIndexedEtag)
		{
		    if (lastIndexedEtag == null) return;
		    foreach (var docToRemove in documentsToRemove)
			{
                if(docToRemove.Value == null)
                    continue;
				if (docToRemove.Value.All(etag => lastIndexedEtag.CompareTo(etag) > 0) == false)
					continue;

				HashSet<Etag> _;
				documentsToRemove.TryRemove(docToRemove.Key, out _);
			}

			JsonDocument result;
			while (prefetchingQueue.TryPeek(out result) && lastIndexedEtag.CompareTo(result.Etag) >= 0)
			{
				prefetchingQueue.TryDequeue(out result);
			}

			HandleCleanupOfUnusedDocumentsInQueue();
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
			public Etag StartingEtag;
			public Task<List<JsonDocument>> Task;

		}

		#endregion

		public IDisposable UpdateCurrentlyUsedBatches(List<JsonDocument> docBatch)
		{
			var batchId = Guid.NewGuid();

			autoTuner.CurrentlyUsedBatchSizesInBytes.TryAdd(batchId, docBatch.Sum(x => x.SerializedSizeOnDisk));
			return new DisposableAction(() =>
			{
				long _;
				autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(batchId, out _);
			});
		}

		public void UpdateAutoThrottler(List<JsonDocument> jsonDocs, TimeSpan indexingDuration)
		{
			int currentBatchLength = autoTuner.NumberOfItemsToProcessInSingleBatch;
			int futureLen = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					return x.Task.Result.Count;
				}
				return currentBatchLength / 15;
			});

			long futureSize = futureIndexBatches.Values.Sum(x =>
			{
				if (x.Task.IsCompleted)
				{
					var jsonResults = x.Task.Result;
					return jsonResults.Sum(s => (long)s.SerializedSizeOnDisk);
				}
				return currentBatchLength * 256;
			});

			autoTuner.AutoThrottleBatchSize(
				jsonDocs.Count + futureLen,
				futureSize + jsonDocs.Sum(x => (long)x.SerializedSizeOnDisk),
				indexingDuration);
		}

		public void OutOfMemoryExceptionHappened()
		{
			autoTuner.HandleOutOfMemory();
		}

		public void HandleLowMemory()
		{
			ClearQueueAndFutureBatches();
		}

		public void SoftMemoryRelease()
		{
			
		}

		public LowMemoryHandlerStatistics GetStats()
		{
			var futureIndexBatchesSize = futureIndexBatches.Sum(x => x.Value.Task.IsCompleted ? x.Value.Task.Result.Sum(y => y.SerializedSizeOnDisk) : 0);
			var futureIndexBatchesDocCount = futureIndexBatches.Sum(x => x.Value.Task.IsCompleted ? x.Value.Task.Result.Count : 0);
			return new LowMemoryHandlerStatistics
			{
				Name = "PrefetchingBehavior",
				DatabaseName = context.DatabaseName,
				EstimatedUsedMemory = prefetchingQueue.LoadedSize + futureIndexBatchesSize,
				Metadata = new
				{
					PrefetchingUserType = this.PrefetchingUser,
					PrefetchingUserDescription = userDescription,
					PrefetchingQueueDocCount =prefetchingQueue.Count,
					FutureIndexBatchSizeDocCount = futureIndexBatchesDocCount

				}
			};
		}

		public void ClearQueueAndFutureBatches()
		{
			futureIndexBatches.Clear();
			prefetchingQueue.Clear();
		}
	}
}
