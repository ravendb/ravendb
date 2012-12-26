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

		private readonly ConcurrentDictionary<Guid, InMemoryIndexBatch> inMemoryIndexBatches =
			new ConcurrentDictionary<Guid, InMemoryIndexBatch>();

		private int currentIndexingAge;

		public PrefetchingBehavior(WorkContext context, BaseBatchSizeAutoTuner autoTuner)
		{
			this.context = context;
			this.autoTuner = autoTuner;
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
			var results =
				GetInMemoryJsonDocuments(etag) ??
				GetFutureJsonDocuments(etag) ??
				GetJsonDocsFromDisk(etag);
		
			var jsonDocuments = MergeWithOtherFutureResults(results);
			return jsonDocuments;
		}

		private List<JsonDocument> GetInMemoryJsonDocuments(Guid etag)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return null;
			var nextDocEtag = GetNextDocEtag(etag);
			InMemoryIndexBatch nextBatch;
			if (inMemoryIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false)
				return null;

			lock (nextBatch)
			{
				// make sure that if there are any concurrent updates, it will wait for them
			}

			return nextBatch.Documents;
		}

		private List<JsonDocument> GetFutureJsonDocuments(Guid etag)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing)
				return null;
			try
			{
				Guid nextDocEtag = GetNextDocEtag(etag);
				FutureIndexBatch nextBatch;
				if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false)
					return null;

				if (Task.CurrentId == nextBatch.Task.Id)
					return null;
				return nextBatch.Task.Result;
			}
			catch (Exception e)
			{
				log.WarnException("Error when getting next batch value asyncronously, will try in sync manner", e);
				return null;
			}
		}


		private List<JsonDocument> GetJsonDocsFromDisk(Guid etag)
		{
			Guid? untilEtag = GetNextEtagInMemory(etag);
			
			List<JsonDocument> jsonDocs = null;
			context.TransactionaStorage.Batch(actions =>
			{
				jsonDocs = actions.Documents
					.GetDocumentsAfter(
						etag,
						autoTuner.NumberOfItemsToIndexInSingleBatch,
						autoTuner.MaximumSizeAllowedToFetchFromStorage,
						untilEtag)
					.Where(x => x != null)
					.Select(doc =>
					{
						DocumentRetriever.EnsureIdInMetadata(doc);
						return doc;
					})
					.ToList();
			});
			if(untilEtag == null)
			{
				MaybeAddFutureBatch(jsonDocs);
			}
			return jsonDocs;
		}

		private Guid? GetNextEtagInMemory(Guid etag)
		{
			var current = new ComparableByteArray(etag);
			return inMemoryIndexBatches.Keys.Concat(futureIndexBatches.Keys)
				.Where(x => current.CompareTo(x) < 0)
				.OrderBy(x => x, ByteArrayComparer.Instance)
				.FirstOrDefault();
		}

		private List<JsonDocument> MergeWithOtherFutureResults(List<JsonDocument> results, int timeToWait = 0)
		{
			if (results == null || results.Count == 0)
				return results;

			var nextDocEtag = GetNextDocEtag(GetNextHighestEtag(results));
			while (true)
			{

				InMemoryIndexBatch value;
				if(inMemoryIndexBatches.TryRemove(nextDocEtag, out value))
				{
					lock(value)
					{
						// make sur that there an no current modifications
					}
					results.AddRange(value.Documents);
					nextDocEtag = GetNextDocEtag(value.Documents.Last().Etag.Value);
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

			// a single doc may appear multiple times, if it was updated
			// while we were fetching things, so we have several versions
			// of the same doc loaded, this will make sure that we will only 
			// take one of them.
			return results
				.GroupBy(x => x.Key)
				.Select(g => g.OrderBy(x => x.Etag, ByteArrayComparer.Instance).First())
				.ToList();
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
					return 0;
				});

				if (alreadyLoaded > autoTuner.NumberOfItemsToIndexInSingleBatch)
					return;
			}

			// ensure we don't do TOO much future cachings
			if (MemoryStatistics.AvailableMemory <
				context.Configuration.AvailableMemoryForRaisingIndexBatchSizeLimit)
				return;

			// we loaded the maximum amount, there are probably more items to read now.
			Guid highestLoadedEtag = GetNextHighestEtag(past);
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
						jsonDocuments = GetJsonDocsFromDisk(nextEtag);
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
			if (inMemoryIndexBatches.ContainsKey(oneUpEtag) ||
				futureIndexBatches.ContainsKey(oneUpEtag))
				return oneUpEtag;

			context.TransactionaStorage.Batch(
				accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(highestEtag); });
			return nextDocEtag;
		}

		private static Guid GetNextHighestEtag(List<JsonDocument> past)
		{
			JsonDocument jsonDocument = GetHighestEtag(past);
			if (jsonDocument == null)
				return Guid.Empty;
			return jsonDocument.Etag ?? Guid.Empty;
		}

		public static JsonDocument GetHighestEtag(List<JsonDocument> past)
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

			// make sure that we don't have too much "in memory cache" items
			foreach (InMemoryIndexBatch source in inMemoryIndexBatches.Values.Where(x => (indexingAge - x.Age) > numberOfIndexingGenerationsAllowed).ToList())
			{
				InMemoryIndexBatch _;
				inMemoryIndexBatches.TryRemove(source.StartingEtag, out _);
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

		public void AfterCommit(JsonDocument[] docs)
		{
			if (context.Configuration.DisableDocumentPreFetchingForIndexing || docs.Length == 0)
				return;

			int countOfLoadedItems = inMemoryIndexBatches.Values.Sum(x => x.Documents.Count);
			if (countOfLoadedItems > // don't use too much
				context.Configuration.MaxNumberOfItemsToIndexInSingleBatch)
				return;

			foreach (JsonDocument doc in docs)
			{
				DocumentRetriever.EnsureIdInMetadata(doc);
			}

			if (docs.Length == 1)
			{
				AddInMemoryBatch(docs, 0, 1);
				return;
			}
			int start = 0;
			foreach (var end in SplitNonConsecutiveDocs(docs))
			{
				AddInMemoryBatch(docs, start, end);
				start = end;
			}
		}

		private void AddInMemoryBatch(JsonDocument[] docs, int start, int end)
		{
			var startingEtag = GetLowestEtag(docs, start, end);

			var batchToAdd = inMemoryIndexBatches.Values.FirstOrDefault(x => x.EndEtag == startingEtag);
			var actualDocsToTake = docs.Skip(start).Take(end - start);
			if (batchToAdd != null)
			{
				lock (batchToAdd)
				{
					batchToAdd.Documents.AddRange(actualDocsToTake);
					batchToAdd.EndEtag = Etag.Increment(docs[end - 1].Etag.Value, 1);
				}
				return;
			}

			var inMemoryIndexBatch = new InMemoryIndexBatch
			{
				Age = Thread.VolatileRead(ref currentIndexingAge),
				Documents = new List<JsonDocument>(actualDocsToTake),
				StartingEtag = startingEtag
			};
			inMemoryIndexBatch.EndEtag = Etag.Increment(inMemoryIndexBatch.Documents.Last().Etag.Value, 1);
			inMemoryIndexBatches.TryAdd(startingEtag, inMemoryIndexBatch);
		}

		private IEnumerable<int> SplitNonConsecutiveDocs(JsonDocument[] docs)
		{
			var etag = docs[0].Etag.Value;
			var doc = docs[0];
			for (int i = 1; i < docs.Length; i++)
			{
				if (Etag.GetDiffrence(doc.Etag.Value, etag) != 1)
				{
					yield return i;
				}
			}
			yield return docs.Length;
		}


		private static Guid GetLowestEtag(JsonDocument[] past, int start, int end)
		{
			var lowest = new ComparableByteArray(past[start].Etag.Value);
			for (int i = start + 1; i < end; i++)
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