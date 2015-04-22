//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
	public class IndexingExecuter : AbstractIndexingExecuter
	{
		private readonly ConcurrentSet<PrefetchingBehavior> prefetchingBehaviors = new ConcurrentSet<PrefetchingBehavior>();
		private readonly Prefetcher prefetcher;
		private readonly PrefetchingBehavior defaultPrefetchingBehavior;
		
		public IndexingExecuter(WorkContext context, Prefetcher prefetcher, IndexReplacer indexReplacer)
			: base(context, indexReplacer)
		{
			autoTuner = new IndexBatchSizeAutoTuner(context);
			this.prefetcher = prefetcher;
			defaultPrefetchingBehavior = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner);
			defaultPrefetchingBehavior.ShouldHandleUnusedDocumentsAddedAfterCommit = true;
			prefetchingBehaviors.TryAdd(defaultPrefetchingBehavior);
		}

		public List<PrefetchingBehavior> PrefetchingBehaviors
		{
			get { return prefetchingBehaviors.ToList(); }
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
		{
			var isStale = actions.Staleness.IsMapStale(indexesStat.Id);
			var indexingPriority = indexesStat.Priority;
			if (isStale == false)
				return false;

			if (indexingPriority == IndexingPriority.None)
				return true;


			if ((indexingPriority & IndexingPriority.Normal) == IndexingPriority.Normal)
			{
				onlyFoundIdleWork.Value = false;
				return true;
			}

			if ((indexingPriority & (IndexingPriority.Disabled | IndexingPriority.Error)) != IndexingPriority.None)
				return false;

			if (isIdle == false)
				return false; // everything else is only valid on idle runs

			if ((indexingPriority & IndexingPriority.Idle) == IndexingPriority.Idle)
				return true;

			if ((indexingPriority & IndexingPriority.Abandoned) == IndexingPriority.Abandoned)
			{
				var timeSinceLastIndexing = (SystemTime.UtcNow - indexesStat.LastIndexingTime);

				return (timeSinceLastIndexing > context.Configuration.TimeToWaitBeforeRunningAbandonedIndexes);
			}

			throw new InvalidOperationException("Unknown indexing priority for index " + indexesStat.Id + ": " + indexesStat.Priority);
		}


		protected override void UpdateStalenessMetrics(int staleCount)
		{
			context.MetricsCounters.StaleIndexMaps.Update(staleCount);
		}

		protected override bool ShouldSkipIndex(Index index)
		{
			return index.IsTestIndex;
		}

		protected override DatabaseTask GetApplicableTask(IStorageActionsAccessor actions)

		{
			var removeFromIndexTasks = (DatabaseTask) actions.Tasks.GetMergedTask<RemoveFromIndexTask>();
			var touchReferenceDocumentIfChangedTask = removeFromIndexTasks ?? actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>();


			return touchReferenceDocumentIfChangedTask;
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushMapIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexId = indexesStat.Id,
				LastIndexedEtag = indexesStat.LastIndexedEtag,
				LastIndexedTimestamp = indexesStat.LastIndexedTimestamp
			};
		}


		private class IndexingGroup : IDisposable
		{
			public Etag LastIndexedEtag;
			public DateTime? LastQueryTime;

			public List<IndexToWorkOn> Indexes;
			public PrefetchingBehavior PrefetchingBehavior;
			public List<JsonDocument> JsonDocs;
			private IDisposable prefetchDisposable;
			public IndexingBatchInfo BatchInfo { get; set; }
			private int disposed = 0;
			private int indexedAmount = 0;

			public event Action<IndexingGroup> IndexingGroupProcessingFinished;


			public void SignalIndexingComplete()
			{
				if (Interlocked.Increment(ref indexedAmount) == Indexes.Count && IndexingGroupProcessingFinished != null)
				{
					IndexingGroupProcessingFinished(this);
				}
			}

			public void ReleaseIndexingGroupFinished()
			{
				IndexingGroupProcessingFinished = null;
			}

			public void PrefetchDocuments()
			{
				/*prefetchDisposable  = new WeakReference<IDisposable>(
					PrefetchingBehavior.DocumentBatchFrom(LastIndexedEtag, out JsonDocs));*/
				prefetchDisposable =
					PrefetchingBehavior.DocumentBatchFrom(LastIndexedEtag, out JsonDocs);
			}

			~IndexingGroup()
			{
				if (Thread.VolatileRead(ref disposed) == 0)
				{
					this.Dispose();
				}
			}

			public void Dispose()
			{
				if (prefetchDisposable != null)
				{
					IDisposable prefetcherDisposableValue;
					/*if (prefetchDisposable.TryGetTarget(out prefetcherDisposableValue))
					{
						prefetcherDisposableValue.Dispose();
					}*/
					prefetchDisposable.Dispose();
				}
				Interlocked.Increment(ref disposed);
			}
		}

		public class IndexingBatchOperation
		{
			public IndexingBatchForIndex IndexingBatch { get; set; }
			public Etag LastEtag { get; set; }
			public DateTime LastModified { get; set; }
			public IndexingBatchInfo IndexingBatchInfo { get; set; }
		}


		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexes)
		{
			ConcurrentSet<PrefetchingBehavior> usedPrefetchers;
			List<IndexingGroup> groupedIndexes;
			var completedGroups = 0;

			if (GenerateIndexingGroupsByEtagRanges(indexes, out usedPrefetchers, out groupedIndexes))
			{
				return;
			}


			foreach (var indexToWorkOn in indexes)
			{
				indexToWorkOn.Index.IsMapIndexingInProgress = true;
			}

			var indexingAutoTunerContext = ((IndexBatchSizeAutoTuner) autoTuner).ConsiderLimitingNumberOfItemsToProcessForThisBatch(
				groupedIndexes.Max(x => x.Indexes.Max(y => y.Index.MaxIndexOutputsPerDocument)),
				groupedIndexes.Any(x => x.Indexes.Any(y => y.Index.IsMapReduce)));
			indexes.ForEach(x => x.Index.CurrentNumberOfItemsToIndexInSingleBatch = autoTuner.NumberOfItemsToProcessInSingleBatch);
			using (indexingAutoTunerContext)
			{
				var indexBatchOperations = new ConcurrentDictionary<IndexingBatchOperation, object>();

				var operationWasCancelled = GenerateIndexingBatchesAndPrefetchDocuments(groupedIndexes, indexBatchOperations);

				var executionStopwatch = Stopwatch.StartNew();

				foreach (var indexingGroup in groupedIndexes)
				{
					indexingGroup.IndexingGroupProcessingFinished += x =>
					{
						if (!operationWasCancelled)
						{
							ReleasePrefethersAndUpdateStatistics(x, executionStopwatch.Elapsed);
						}

						if (Interlocked.Increment(ref completedGroups) == groupedIndexes.Count)
						{
							RemoveUnusedPrefetchers(usedPrefetchers);
						}
					};
				}

				if (!operationWasCancelled)
					operationWasCancelled = PerformIndexingOnIndexBatches(indexBatchOperations);
			}
		}

		private void ReleasePrefethersAndUpdateStatistics(IndexingGroup indexingGroup, TimeSpan ellapsedTimeSpan)
		{
			if (indexingGroup.JsonDocs != null && indexingGroup.JsonDocs.Count > 0)
			{
				indexingGroup.PrefetchingBehavior.CleanupDocuments(indexingGroup.LastIndexedEtag);
				indexingGroup.PrefetchingBehavior.UpdateAutoThrottler(indexingGroup.JsonDocs, ellapsedTimeSpan);
				indexingGroup.PrefetchingBehavior.BatchProcessingComplete();
				context.ReportIndexingBatchCompleted(indexingGroup.BatchInfo);
			}
			indexingGroup.ReleaseIndexingGroupFinished();
		}

		private void ReleasePrefethersAndUpdateStatistics(List<IndexingGroup> groupedIndexes, TimeSpan ellapsedTimeSpan)
		{
			foreach (var indexingGroup in groupedIndexes)
			{
				if (indexingGroup.JsonDocs != null && indexingGroup.JsonDocs.Count > 0)
				{
					indexingGroup.PrefetchingBehavior.CleanupDocuments(indexingGroup.LastIndexedEtag);
					indexingGroup.PrefetchingBehavior.UpdateAutoThrottler(indexingGroup.JsonDocs, ellapsedTimeSpan);
					indexingGroup.PrefetchingBehavior.BatchProcessingComplete();
					context.ReportIndexingBatchCompleted(indexingGroup.BatchInfo);
				}
			}
		}


		private bool PerformIndexingOnIndexBatches(ConcurrentDictionary<IndexingBatchOperation, object> indexBatchOperations)
		{
			bool operationWasCancelled = false;
			try
			{
				context.MetricsCounters.IndexedPerSecond.Mark(indexBatchOperations.Keys.Count);


				var executedPartially = 0;
				context.Database.MappingThreadPool.ExecuteBatch(indexBatchOperations.Keys.ToList(),
					indexBatchOperation =>
					{
						context.CancellationToken.ThrowIfCancellationRequested();
						using (LogContext.WithDatabase(context.DatabaseName))
						{
							try
							{
								var performance = HandleIndexingFor(indexBatchOperation.IndexingBatch, indexBatchOperation.LastEtag, indexBatchOperation.LastModified, CancellationToken.None);

								if (performance != null)
									indexBatchOperation.IndexingBatchInfo.PerformanceStats.TryAdd(indexBatchOperation.IndexingBatch.Index.PublicName, performance);

								if (Thread.VolatileRead(ref executedPartially) == 1)
								{
									context.NotifyAboutWork();
								}
							}
							catch (InvalidDataException e)
							{
								Log.ErrorException("Failed to index because of data corruption. ", e);
								context.AddError(indexBatchOperation.IndexingBatch.IndexId, indexBatchOperation.IndexingBatch.Index.PublicName, null, string.Format("Failed to index because of data corruption. Reason: {0}", e.Message));
							}
						}
					}, allowPartialBatchResumption: MemoryStatistics.AvailableMemory > 1.5*context.Configuration.MemoryLimitForProcessingInMb, description: "Executing Map Indexing");
				Interlocked.Increment(ref executedPartially);
			}
			catch (InvalidDataException e)
			{
				Log.ErrorException("Failed to index because of data corruption. ", e);
				indexBatchOperations.Keys.ForEach(indexBatch =>
					context.AddError(indexBatch.IndexingBatch.Index.IndexId, indexBatch.IndexingBatch.Index.PublicName, null, string.Format("Failed to index because of data corruption. Reason: {0}", e.Message)));
			}
			catch (OperationCanceledException)
			{
				operationWasCancelled = true;
			}


			return operationWasCancelled;
		}


		private bool GenerateIndexingBatchesAndPrefetchDocuments(List<IndexingGroup> groupedIndexes, ConcurrentDictionary<IndexingBatchOperation, object> indexBatchOperations)
		{
			bool operationWasCancelled = false;
			context.Database.MappingThreadPool.ExecuteBatch(groupedIndexes,
				indexingGroup =>
				{
					try
					{
						indexingGroup.PrefetchDocuments();
						var curGroupJsonDocs = indexingGroup.JsonDocs;
						if (Log.IsDebugEnabled)
						{
							Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
								curGroupJsonDocs.Count, indexingGroup.LastIndexedEtag, string.Join(", ", curGroupJsonDocs.Select(x => x.Key)));
						}


						indexingGroup.BatchInfo =
							context.ReportIndexingBatchStarted(curGroupJsonDocs.Count,
								curGroupJsonDocs.Sum(x => x.SerializedSizeOnDisk),
								indexingGroup.Indexes.Select(x => x.Index.PublicName).ToList());


						context.CancellationToken.ThrowIfCancellationRequested();
						var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(curGroupJsonDocs);
						var lastModified = lastByEtag.LastModified.Value;
						var lastEtag = lastByEtag.Etag;
						List<IndexToWorkOn> filteredOutIndexes;
						var indexBatches = FilterIndexes(indexingGroup.Indexes, curGroupJsonDocs, lastEtag, out filteredOutIndexes).OrderByDescending(x => x.Index.LastQueryTime).ToList();

						foreach (var filteredOutIndex in filteredOutIndexes)
						{
							indexingGroup.SignalIndexingComplete();
							filteredOutIndex.Index.IsMapIndexingInProgress = false;
						}


						foreach (var indexBatch in indexBatches)
						{
							var indexingBatchOperation = new IndexingBatchOperation
							{
								IndexingBatch = indexBatch,
								LastEtag = lastEtag,
								LastModified = lastModified,
								IndexingBatchInfo = indexingGroup.BatchInfo
							};
							if (indexBatchOperations.TryAdd(indexingBatchOperation, new object()))
							{
								indexingBatchOperation.IndexingBatch.OnIndexingComplete += indexingGroup.SignalIndexingComplete;
							}
						}
					}
					catch (OperationCanceledException)
					{
						operationWasCancelled = true;
					}
					catch (InvalidDataException e)
					{
						Log.ErrorException("Failed to index because of data corruption. ", e);
						indexingGroup.Indexes.ForEach(index =>
							Log.ErrorException("Failed to index because of data corruption. ", e));
					}
				}, description: "Prefatching Index Groups");
			return operationWasCancelled;
		}


		private bool GenerateIndexingGroupsByEtagRanges(IList<IndexToWorkOn> indexes, out ConcurrentSet<PrefetchingBehavior> usedPrefetchers, out List<IndexingGroup> indexingGroups)
		{
			usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();
			indexingGroups = new List<IndexingGroup>();
			usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();

			var groupedIndexesByEtagRange = context.Configuration.IndexingClassifier.GroupMapIndexes(indexes);
			if (groupedIndexesByEtagRange.Count == 0)
				return true;

			groupedIndexesByEtagRange = groupedIndexesByEtagRange.OrderByDescending(x => x.Key).ToDictionary(x => x.Key, x => x.Value);

			foreach (var indexingGroup in groupedIndexesByEtagRange)
			{
				var result = new IndexingGroup
				{
					Indexes = indexingGroup.Value,
					LastIndexedEtag = indexingGroup.Key,
					LastQueryTime = indexingGroup.Value.Max(y => y.Index.LastQueryTime),
					PrefetchingBehavior = GetPrefetcherFor(indexingGroup.Key, usedPrefetchers)
				};

				result.PrefetchingBehavior.AdditionalInfo = string.Format("Default prefetcher: {0}. For indexing group: [Indexes: {1}, LastIndexedEtag: {2}]",
					result.PrefetchingBehavior == defaultPrefetchingBehavior, string.Join(", ", result.Indexes.Select(y => y.Index.PublicName)), result.LastIndexedEtag);
				indexingGroups.Add(result);
			}
			indexingGroups = indexingGroups.OrderByDescending(x => x.LastQueryTime).ToList();
			return false;
		}

		private PrefetchingBehavior GetPrefetcherFor(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
		{
			foreach (var prefetchingBehavior in prefetchingBehaviors)
			{
				if (prefetchingBehavior.CanUsePrefetcherToLoadFrom(fromEtag) && usedPrefetchers.TryAdd(prefetchingBehavior))
					return prefetchingBehavior;
			}

			var newPrefetcher = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner);

			var recentEtag = Etag.Empty;
			context.Database.TransactionalStorage.Batch(accessor => { recentEtag = accessor.Staleness.GetMostRecentDocumentEtag(); });

			if (recentEtag.Restarts != fromEtag.Restarts || Math.Abs(recentEtag.Changes - fromEtag.Changes) > context.CurrentNumberOfItemsToIndexInSingleBatch)
			{
				// If the distance between etag of a recent document in db and etag to index from is greater than NumberOfItemsToProcessInSingleBatch
				// then prevent the prefetcher from loading newly added documents. For such prefetcher we will relay only on future batches to prefetch docs to avoid
				// large memory consumption by in-memory prefetching queue that would hold all the new documents, but it would be a long time before we can reach them.
				newPrefetcher.DisableCollectingDocumentsAfterCommit = true;
			}

			prefetchingBehaviors.Add(newPrefetcher);
			usedPrefetchers.Add(newPrefetcher);

			return newPrefetcher;
		}

		private void RemoveUnusedPrefetchers(IEnumerable<PrefetchingBehavior> usedPrefetchingBehaviors)
		{
			var unused = prefetchingBehaviors.Except(usedPrefetchingBehaviors.Union(new[]
			{
				defaultPrefetchingBehavior
			})).ToList();


			if (unused.Count == 0)
				return;

			foreach (var unusedPrefetcher in unused)
			{
				prefetchingBehaviors.TryRemove(unusedPrefetcher);
				prefetcher.RemovePrefetchingBehavior(unusedPrefetcher);
			}
		}

		protected override void CleanupPrefetchers()
		{
			RemoveUnusedPrefetchers(Enumerable.Empty<PrefetchingBehavior>());
		}


		private static IDisposable MapIndexingInProgress(IList<Index> indexesToWorkOn)
		{
			indexesToWorkOn.ForEach(x => x.IsMapIndexingInProgress = true);


			return new DisposableAction(() => indexesToWorkOn.ForEach(x => x.IsMapIndexingInProgress = false));
		}


		public void IndexPrecomputedBatch(PrecomputedIndexingBatch precomputedBatch, CancellationToken token)
		{
			token.ThrowIfCancellationRequested();


			context.MetricsCounters.IndexedPerSecond.Mark(precomputedBatch.Documents.Count);


			var indexToWorkOn = new IndexToWorkOn
			{
				Index = precomputedBatch.Index,
				IndexId = precomputedBatch.Index.indexId,
				LastIndexedEtag = Etag.Empty
			};


			using (LogContext.WithDatabase(context.DatabaseName))
			using (MapIndexingInProgress(new List<Index> {indexToWorkOn.Index}))
			{
				List<IndexToWorkOn> filteredOutIndexes;
				var indexingBatchForIndex =
					FilterIndexes(new List<IndexToWorkOn> {indexToWorkOn}, precomputedBatch.Documents,
						precomputedBatch.LastIndexed, out filteredOutIndexes).FirstOrDefault();

				if (indexingBatchForIndex == null)
					return;

				IndexingBatchInfo batchInfo = null;

				IndexingPerformanceStats performance = null;
				try
				{
					batchInfo = context.ReportIndexingBatchStarted(precomputedBatch.Documents.Count, -1, new List<string>
					{
						indexToWorkOn.Index.PublicName
					});


					batchInfo.BatchType = BatchType.Precomputed;


					if (Log.IsDebugEnabled)
					{
						Log.Debug("Going to index precomputed documents for a new index {0}. Count of precomputed docs {1}",
							precomputedBatch.Index.PublicName, precomputedBatch.Documents.Count);
					}

				        context.ReportIndexingBatchCompleted(batchInfo);



			indexReplacer.ReplaceIndexes(new []{ indexToWorkOn.IndexId });
        

					performance = HandleIndexingFor(indexingBatchForIndex, precomputedBatch.LastIndexed, precomputedBatch.LastModified, token);
				}
				finally
				{
					if (batchInfo != null)
					{
						if (performance != null)
							batchInfo.PerformanceStats.TryAdd(indexingBatchForIndex.Index.PublicName, performance);


						context.ReportIndexingBatchCompleted(batchInfo);
					}
				}
			}
		}

		private IndexingPerformanceStats HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified, CancellationToken token)
		{
			if (currentlyProcessedIndexes.TryAdd(batchForIndex.IndexId, batchForIndex.Index) == false)
			{
				Log.Error("Entered handle indexing with index {0} inside currentlyProcessedIndexes", batchForIndex.Index.PublicName);
				batchForIndex.SignalIndexingComplete();
				return null;
			}
			IndexingPerformanceStats performanceResult = null;

			try
			{
				transactionalStorage.Batch(actions => { performanceResult = IndexDocuments(actions, batchForIndex, token); });


				// This can be null if IndexDocument fails to execute and the exception is catched.
				if (performanceResult != null)
					performanceResult.RunCompleted();
			}
			catch (Exception e)
			{
				Log.WarnException("Failed to index " + batchForIndex.Index.PublicName, e);
			}
			finally
			{
				if (performanceResult != null)
				{
					performanceResult.OnCompleted = null;
				}

				Index _;
				if (Log.IsDebugEnabled)
				{
					Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
						batchForIndex.Batch.Docs.Count,
						lastEtag,
						batchForIndex.Index.PublicName);
				}

				bool keepTrying = true;
				try
				{
					for (int i = 0; i < 10 && keepTrying; i++)
					{
						keepTrying = false;
						transactionalStorage.Batch(actions =>
						{
							try
							{
								// whatever we succeeded in indexing or not, we have to update this
								// because otherwise we keep trying to re-index failed documents
								actions.Indexing.UpdateLastIndexed(batchForIndex.IndexId, lastEtag, lastModified);
							}
							catch (Exception e)
							{
								if (actions.IsWriteConflict(e))
								{
									keepTrying = true;
									return;
								}
								throw;
							}
						});

						if (keepTrying)
							Thread.Sleep(11);
					}
				}
				finally
				{
					currentlyProcessedIndexes.TryRemove(batchForIndex.IndexId, out _);
					batchForIndex.SignalIndexingComplete();
					batchForIndex.Index.IsMapIndexingInProgress = false;
				}
			}

			return performanceResult;
		}


		public class IndexingBatchForIndex
		{
			private static int _counter = 0;
			public int BatchId = 0;

			public IndexingBatchForIndex()
			{
				BatchId = Interlocked.Increment(ref _counter);
			}

			public int IndexId { get; set; }


			public Index Index { get; set; }

			public Etag LastIndexedEtag { get; set; }

			public IndexingBatch Batch { get; set; }
			public event Action OnIndexingComplete;

			public void SignalIndexingComplete()
			{
				if (OnIndexingComplete != null)
				{
					OnIndexingComplete();
				}
			}
		}

		private IEnumerable<IndexingBatchForIndex> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, Etag highestETagInBatch, out List<IndexToWorkOn> filteredOutIndexes)
		{
			var innerFilteredOutIndexes = new ConcurrentStack<IndexToWorkOn>();
			var last = jsonDocs.Last();

			Debug.Assert(last.Etag != null);
			Debug.Assert(last.LastModified != null);

			var lastEtag = last.Etag;
			var lastModified = last.LastModified.Value;


			var documentRetriever = new DocumentRetriever(null, null, context.ReadTriggers, context.Database.InFlightTransactionalState);


			//var filteredDocs = new ConcurrentQueue<Tuple<JsonDocument, object>>();
			//context
			

			/*Parallel.ForEach(jsonDocs.Where(x => x != null).AsParallel()
				new ParallelOptions()
				{
					MaxDegreeOfParallelism = context.CurrentNumberOfParallelTasks
				}, doc =>
				{
					if (doc != null)
					{
						var filteredDoc = documentRetriever.ExecuteReadTriggers(doc, null, ReadOperation.Index);
						filteredDocs.Enqueue(
							filteredDoc == null ?
								Tuple.Create(doc, (object) new FilteredDocument(doc)) :
								Tuple.Create(filteredDoc, JsonToExpando.Convert(doc.ToJson())));
					}
				});*/

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


			var results = new ConcurrentQueue<IndexingBatchForIndex>();
			var actions = new ConcurrentQueue<Action<IStorageActionsAccessor>>();
			context.Database.MappingThreadPool.ExecuteBatch(indexesToWorkOn, indexToWorkOn =>
			{
				var indexName = indexToWorkOn.Index.PublicName;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch(highestETagInBatch);


				foreach (var filteredDoc in filteredDocs)
				{
					var doc = filteredDoc.Doc;
					var json = filteredDoc.Json;


					if (defaultPrefetchingBehavior.FilterDocuments(doc) == false
					    || doc.Etag.CompareTo(indexToWorkOn.LastIndexedEtag) <= 0)
						continue;

					// did we already indexed this document in this index?

					var etag = doc.Etag;
					if (etag == null)
						continue;

					// is the Raven-Entity-Name a match for the things the index executes on?
					if (viewGenerator.ForEntityNames.Count != 0 &&
					    viewGenerator.ForEntityNames.Contains(doc.Metadata.Value<string>(Constants.RavenEntityName)) == false)
					{
						continue;
					}

					batch.Add(doc, json, defaultPrefetchingBehavior.ShouldSkipDeleteFromIndex(doc));

					if (batch.DateTime == null)

						batch.DateTime = doc.LastModified;
					else
						batch.DateTime = batch.DateTime > doc.LastModified
							? doc.LastModified
							: batch.DateTime;
				}

				if (batch.Docs.Count == 0)
				{
					Log.Debug("All documents have been filtered for {0}, no indexing will be performed, updating to {1}, {2}", indexName,
						lastEtag, lastModified);
					// we use it this way to batch all the updates together
					if (indexToWorkOn.LastIndexedEtag.CompareTo(lastEtag) < 0)
						actions.Enqueue(accessor => { 
accessor.Indexing.UpdateLastIndexed(indexToWorkOn.Index.indexId, lastEtag, lastModified); 
accessor.AfterStorageCommit += () =>
						{
							indexToWorkOn.Index.EnsureIndexWriter();
							indexToWorkOn.Index.Flush(lastEtag);
						};
});
					innerFilteredOutIndexes.Push(indexToWorkOn);
					return;
				}
				if (Log.IsDebugEnabled)
				{
					Log.Debug("Going to index {0} documents in {1}: ({2})", batch.Ids.Count, indexToWorkOn, string.Join(", ", batch.Ids));
				}
				results.Enqueue(new IndexingBatchForIndex
				{
					Batch = batch,
					IndexId = indexToWorkOn.IndexId,
					Index = indexToWorkOn.Index,
					LastIndexedEtag = indexToWorkOn.LastIndexedEtag
				});
			});

			filteredOutIndexes = innerFilteredOutIndexes.ToList();
			foreach (var action in actions)
			{
				bool keepTrying = true;
				for (int i = 0; i < 10 && keepTrying; i++)
				{
					keepTrying = false;
					transactionalStorage.Batch(actionsAccessor =>
					{
						try
						{
							if (action != null)
								action(actionsAccessor);
						}
						catch (Exception e)
						{
							if (actionsAccessor.IsWriteConflict(e))
							{
								keepTrying = true;
								return;
							}
							throw;
						}
					});

					if (keepTrying)
						Thread.Sleep(11);
				}
			}
			return results.Where(x => x != null);
		}


		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			return true;
		}

		private IndexingPerformanceStats IndexDocuments(IStorageActionsAccessor actions, IndexingBatchForIndex indexingBatchForIndex, CancellationToken token)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexingBatchForIndex.IndexId);
			if (viewGenerator == null)
				return null; // index was deleted, probably

			var batch = indexingBatchForIndex.Batch;

			IndexingPerformanceStats performanceStats = null;
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
					Log.Debug("Indexing {0} documents for index: {1}. ({2})", batch.Docs.Count, indexingBatchForIndex.Index.PublicName, ids);
				}
				context.CancellationToken.ThrowIfCancellationRequested();
				token.ThrowIfCancellationRequested();

				performanceStats = context.IndexStorage.Index(indexingBatchForIndex.IndexId, viewGenerator, batch, context, actions, batch.DateTime ?? DateTime.MinValue, token);
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception e)
			{
				if (actions.IsWriteConflict(e))
					return null;

				Log.WarnException(string.Format("Failed to index documents for index: {0}", indexingBatchForIndex.Index.PublicName), e);
				context.AddError(indexingBatchForIndex.IndexId, indexingBatchForIndex.Index.PublicName, null, e);
			}

			return performanceStats;
		}

		protected override void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not dispose of IndexingExecuter");

			foreach (var prefetchingBehavior in PrefetchingBehaviors)
			{
				exceptionAggregator.Execute(prefetchingBehavior.Dispose);
			}

			exceptionAggregator.ThrowIfNeeded();
		}
	}
}