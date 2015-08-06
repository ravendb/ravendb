//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Sparrow.Collections;

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
			defaultPrefetchingBehavior = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner, "Default Prefetching behavior");
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
			return index.IsTestIndex ||
			       index.IsMapIndexingInProgress; // precomputed? slow? it is already running, nothing to do with it for now;
		}

		protected override DatabaseTask GetApplicableTask(IStorageActionsAccessor actions)
	    {
		    var removeFromIndexTasks = (DatabaseTask)actions.Tasks.GetMergedTask<RemoveFromIndexTask>();
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

		private class IndexingGroup
		{
			public Etag LastIndexedEtag;
			public DateTime? LastQueryTime;
			public List<IndexToWorkOn> Indexes; 
			public PrefetchingBehavior PrefetchingBehavior;
		}

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexes)
        {
	        var indexingGroups = context.Configuration.IndexingClassifier.GroupMapIndexes(indexes);

	        indexingGroups = indexingGroups.OrderByDescending(x => x.Key).ToDictionary(x => x.Key, x => x.Value);

			if (indexingGroups.Count == 0)
				return;

			var usedPrefetchers = new ConcurrentSet<PrefetchingBehavior>();

			var groupedIndexes = indexingGroups.Select(x =>
			{
				var result = new IndexingGroup
				{
					LastIndexedEtag = x.Key,
					Indexes = x.Value,
					LastQueryTime = x.Value.Max(y => y.Index.LastQueryTime),
					PrefetchingBehavior = GetPrefetcherFor(x.Key, usedPrefetchers)
				};

				result.PrefetchingBehavior.AdditionalInfo = string.Format("Default prefetcher: {0}. For indexing group: [Indexes: {1}, LastIndexedEtag: {2}]",
					result.PrefetchingBehavior == defaultPrefetchingBehavior, string.Join(", ", result.Indexes.Select(y => y.Index.PublicName)), result.LastIndexedEtag);

				return result;
			}).OrderByDescending(x => x.LastQueryTime).ToList();

	        var maxIndexOutputsPerDoc = groupedIndexes.Max(x => x.Indexes.Max(y => y.Index.MaxIndexOutputsPerDocument));
	        var containsMapReduceIndexes = groupedIndexes.Any(x => x.Indexes.Any(y => y.Index.IsMapReduce));

			var recoverTunerState = ((IndexBatchSizeAutoTuner)autoTuner).ConsiderLimitingNumberOfItemsToProcessForThisBatch(maxIndexOutputsPerDoc, containsMapReduceIndexes);

			BackgroundTaskExecuter.Instance.ExecuteAll(context, groupedIndexes, (indexingGroup, i) =>
			{
				context.CancellationToken.ThrowIfCancellationRequested();
				using (LogContext.WithDatabase(context.DatabaseName))
				{
					var prefetchingBehavior = indexingGroup.PrefetchingBehavior;
					var indexesToWorkOn = indexingGroup.Indexes;

					var operationCanceled = false;
					TimeSpan indexingDuration = TimeSpan.Zero;
					var lastEtag = Etag.Empty;

					List<JsonDocument> jsonDocs;
					IndexingBatchInfo batchInfo = null;

					using (MapIndexingInProgress(indexesToWorkOn))
					using (prefetchingBehavior.DocumentBatchFrom(indexingGroup.LastIndexedEtag, out jsonDocs))
					{
						try
						{
							if (Log.IsDebugEnabled)
							{
								Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
									jsonDocs.Count, indexingGroup.LastIndexedEtag, string.Join(", ", jsonDocs.Select(x => x.Key)));
							}

							batchInfo = context.ReportIndexingBatchStarted(jsonDocs.Count, jsonDocs.Sum(x => x.SerializedSizeOnDisk), indexesToWorkOn.Select(x => x.Index.PublicName).ToList());

							context.CancellationToken.ThrowIfCancellationRequested();

							if (jsonDocs.Count <= 0)
							{
								return;
							}

							var sw = Stopwatch.StartNew();

							lastEtag = DoActualIndexing(indexesToWorkOn, jsonDocs, batchInfo);

							indexingDuration = sw.Elapsed;
						}
						catch (InvalidDataException e)
						{
							Log.ErrorException("Failed to index because of data corruption. ", e);
							indexesToWorkOn.ForEach(index =>
								context.AddError(index.IndexId, index.Index.PublicName, null, string.Format("Failed to index because of data corruption. Reason: {0}", e.Message)));
						}
						catch (OperationCanceledException)
						{
							operationCanceled = true;
						}
						catch (AggregateException e)
						{
							var anyOperationsCanceled = e
								.InnerExceptions
								.OfType<OperationCanceledException>()
								.Any();

							if (anyOperationsCanceled == false)
								throw;

							operationCanceled = true;
						}
						finally
						{
							if (operationCanceled == false && jsonDocs != null && jsonDocs.Count > 0)
							{
								prefetchingBehavior.CleanupDocuments(lastEtag);
								prefetchingBehavior.UpdateAutoThrottler(jsonDocs, indexingDuration);
							}

							prefetchingBehavior.BatchProcessingComplete();
							if (batchInfo != null)
								context.ReportIndexingBatchCompleted(batchInfo);
						}
					}
				}
			});

			if (recoverTunerState != null)
				recoverTunerState();

			RemoveUnusedPrefetchers(usedPrefetchers);
		}

		private PrefetchingBehavior GetPrefetcherFor(Etag fromEtag, ConcurrentSet<PrefetchingBehavior> usedPrefetchers)
		{
			foreach (var prefetchingBehavior in prefetchingBehaviors)
			{
				if (prefetchingBehavior.CanUsePrefetcherToLoadFrom(fromEtag) && usedPrefetchers.TryAdd(prefetchingBehavior))
					return prefetchingBehavior;
			}

			var newPrefetcher = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner,string.Format("Etags from: {0}", fromEtag));

			var recentEtag = Etag.Empty;
			context.Database.TransactionalStorage.Batch(accessor =>
			{
				recentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
			});

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

			if(unused.Count == 0)
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

		private static IDisposable MapIndexingInProgress(IList<IndexToWorkOn> indexesToWorkOn)
		{
			indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = true);

			return new DisposableAction(() => indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = false));
		}

		private Etag DoActualIndexing(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, IndexingBatchInfo indexingBatchInfo)
		{
			var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(jsonDocs);
			var lastModified = lastByEtag.LastModified.Value;
			var lastEtag = lastByEtag.Etag;

            context.MetricsCounters.IndexedPerSecond.Mark(jsonDocs.Count);
            
			var result = FilterIndexes(indexesToWorkOn, jsonDocs, lastEtag).OrderByDescending(x => x.Index.LastQueryTime).ToList();

			BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, result,
				index =>
				{
					using (LogContext.WithDatabase(context.DatabaseName))
					{
						var performance = HandleIndexingFor(index, lastEtag, lastModified, context.CancellationToken);

						if (performance != null)
							indexingBatchInfo.PerformanceStats.TryAdd(index.Index.PublicName, performance);
					}
				});

			return lastEtag;
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
			using (MapIndexingInProgress(new List<IndexToWorkOn> { indexToWorkOn }))
	        {
				var indexingBatchForIndex =
					FilterIndexes(new List<IndexToWorkOn> { indexToWorkOn }, precomputedBatch.Documents,
									precomputedBatch.LastIndexed).FirstOrDefault();

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

			indexReplacer.ReplaceIndexes(new []{ indexToWorkOn.IndexId });
        }

		private IndexingPerformanceStats HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified, CancellationToken token)
		{
		    currentlyProcessedIndexes.TryAdd(batchForIndex.IndexId, batchForIndex.Index);

			IndexingPerformanceStats performanceResult = null;
			var wasOutOfMemory = false;
			var wasOperationCanceled = false;
			try
			{
				transactionalStorage.Batch(actions =>
				{
					performanceResult = IndexDocuments(actions, batchForIndex, token);
				});

				// This can be null if IndexDocument fails to execute and the exception is catched.
				if (performanceResult != null)
					performanceResult.RunCompleted();
			}
			catch (OperationCanceledException)
			{
				wasOperationCanceled = true;
				throw;
			}
			catch (Exception e)
			{
				var exception = e;
				var aggregateException = exception as AggregateException;
				if (aggregateException != null)
					exception = aggregateException.ExtractSingleInnerException();

				if (TransactionalStorageHelper.IsWriteConflict(exception))
					return null;

				Log.WarnException(string.Format("Failed to index documents for index: {0}", batchForIndex.Index.PublicName), exception);

				wasOutOfMemory = TransactionalStorageHelper.IsOutOfMemoryException(exception);

				if (wasOutOfMemory == false)
					context.AddError(batchForIndex.IndexId, batchForIndex.Index.PublicName, null, exception);
			}
			finally
			{
				if (performanceResult != null)
                {                    
                    performanceResult.OnCompleted = null;
                }				

				if (Log.IsDebugEnabled)
				{
					Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
							  batchForIndex.Batch.Docs.Count,
							  lastEtag,
							  batchForIndex.Index.PublicName);
				}

				if (wasOutOfMemory == false && wasOperationCanceled == false)
				{
					transactionalStorage.Batch(actions =>
					{
						// whatever we succeeded in indexing or not, we have to update this
						// because otherwise we keep trying to re-index failed documents
						actions.Indexing.UpdateLastIndexed(batchForIndex.IndexId, lastEtag, lastModified);
					});
				}
				else if (wasOutOfMemory)
					HandleOutOfMemory(batchForIndex);

				Index _;
				currentlyProcessedIndexes.TryRemove(batchForIndex.IndexId, out _);
			}

			return performanceResult;
		}

		private void HandleOutOfMemory(IndexingBatchForIndex batchForIndex)
		{
			transactionalStorage.Batch(actions =>
			{
				var instance = context.IndexStorage.GetIndexInstance(batchForIndex.IndexId);
				if (instance == null)
				{
					return;
				}

				Log.Error("Disabled index '{0}'. Reason: out of memory.", instance.PublicName);

				string configurationKey = null;
				if (string.Equals(context.Database.TransactionalStorage.FriendlyName, InMemoryRavenConfiguration.VoronTypeName, StringComparison.OrdinalIgnoreCase))
				{
					configurationKey = Constants.Voron.MaxScratchBufferSize;
				}
				else if (string.Equals(context.Database.TransactionalStorage.FriendlyName, InMemoryRavenConfiguration.EsentTypeName, StringComparison.OrdinalIgnoreCase))
				{
					configurationKey = Constants.Esent.MaxVerPages;
				}

				Debug.Assert(configurationKey != null);

				actions.Indexing.SetIndexPriority(batchForIndex.IndexId, IndexingPriority.Disabled);
				context.Database.AddAlert(new Alert { AlertLevel = AlertLevel.Error, CreatedAt = SystemTime.UtcNow, Title = string.Format("Index '{0}' was disabled", instance.PublicName), UniqueKey = string.Format("Index '{0}' was disabled", instance.IndexId), Message = string.Format("Out of memory exception occured in storage during indexing process for index '{0}'. As a result of this action, index changed state to disabled. Try increasing '{1}' value in configuration.", instance.PublicName, configurationKey) });
				instance.Priority = IndexingPriority.Disabled;
			});
		}

		public class IndexingBatchForIndex
		{
			public int IndexId { get; set; }

            public Index Index { get; set; }

			public Etag LastIndexedEtag { get; set; }

			public IndexingBatch Batch { get; set; }
		}

		private IEnumerable<IndexingBatchForIndex> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs, Etag highestETagInBatch)
		{
			var last = jsonDocs.Last();

			Debug.Assert(last.Etag != null);
			Debug.Assert(last.LastModified != null);

			var lastEtag = last.Etag;
			var lastModified = last.LastModified.Value;

			var documentRetriever = new DocumentRetriever(null, null, context.ReadTriggers, context.Database.InFlightTransactionalState);

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

			var results = new IndexingBatchForIndex[indexesToWorkOn.Count];
			var actions = new Action<IStorageActionsAccessor>[indexesToWorkOn.Count];

			BackgroundTaskExecuter.Instance.ExecuteAll(context, indexesToWorkOn, (indexToWorkOn, i) =>
			{
				var indexName = indexToWorkOn.Index.PublicName;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch(highestETagInBatch);

				foreach (var item in filteredDocs)
				{
					if (defaultPrefetchingBehavior.FilterDocuments(item.Doc) == false)
						continue;

					// did we already indexed this document in this index?
					var etag = item.Doc.Etag;
					if (etag == null)
						continue;

					// is the Raven-Entity-Name a match for the things the index executes on?
					if (viewGenerator.ForEntityNames.Count != 0 &&
						viewGenerator.ForEntityNames.Contains(item.Doc.Metadata.Value<string>(Constants.RavenEntityName)) == false)
					{
						continue;
					}

					batch.Add(item.Doc, item.Json, defaultPrefetchingBehavior.ShouldSkipDeleteFromIndex(item.Doc));

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
					actions[i] = accessor =>
					{
						accessor.Indexing.UpdateLastIndexed(indexToWorkOn.Index.indexId, lastEtag, lastModified);

						accessor.AfterStorageCommit += () =>
						{
							indexToWorkOn.Index.EnsureIndexWriter();
							indexToWorkOn.Index.Flush(lastEtag);
						};
					};
					return;
				}
				if (Log.IsDebugEnabled)
				{
					Log.Debug("Going to index {0} documents in {1}: ({2})", batch.Ids.Count, indexToWorkOn, string.Join(", ", batch.Ids));
				}
				results[i] = new IndexingBatchForIndex
				{
					Batch = batch,
					IndexId = indexToWorkOn.IndexId,
                    Index = indexToWorkOn.Index,
					LastIndexedEtag = indexToWorkOn.LastIndexedEtag
				};

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

		private IndexingPerformanceStats IndexDocuments(IStorageActionsAccessor actions, IndexingBatchForIndex indexingBatchForIndex, CancellationToken token)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexingBatchForIndex.IndexId);
			if (viewGenerator == null)
				return null; // index was deleted, probably

			var batch = indexingBatchForIndex.Batch;

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

			token.ThrowIfCancellationRequested();

			return context.IndexStorage.Index(indexingBatchForIndex.IndexId, viewGenerator, batch, context, actions, batch.DateTime ?? DateTime.MinValue, token); ;
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
