//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class IndexingExecuter : AbstractIndexingExecuter
	{
		readonly PrefetchingBehavior prefetchingBehavior;

		public IndexingExecuter(WorkContext context, Prefetcher prefetcher)
			: base(context)
		{
			autoTuner = new IndexBatchSizeAutoTuner(context);
			prefetchingBehavior = prefetcher.GetPrefetchingBehavior(PrefetchingUser.Indexer, autoTuner);
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
		{
		    var isStale = actions.Staleness.IsMapStale(indexesStat.Id);
			var indexingPriority = indexesStat.Priority;
			if (isStale == false)
				return false;

			if (indexingPriority == IndexingPriority.None)
				return true;

			if (indexingPriority.HasFlag(IndexingPriority.Normal))
			{
				onlyFoundIdleWork.Value = false;
				return true;
			}

			if (indexingPriority.HasFlag(IndexingPriority.Disabled) || 
                indexingPriority.HasFlag(IndexingPriority.Error))
				return false;

			if (isIdle == false)
				return false; // everything else is only valid on idle runs

			if (indexingPriority.HasFlag(IndexingPriority.Idle))
				return true;

			if (indexingPriority.HasFlag(IndexingPriority.Abandoned))
			{
				var timeSinceLastIndexing = (SystemTime.UtcNow - indexesStat.LastIndexingTime);

				return (timeSinceLastIndexing > context.Configuration.TimeToWaitBeforeRunningAbandonedIndexes);
			}

			throw new InvalidOperationException("Unknown indexing priority for index " + indexesStat.Id + ": " + indexesStat.Priority);
		}

		protected override DatabaseTask GetApplicableTask(IStorageActionsAccessor actions)
		{
            return (DatabaseTask)actions.Tasks.GetMergedTask<RemoveFromIndexTask>() ??
		           actions.Tasks.GetMergedTask<TouchMissingReferenceDocumentTask>();
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

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			indexesToWorkOn = context.Configuration.IndexingScheduler.FilterMapIndexes(indexesToWorkOn);

			if (indexesToWorkOn.Count == 0)
				return;

            var lastIndexedEtagForAllIndexes =
                   indexesToWorkOn.Min(x => new ComparableByteArray(x.LastIndexedEtag.ToByteArray())).ToEtag();

			context.CancellationToken.ThrowIfCancellationRequested();

			var operationCancelled = false;
			TimeSpan indexingDuration = TimeSpan.Zero;
			List<JsonDocument> jsonDocs = null;
			var lastEtag = Etag.Empty;

			indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = true);

			try
			{
                jsonDocs = prefetchingBehavior.GetDocumentsBatchFrom(lastIndexedEtagForAllIndexes);
					
				if (Log.IsDebugEnabled)
				{
					Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
                              jsonDocs.Count, lastIndexedEtagForAllIndexes, string.Join(", ", jsonDocs.Select(x => x.Key)));
				}

				context.ReportIndexingActualBatchSize(jsonDocs.Count);

				context.CancellationToken.ThrowIfCancellationRequested();

				if (jsonDocs.Count <= 0)
					return;

				var sw = Stopwatch.StartNew();

				lastEtag = DoActualIndexing(indexesToWorkOn, jsonDocs);

				indexingDuration = sw.Elapsed;
			}
			catch (OperationCanceledException)
			{
				operationCancelled = true;
			}
			finally
			{
				if (operationCancelled == false && jsonDocs != null && jsonDocs.Count > 0)
				{
					prefetchingBehavior.CleanupDocuments(lastEtag);
					prefetchingBehavior.UpdateAutoThrottler(jsonDocs, indexingDuration);
				}

				prefetchingBehavior.BatchProcessingComplete();

				indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = false);
			}
		}

		private Etag DoActualIndexing(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs)
		{
			var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(jsonDocs);
			var lastModified = lastByEtag.LastModified.Value;
			var lastEtag = lastByEtag.Etag;

            context.MetricsCounters.IndexedPerSecond.Mark(jsonDocs.Count);
            
			var result = FilterIndexes(indexesToWorkOn, jsonDocs, lastEtag).ToList();

			BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, result,
																  index => HandleIndexingFor(index, lastEtag, lastModified));

			return lastEtag;
		}

        public void IndexPrecomputedBatch(PrecomputedIndexingBatch precomputedBatch)
        {
            context.MetricsCounters.IndexedPerSecond.Mark(precomputedBatch.Documents.Count);

            var indexToWorkOn = new IndexToWorkOn()
            {
                Index = precomputedBatch.Index,
                IndexId = precomputedBatch.Index.indexId,
                LastIndexedEtag = Etag.Empty
            };

            var indexingBatchForIndex =
                FilterIndexes(new List<IndexToWorkOn>() {indexToWorkOn}, precomputedBatch.Documents,
                              precomputedBatch.LastIndexed).FirstOrDefault();

            if (indexingBatchForIndex == null)
                return;

            if (Log.IsDebugEnabled)
            {
                Log.Debug("Going to index precomputed documents for a new index {0}. Count of precomputed docs {1}",
                          precomputedBatch.Index.PublicName, precomputedBatch.Documents.Count);
            }

            HandleIndexingFor(indexingBatchForIndex, precomputedBatch.LastIndexed, precomputedBatch.LastModified);
        }

		private void HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified)
		{
		    currentlyProcessedIndexes.TryAdd(batchForIndex.IndexId, batchForIndex.Index);

			try
			{
				transactionalStorage.Batch(actions => IndexDocuments(actions, batchForIndex));
			}
			catch (Exception e)
			{
				Log.Warn("Failed to index " + batchForIndex.IndexId, e);
			}
			finally
			{
				if (Log.IsDebugEnabled)
				{
					Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
							  batchForIndex.Batch.Docs.Count,
							  lastEtag,
							  batchForIndex.IndexId);
				}

				transactionalStorage.Batch(actions =>
					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed documents
										   actions.Indexing.UpdateLastIndexed(batchForIndex.IndexId, lastEtag, lastModified));

				Index _;
				currentlyProcessedIndexes.TryRemove(batchForIndex.IndexId, out _);
			}
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

			var documentRetriever = new DocumentRetriever(null, context.ReadTriggers, context.Database.InFlightTransactionalState);

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
				var indexName = indexToWorkOn.IndexId;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch(highestETagInBatch);

				foreach (var item in filteredDocs)
				{
					if (prefetchingBehavior.FilterDocuments(item.Doc) == false)
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

					batch.Add(item.Doc, item.Json, prefetchingBehavior.ShouldSkipDeleteFromIndex(item.Doc));

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
					actions[i] = accessor => accessor.Indexing.UpdateLastIndexed(indexToWorkOn.Index.indexId, lastEtag, lastModified);
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

		private void IndexDocuments(IStorageActionsAccessor actions, IndexingBatchForIndex indexingBatchForIndex)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexingBatchForIndex.IndexId);
			if (viewGenerator == null)
				return; // index was deleted, probably

			var batch = indexingBatchForIndex.Batch;
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

				
				context.IndexStorage.Index(indexingBatchForIndex.IndexId, viewGenerator, batch, context, actions, batch.DateTime ?? DateTime.MinValue);
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (Exception e)
			{
				if (actions.IsWriteConflict(e))
					return;
				Log.WarnException(string.Format("Failed to index documents for index: {0}", indexingBatchForIndex.Index.PublicName), e);
				context.AddError(indexingBatchForIndex.IndexId, indexingBatchForIndex.Index.PublicName, null, e.Message);
			}
		}

		protected override void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not dispose of IndexingExecuter");

			exceptionAggregator.Execute(prefetchingBehavior.Dispose);

			exceptionAggregator.ThrowIfNeeded();
		}
		}
    }
