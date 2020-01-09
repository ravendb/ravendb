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
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Task = Raven.Database.Tasks.Task;

namespace Raven.Database.Indexing
{
	public class IndexingExecuter : AbstractIndexingExecuter
	{
		private readonly PrefetchingBehavior prefetchingBehavior;

		public IndexingExecuter(WorkContext context, Prefetcher prefetcher)
			: base(context)
		{
			autoTuner = new IndexBatchSizeAutoTuner(context);
			prefetchingBehavior = prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, autoTuner);
		}

		public PrefetchingBehavior PrefetchingBehavior
		{
			get { return prefetchingBehavior; }
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
		{
			var isStale = actions.Staleness.IsMapStale(indexesStat.Name);
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

			if (indexingPriority.HasFlag(IndexingPriority.Disabled))
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

			throw new InvalidOperationException("Unknown indexing priority for index " + indexesStat.Name + ": " + indexesStat.Priority);
		}

		protected override Task GetApplicableTask(IStorageActionsAccessor actions)
		{
		    return (Task)actions.Tasks.GetMergedTask<RemoveFromIndexTask>() ??
		           actions.Tasks.GetMergedTask<TouchReferenceDocumentIfChangedTask>();
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
				LastIndexedEtag = indexesStat.LastIndexedEtag,
				LastIndexedTimestamp = indexesStat.LastIndexedTimestamp
			};
		}

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			indexesToWorkOn = context.Configuration.IndexingScheduler.FilterMapIndexes(indexesToWorkOn);

            var lastIndexedEtagForAllIndexes =
                   indexesToWorkOn.Min(x => new ComparableByteArray(x.LastIndexedEtag.ToByteArray())).ToEtag();

			context.CancellationToken.ThrowIfCancellationRequested();

			var operationCancelled = false;
			TimeSpan indexingDuration = TimeSpan.Zero;
			var lastEtag = Etag.Empty;

			indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = true);
			
			List<JsonDocument> jsonDocs;
	        using (PrefetchingBehavior.DocumentBatchFrom(lastIndexedEtagForAllIndexes, out jsonDocs))
	        {
		        try
		        {
			        if (Log.IsDebugEnabled)
			        {
				        Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
					        jsonDocs.Count, lastIndexedEtagForAllIndexes, string.Join(", ", jsonDocs.Select(x => x.Key)));
			        }

			        context.ReportIndexingBatchStarted(jsonDocs.Count, jsonDocs.Sum(x => x.SerializedSizeOnDisk));

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
				        PrefetchingBehavior.CleanupDocuments(lastEtag);
				        PrefetchingBehavior.UpdateAutoThrottler(jsonDocs, indexingDuration);
			        }

			        PrefetchingBehavior.BatchProcessingComplete();
			        indexesToWorkOn.ForEach(x => x.Index.IsMapIndexingInProgress = false);
		        }
	        }
		}

		private Etag DoActualIndexing(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs)
		{
			var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(jsonDocs);
			var lastModified = lastByEtag.LastModified.Value;
			var lastEtag = lastByEtag.Etag;

			context.PerformanceCounters.IndexedPerSecond.IncrementBy(jsonDocs.Count);
			var result = FilterIndexes(indexesToWorkOn, jsonDocs, lastEtag).ToList();

			BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, result,
																  index => HandleIndexingFor(index, lastEtag, lastModified));

			return lastEtag;
		}

		private void HandleIndexingFor(IndexingBatchForIndex batchForIndex, Etag lastEtag, DateTime lastModified)
		{
            var wasOperationCanceled = false;

            try
            {
                transactionalStorage.Batch(actions => IndexDocuments(actions, batchForIndex.IndexName, batchForIndex.Batch));
            }
            catch (IndexDoesNotExistsException)
            {
                wasOperationCanceled = true;
            }
            catch (ObjectDisposedException e)
            {
                wasOperationCanceled = true;
                Log.WarnException("Failed to index because index was disposed", e);
			}
			catch (Exception e)
			{
				Log.Warn("Failed to index " + batchForIndex.IndexName, e);
			}
			finally
			{
                if (wasOperationCanceled == false)
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug("After indexing {0} documents, the new last etag for is: {1} for {2}",
                            batchForIndex.Batch.Docs.Count,
                            lastEtag,
                            batchForIndex.IndexName);
                    }

                    transactionalStorage.Batch(actions =>
                        // whatever we succeeded in indexing or not, we have to update this
                        // because otherwise we keep trying to re-index failed documents
                        actions.Indexing.UpdateLastIndexed(batchForIndex.IndexName, lastEtag, lastModified));
                }
			}
		}


		public class IndexingBatchForIndex
		{
			public string IndexName { get; set; }

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
				var indexName = indexToWorkOn.IndexName;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch(highestETagInBatch);

				foreach (var item in filteredDocs)
				{
					if (PrefetchingBehavior.FilterDocuments(item.Doc) == false)
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

					batch.Add(item.Doc, item.Json, PrefetchingBehavior.ShouldSkipDeleteFromIndex(item.Doc));

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
				results[i] = new IndexingBatchForIndex
				{
					Batch = batch,
					IndexName = indexToWorkOn.IndexName,
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
                {
                    Log.Debug(string.Format("Write conflict encountered for index {0}. Will retry on the next indexing batch.", index));
					throw new OperationCanceledException();
				}

				Log.WarnException(string.Format("Failed to index documents for index: {0}", index), e);
			}
		}

		protected override void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not dispose of IndexingExecuter");

			exceptionAggregator.Execute(PrefetchingBehavior.Dispose);

			exceptionAggregator.ThrowIfNeeded();
		}
    }
}
