//-----------------------------------------------------------------------
// <copyright file="IndexingExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
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
		readonly PrefetchingBehavior prefetchingBehavior;

		public IndexingExecuter(WorkContext context)
			: base(context)
		{
			autoTuner = new IndexBatchSizeAutoTuner(context);
			prefetchingBehavior = new PrefetchingBehavior(context, autoTuner);
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
				LastIndexedEtag = indexesStat.LastIndexedEtag,
			};
		}

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			indexesToWorkOn = context.Configuration.IndexingScheduler.FilterMapIndexes(indexesToWorkOn);

			var lastIndexedGuidForAllIndexes = indexesToWorkOn.Min(x => new ComparableByteArray(x.LastIndexedEtag.ToByteArray())).ToGuid();

			context.CancellationToken.ThrowIfCancellationRequested();

			var operationCancelled = false;
			TimeSpan indexingDuration = TimeSpan.Zero;
			List<JsonDocument> jsonDocs = null;
			var lastEtag = Guid.Empty;
			try
			{
				jsonDocs = prefetchingBehavior.GetDocumentsBatchFrom(lastIndexedGuidForAllIndexes);

				if (Log.IsDebugEnabled)
				{
					Log.Debug("Found a total of {0} documents that requires indexing since etag: {1}: ({2})",
							  jsonDocs.Count, lastIndexedGuidForAllIndexes, string.Join(", ", jsonDocs.Select(x => x.Key)));
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
					prefetchingBehavior.CleanupDocumentsToRemove(lastEtag);
					UpdateAutoThrottler(jsonDocs, indexingDuration);
				}

				prefetchingBehavior.BatchProcessingComplete();
			}
		}

		private Guid DoActualIndexing(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs)
		{
			var lastByEtag = PrefetchingBehavior.GetHighestJsonDocumentByEtag(jsonDocs);
			var lastModified = lastByEtag.LastModified.Value;
			var lastEtag = lastByEtag.Etag.Value;

			context.IndexedPerSecIncreaseBy(jsonDocs.Count);
			var result = FilterIndexes(indexesToWorkOn, jsonDocs).ToList();

			ExecuteAllInterleaved(result, index => HandleIndexingFor(index, lastEtag, lastModified));

			return lastEtag;
		}

		SemaphoreSlim indexingSemaphore;
		private ManualResetEventSlim indexingCompletedEvent;
		readonly ConcurrentSet<System.Threading.Tasks.Task> pendingTasks = new ConcurrentSet<System.Threading.Tasks.Task>();

		private void ExecuteAllInterleaved(IList<IndexingBatchForIndex> result, Action<IndexingBatchForIndex> action)
		{
			if (result.Count == 0)
				return;
			/*
			This is EXPLICILTY not here, we always want to allow this to run additional indexes
			if we still have free spots to run them from threading perspective.
			 
			if (result.Count == 1)
			{
				action(result[0]);
				return;
			}
			*/

			var maxNumberOfParallelIndexTasks = context.Configuration.MaxNumberOfParallelIndexTasks;

			SortResultsMixedAccordingToTimePerDoc(result);

			int isSlowIndex = 0;
			var totalIndexingTime = Stopwatch.StartNew();
			var tasks = new System.Threading.Tasks.Task[result.Count];
			for (int i = 0; i < result.Count; i++)
			{
				var index = result[i];
				var indexToWorkOn = index;

				var sp = Stopwatch.StartNew();
				var task = new System.Threading.Tasks.Task(() => action(indexToWorkOn));
				indexToWorkOn.Index.CurrentMapIndexingTask = tasks[i] = task.ContinueWith(done =>
				{
					try
					{
						sp.Stop();

						if (done.IsFaulted) // this observe the exception
						{
							Log.WarnException("Failed to execute indexing task", done.Exception);
						}

						indexToWorkOn.Index.LastIndexingDuration = sp.Elapsed;
						indexToWorkOn.Index.TimePerDoc = sp.ElapsedMilliseconds / Math.Max(1, indexToWorkOn.Batch.Docs.Count);
						indexToWorkOn.Index.CurrentMapIndexingTask = null;

						return done;
					}
					finally
					{
						indexingSemaphore.Release();
						indexingCompletedEvent.Set();
						if (Thread.VolatileRead(ref isSlowIndex) != 0)
						{
							// we now need to notify the engine that the slow index(es) is done, and we need to resume its indexing
							context.ShouldNotifyAboutWork(() => "Slow Index Completed Indexing Batch");
							context.NotifyAboutWork();
						}
					}
				}).Unwrap();

				indexingSemaphore.Wait();

				task.Start(context.Database.BackgroundTaskScheduler);
			}

			// we only get here AFTER we finished scheduling all the indexes
			// we wait until we have at least parallel / 2 spots opened (for the _next_ indexing batch) or 8, if we are 
			// running on Enterprise / high end systems
			int minIndexingSpots = Math.Min((maxNumberOfParallelIndexTasks / 2), 8);
			while (indexingSemaphore.CurrentCount < minIndexingSpots)
			{
				indexingCompletedEvent.Wait();
				indexingCompletedEvent.Reset();
			}

			// now we have the chance to start a new indexing batch with the old items, but we still
			// want to wait for a bit to _avoid_ creating multiple batches if we can possibly avoid it.
			// We will wait for 3/4 the time we waited so far, and a min of 15 seconds
			var timeToWait = Math.Max((int)(totalIndexingTime.ElapsedMilliseconds / 4) * 3, 15000);
			var totalWaitTime = Stopwatch.StartNew();
			while (indexingSemaphore.CurrentCount < maxNumberOfParallelIndexTasks)
			{
				int timeout = timeToWait - (int)totalWaitTime.ElapsedMilliseconds;
				if (timeout <= 0)
					break;
				indexingCompletedEvent.Reset();
				indexingCompletedEvent.Wait(timeout);
			}

			var creatingNewBatch = indexingSemaphore.CurrentCount < maxNumberOfParallelIndexTasks;
			if (creatingNewBatch == false)
				return;

			Interlocked.Increment(ref isSlowIndex);

			if (Log.IsDebugEnabled == false)
				return;

			var slowIndexes = result.Where(x =>
			{
				var currentMapIndexingTask = x.Index.CurrentMapIndexingTask;
				return currentMapIndexingTask != null && !currentMapIndexingTask.IsCompleted;
			})
				.Select(x => x.IndexName)
				.ToArray();

			Log.Debug("Indexing is now split because there are {0:#,#} slow indexes [{1}], memory usage may increase, and those indexing may experience longer stale times (but other indexes will be faster)",
					 slowIndexes.Length,
					 string.Join(", ", slowIndexes));
		}

		private static void SortResultsMixedAccordingToTimePerDoc(IList<IndexingBatchForIndex> result)
		{
			var orderedBy = result.OrderBy(x => x.Index.TimePerDoc).ToArray();
			int startPos = 0, endPos = orderedBy.Length;
			int resultPos = 0;
			while (startPos < endPos)
			{
				result[resultPos++] = orderedBy[startPos++];
				if (resultPos + 1 < result.Count)
					result[resultPos++] = orderedBy[--endPos];
			}
		}

		private void HandleIndexingFor(IndexingBatchForIndex batchForIndex, Guid lastEtag, DateTime lastModified)
		{
			try
			{
				transactionalStorage.Batch(actions => IndexDocuments(actions, batchForIndex.IndexName, batchForIndex.Batch));
			}
			catch (Exception e)
			{
				Log.Warn("Failed to index " + batchForIndex.IndexName, e);
			}
			finally
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

		private void UpdateAutoThrottler(List<JsonDocument> jsonDocs, TimeSpan indexingDuration)
		{
			int futureLen;
			int futureSize;
			prefetchingBehavior.GetFutureStats(autoTuner.NumberOfItemsToIndexInSingleBatch, out futureLen, out futureSize);
			autoTuner.AutoThrottleBatchSize(jsonDocs.Count + futureLen, futureSize + jsonDocs.Sum(x => x.SerializedSizeOnDisk), indexingDuration);
		}

		public class IndexingBatchForIndex
		{
			public string IndexName { get; set; }

			public Index Index { get; set; }

			public Guid LastIndexedEtag { get; set; }

			public IndexingBatch Batch { get; set; }
		}

		private IEnumerable<IndexingBatchForIndex> FilterIndexes(IList<IndexToWorkOn> indexesToWorkOn, List<JsonDocument> jsonDocs)
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

			var results = new IndexingBatchForIndex[indexesToWorkOn.Count];
			var actions = new Action<IStorageActionsAccessor>[indexesToWorkOn.Count];

			BackgroundTaskExecuter.Instance.ExecuteAll(context, indexesToWorkOn, (indexToWorkOn, i) =>
			{
				var indexLastIndexEtag = new ComparableByteArray(indexToWorkOn.LastIndexedEtag.ToByteArray());
				if (indexLastIndexEtag.CompareTo(lastIndexedEtag) >= 0)
					return;

				var indexName = indexToWorkOn.IndexName;
				var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexName);
				if (viewGenerator == null)
					return; // probably deleted

				var batch = new IndexingBatch();

				foreach (var item in filteredDocs)
				{
					if (prefetchingBehavior.FilterDocuments(item.Doc))
						continue;

					// did we already indexed this document in this index?
					var etag = item.Doc.Etag;
					if (etag == null)
						continue;

					if (indexLastIndexEtag.CompareTo(new ComparableByteArray(etag.Value.ToByteArray())) >= 0)
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
				Log.WarnException(string.Format("Failed to index documents for index: {0}", index), e);
			}
		}

		public PrefetchingBehavior PrefetchingBehavior
		{
			get { return prefetchingBehavior; }
		}

		protected override void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not dispose of IndexingExecuter");
			foreach (var pendingTask in pendingTasks)
			{
				exceptionAggregator.Execute(pendingTask.Wait);
			}
			pendingTasks.Clear();
			if (indexingCompletedEvent != null)
				exceptionAggregator.Execute(indexingCompletedEvent.Dispose);
			if (indexingSemaphore != null)
				exceptionAggregator.Execute(indexingSemaphore.Dispose);
			exceptionAggregator.ThrowIfNeeded();

			indexingCompletedEvent = null;
			indexingSemaphore = null;
		}

		protected override void Init()
		{
			indexingSemaphore = new SemaphoreSlim(context.Configuration.MaxNumberOfParallelIndexTasks);
			indexingCompletedEvent = new ManualResetEventSlim(false);
			base.Init();
		}
	}
}