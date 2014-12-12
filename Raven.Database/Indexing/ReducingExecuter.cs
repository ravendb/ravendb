using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;

namespace Raven.Database.Indexing
{
	public class ReducingExecuter : AbstractIndexingExecuter
	{
		public ReducingExecuter(WorkContext context, IndexReplacer indexReplacer)
			: base(context, indexReplacer)
		{
			autoTuner = new ReduceBatchSizeAutoTuner(context);
		}

		protected ReducingPerformanceStats[] HandleReduceForIndex(IndexToWorkOn indexToWorkOn)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexToWorkOn.IndexId);
			if (viewGenerator == null)
				return null;

			bool operationCanceled = false;
			var itemsToDelete = new ConcurrentSet<object>();

			IList<ReduceTypePerKey> mappedResultsInfo = null;
			transactionalStorage.Batch(actions =>
			{
				mappedResultsInfo = actions.MapReduce.GetReduceTypesPerKeys(indexToWorkOn.IndexId,
					context.CurrentNumberOfItemsToReduceInSingleBatch,
					context.NumberOfItemsToExecuteReduceInSingleStep).ToList();
			});

			var singleStepReduceKeys = mappedResultsInfo.Where(x => x.OperationTypeToPerform == ReduceType.SingleStep).Select(x => x.ReduceKey).ToArray();
			var multiStepsReduceKeys = mappedResultsInfo.Where(x => x.OperationTypeToPerform == ReduceType.MultiStep).Select(x => x.ReduceKey).ToArray();

			currentlyProcessedIndexes.TryAdd(indexToWorkOn.IndexId, indexToWorkOn.Index);

			var performanceStats = new List<ReducingPerformanceStats>();

			try
			{
				if (singleStepReduceKeys.Length > 0)
				{
					Log.Debug("SingleStep reduce for keys: {0}",singleStepReduceKeys.Select(x => x + ","));
					var singleStepStats = SingleStepReduce(indexToWorkOn, singleStepReduceKeys, viewGenerator, itemsToDelete);

					performanceStats.Add(singleStepStats);
				}

				if (multiStepsReduceKeys.Length > 0)
				{
					Log.Debug("MultiStep reduce for keys: {0}", singleStepReduceKeys.Select(x => x + ","));
					var multiStepStats = MultiStepReduce(indexToWorkOn, multiStepsReduceKeys, viewGenerator, itemsToDelete);

					performanceStats.Add(multiStepStats);
				}
			}
			catch (OperationCanceledException)
			{
				operationCanceled = true;
			}
			finally
			{
				if (operationCanceled == false)
				{
					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed mapped results
					transactionalStorage.Batch(actions =>
					{
						var latest = actions.MapReduce.DeleteScheduledReduction(itemsToDelete);

						if (latest == null)
							return;
						actions.Indexing.UpdateLastReduced(indexToWorkOn.Index.indexId, latest.Etag, latest.Timestamp);
					});
				}

				Index _;
				currentlyProcessedIndexes.TryRemove(indexToWorkOn.IndexId, out _);
			}

			return performanceStats.ToArray();
		}

	    protected override void UpdateStalenessMetrics(int staleCount)
	    {
	        context.MetricsCounters.StaleIndexReduces.Update(staleCount);
	    }

		protected override bool ShouldSkipIndex(Index index)
		{
			return false;
		}

		private ReducingPerformanceStats MultiStepReduce(IndexToWorkOn index, string[] keysToReduce, AbstractViewGenerator viewGenerator, ConcurrentSet<object> itemsToDelete)
		{
			var needToMoveToMultiStep = new HashSet<string>();
			transactionalStorage.Batch(actions =>
			{
				foreach (var localReduceKey in keysToReduce)
				{
					var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexId, localReduceKey);

					if (lastPerformedReduceType != ReduceType.MultiStep)
						needToMoveToMultiStep.Add(localReduceKey);

					if (lastPerformedReduceType != ReduceType.SingleStep)
						continue;
					// we exceeded the limit of items to reduce in single step
					// now we need to schedule reductions at level 0 for all map results with given reduce key
					var mappedItems = actions.MapReduce.GetMappedBuckets(index.IndexId, localReduceKey).ToList();
					foreach (var result in mappedItems.Select(x => new ReduceKeyAndBucket(x, localReduceKey)))
					{
						actions.MapReduce.ScheduleReductions(index.IndexId, 0, result);
					}
				}
			});

			var reducePerformance = new ReducingPerformanceStats
			{
				ReduceType = ReduceType.MultiStep,
				LevelStats = new List<ReduceLevelPeformanceStats>()
			};

			for (int i = 0; i < 3; i++)
			{
				var level = i;

				var reduceLevelStats = new ReduceLevelPeformanceStats()
				{
					Level = level,
					Started = SystemTime.UtcNow,
				};

				var reduceParams = new GetItemsToReduceParams(
					index.IndexId,
					keysToReduce,
					level,
					true,
					itemsToDelete);

				var gettigItemsToReduceDuration = new Stopwatch();

				bool retry = true;
				while (retry && reduceParams.ReduceKeys.Count > 0)
				{
					var reduceBatchAutoThrottlerId = Guid.NewGuid();
					try
					{
						transactionalStorage.Batch(actions =>
						{
							context.CancellationToken.ThrowIfCancellationRequested();

							var batchTimeWatcher = Stopwatch.StartNew();

							reduceParams.Take = context.CurrentNumberOfItemsToReduceInSingleBatch;

							gettigItemsToReduceDuration.Start();
							var persistedResults = actions.MapReduce.GetItemsToReduce(reduceParams).ToList();
							gettigItemsToReduceDuration.Stop();
							if (persistedResults.Count == 0)
							{
								retry = false;
								return;
							}

							var count = persistedResults.Count;
							var size = persistedResults.Sum(x => x.Size);
							autoTuner.CurrentlyUsedBatchSizesInBytes.GetOrAdd(reduceBatchAutoThrottlerId, size);

							if (Log.IsDebugEnabled)
							{
								if (persistedResults.Count > 0)
									Log.Debug(() => string.Format("Found {0} results for keys [{1}] for index {2} at level {3} in {4}",
										persistedResults.Count,
										string.Join(", ", persistedResults.Select(x => x.ReduceKey).Distinct()),
										index.IndexId, level, batchTimeWatcher.Elapsed));
								else
									Log.Debug("No reduce keys found for {0}", index.IndexId);
							}

							context.CancellationToken.ThrowIfCancellationRequested();

							var requiredReduceNextTime = persistedResults.Select(x => new ReduceKeyAndBucket(x.Bucket, x.ReduceKey))
								.OrderBy(x => x.Bucket)
								.Distinct()
								.ToArray();
							foreach (var mappedResultInfo in requiredReduceNextTime)
							{
								actions.MapReduce.RemoveReduceResults(index.IndexId, level + 1, mappedResultInfo.ReduceKey,
									mappedResultInfo.Bucket);
							}

							if (level != 2)
							{
								var reduceKeysAndBuckets = requiredReduceNextTime
									.Select(x => new ReduceKeyAndBucket(x.Bucket/1024, x.ReduceKey))
									.Distinct()
									.ToArray();
								foreach (var reduceKeysAndBucket in reduceKeysAndBuckets)
								{
									actions.MapReduce.ScheduleReductions(index.IndexId, level + 1, reduceKeysAndBucket);
								}
							}

							var results = persistedResults
								.Where(x => x.Data != null)
								.GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data))
								.ToArray();
							var reduceKeys = new HashSet<string>(persistedResults.Select(x => x.ReduceKey),
								StringComparer.InvariantCultureIgnoreCase);

							context.MetricsCounters.ReducedPerSecond.Mark(results.Length);

							context.CancellationToken.ThrowIfCancellationRequested();
							var reduceTimeWatcher = Stopwatch.StartNew();

							var performance = context.IndexStorage.Reduce(index.IndexId, viewGenerator, results, level, context, actions, reduceKeys, persistedResults.Count);

							reduceLevelStats.Add(performance);

							var batchDuration = batchTimeWatcher.Elapsed;
							Log.Debug("Indexed {0} reduce keys in {1} with {2} results for index {3} in {4} on level {5}", reduceKeys.Count, batchDuration,
								results.Length, index.IndexId, reduceTimeWatcher.Elapsed, level);

							autoTuner.AutoThrottleBatchSize(count, size, batchDuration);
						});
					}
					finally
					{
						long _;
						autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(reduceBatchAutoThrottlerId, out _);
					}
				}

				reduceLevelStats.Completed = SystemTime.UtcNow;
				reduceLevelStats.Duration = reduceLevelStats.Completed - reduceLevelStats.Started;
				reduceLevelStats.ReduceStoragePerformance.GetItemsToReduceDurationMs = gettigItemsToReduceDuration.ElapsedMilliseconds;

				reducePerformance.LevelStats.Add(reduceLevelStats);
			}

			foreach (var reduceKey in needToMoveToMultiStep)
			{
				string localReduceKey = reduceKey;
				transactionalStorage.Batch(actions =>
										   actions.MapReduce.UpdatePerformedReduceType(index.IndexId, localReduceKey,
																					   ReduceType.MultiStep));
			}

			return reducePerformance;
		}

		private ReducingPerformanceStats SingleStepReduce(IndexToWorkOn index, string[] keysToReduce, AbstractViewGenerator viewGenerator,
												ConcurrentSet<object> itemsToDelete)
		{
			var needToMoveToSingleStepQueue = new ConcurrentQueue<HashSet<string>>();

			Log.Debug(() => string.Format("Executing single step reducing for {0} keys [{1}]", keysToReduce.Length, string.Join(", ", keysToReduce)));
			var batchTimeWatcher = Stopwatch.StartNew();
			var reducingBatchThrottlerId = Guid.NewGuid();

			var reducePerformanceStats = new ReducingPerformanceStats()
			{
				ReduceType = ReduceType.SingleStep,
				LevelStats = new List<ReduceLevelPeformanceStats>()
			};

			var reduceLevelStats = new ReduceLevelPeformanceStats()
			{
				Started = SystemTime.UtcNow,
				Level = 2
			};

			try
			{
				var performanceStats = new ConcurrentQueue<IndexingPerformanceStats>();

				long gettigItemsToReduceDurationMs = 0;

				BackgroundTaskExecuter.Instance.ExecuteAllBuffered(context, keysToReduce, enumerator =>
				{
					var localNeedToMoveToSingleStep = new HashSet<string>();
					needToMoveToSingleStepQueue.Enqueue(localNeedToMoveToSingleStep);
					var localKeys = new HashSet<string>();
					while (enumerator.MoveNext())
					{
						localKeys.Add(enumerator.Current);
					}

					transactionalStorage.Batch(actions =>
					{
						var getItemsToReduceParams = new GetItemsToReduceParams(index: index.IndexId, reduceKeys: localKeys, level: 0,
							loadData: false,
							itemsToDelete: itemsToDelete)
						{
							Take = int.MaxValue // just get all, we do the rate limit when we load the number of keys to reduce, anyway
						};


						var getItemsToReduceDuration = Stopwatch.StartNew();
						var scheduledItems = actions.MapReduce.GetItemsToReduce(getItemsToReduceParams).ToList();

						Interlocked.Add(ref gettigItemsToReduceDurationMs, getItemsToReduceDuration.ElapsedMilliseconds);

						autoTuner.CurrentlyUsedBatchSizesInBytes.GetOrAdd(reducingBatchThrottlerId, scheduledItems.Sum(x => x.Size));
						if (scheduledItems.Count == 0)
						{
							if (Log.IsWarnEnabled)
							{
								Log.Warn("Found single reduce items ({0}) that didn't have any items to reduce. Deleting level 1 & level 2 items for those keys. (If you can reproduce this, please contact support@ravendb.net)",
									string.Join(", ", keysToReduce));
							}
							// Here we have an interesting issue. We have scheduled reductions, because GetReduceTypesPerKeys() returned them
							// and at the same time, we don't have any at level 0. That probably means that we have them at level 1 or 2.
							// They shouldn't be here, and indeed, we remove them just a little down from here in this function.
							// That said, they might bave smuggled in between versions, or something happened to cause them to be here.
							// In order to avoid that, we forcibly delete those extra items from the scheduled reductions, and move on
							foreach (var reduceKey in keysToReduce)
							{
								actions.MapReduce.DeleteScheduledReduction(index.IndexId, 1, reduceKey);
								actions.MapReduce.DeleteScheduledReduction(index.IndexId, 2, reduceKey);
							}
						}

						foreach (var reduceKey in localKeys)
						{
							var lastPerformedReduceType = actions.MapReduce.GetLastPerformedReduceType(index.IndexId, reduceKey);

							if (lastPerformedReduceType != ReduceType.SingleStep)
								localNeedToMoveToSingleStep.Add(reduceKey);

							if (lastPerformedReduceType != ReduceType.MultiStep)
								continue;

							Log.Debug("Key {0} was moved from multi step to single step reduce, removing existing reduce results records",
								reduceKey);

							// now we are in single step but previously multi step reduce was performed for the given key
							var mappedBuckets = actions.MapReduce.GetMappedBuckets(index.IndexId, reduceKey).ToList();

							// add scheduled items too to be sure we will delete reduce results of already deleted documents
							mappedBuckets.AddRange(scheduledItems.Select(x => x.Bucket));

							foreach (var mappedBucket in mappedBuckets.Distinct())
							{
								actions.MapReduce.RemoveReduceResults(index.IndexId, 1, reduceKey, mappedBucket);
								actions.MapReduce.RemoveReduceResults(index.IndexId, 2, reduceKey, mappedBucket/1024);
							}
						}
					});
				});

				var keysLeftToReduce = new HashSet<string>(keysToReduce);
				while (keysLeftToReduce.Count > 0)
				{
					context.TransactionalStorage.Batch(
						actions =>
						{
							context.CancellationToken.ThrowIfCancellationRequested();
							var take = context.CurrentNumberOfItemsToReduceInSingleBatch;
							var keysReturned = new HashSet<string>();
							var mappedResults = actions.MapReduce.GetMappedResults(
								index.IndexId,
								keysLeftToReduce,
								true,
								take,
								keysReturned
								).ToList();

							var count = mappedResults.Count;
							var size = mappedResults.Sum(x => x.Size);

							mappedResults.ApplyIfNotNull(x => x.Bucket = 0);

							var results =
								mappedResults.Where(x => x.Data != null).GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data)).ToArray();

							context.MetricsCounters.ReducedPerSecond.Mark(results.Length);

							context.CancellationToken.ThrowIfCancellationRequested();

							var performance = context.IndexStorage.Reduce(index.IndexId, viewGenerator, results, 2, context, actions, keysReturned, mappedResults.Count);

							performanceStats.Enqueue(performance);

							autoTuner.AutoThrottleBatchSize(count, size, batchTimeWatcher.Elapsed);
						});
				}

				var needToMoveToSingleStep = new HashSet<string>();
				HashSet<string> set;
				while (needToMoveToSingleStepQueue.TryDequeue(out set))
				{
					needToMoveToSingleStep.UnionWith(set);
				}

				foreach (var reduceKey in needToMoveToSingleStep)
				{
					string localReduceKey = reduceKey;
					transactionalStorage.Batch(actions =>
						actions.MapReduce.UpdatePerformedReduceType(index.IndexId, localReduceKey, ReduceType.SingleStep));
				}

				reduceLevelStats.Completed = SystemTime.UtcNow;
				reduceLevelStats.Duration = reduceLevelStats.Completed - reduceLevelStats.Started;
				reduceLevelStats.ReduceStoragePerformance.GetItemsToReduceDurationMs = gettigItemsToReduceDurationMs;

				foreach (var stats in performanceStats)
				{
					reduceLevelStats.Add(stats);
				}

				reducePerformanceStats.LevelStats.Add(reduceLevelStats);
			}
			finally
			{
				long _;
				autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(reducingBatchThrottlerId, out _);
			}

			return reducePerformanceStats;
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions, bool isIdle, Reference<bool> onlyFoundIdleWork)
		{
			onlyFoundIdleWork.Value = false;
			var isReduceStale = actions.Staleness.IsReduceStale(indexesStat.Id);

			if (isReduceStale == false)
				return false;

			if (indexesStat.Priority.HasFlag(IndexingPriority.Error))
				return false;

			return true;
		}

		protected override DatabaseTask GetApplicableTask(IStorageActionsAccessor actions)
		{
			return null;
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushReduceIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexId = indexesStat.Id,
				LastIndexedEtag = Etag.Empty
			};
		}

        protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			ReducingBatchInfo reducingBatchInfo = null;

	        try
	        {
		        context.ReportReducingBatchStarted(indexesToWorkOn.Select(x => x.Index.PublicName).ToList(), out reducingBatchInfo);

		        BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(context, indexesToWorkOn, index =>
		        {
			        var performanceStats = HandleReduceForIndex(index);

					foreach (var stats in performanceStats)
				    {
						reducingBatchInfo.PerformanceStats.TryAdd(index.Index.PublicName, stats);
				    }
		        });
	        }
	        finally
	        {
		        if(reducingBatchInfo != null)
					reducingBatchInfo.BatchCompleted();
	        }
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Id);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}
