using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Json;
using Raven.Database.Storage;
using Task = Raven.Database.Tasks.Task;

namespace Raven.Database.Indexing
{
	public class ReducingExecuter : AbstractIndexingExecuter
	{
		public ReducingExecuter(WorkContext context)
			: base(context)
		{
			autoTuner = new ReduceBatchSizeAutoTuner(context);
		}

		protected void HandleReduceForIndex(IndexToWorkOn indexToWorkOn)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexToWorkOn.IndexName);
			if (viewGenerator == null)
				return;
			TimeSpan reduceDuration = TimeSpan.Zero;
			int totalCount = 0;
			int totalSize = 0;
			bool operationCanceled = false;
			var itemsToDelete = new List<object>();
			try
			{
				var sw = Stopwatch.StartNew();
				for (int i = 0; i < 3; i++)
				{
					var level = i;
					transactionalStorage.Batch(actions =>
					{
						context.CancellationToken.ThrowIfCancellationRequested();

						var sp = Stopwatch.StartNew();
						var persistedResults = actions.MapReduce.GetItemsToReduce
							(
								take: context.CurrentNumberOfItemsToReduceInSingleBatch,
								level: level,
								index: indexToWorkOn.IndexName,
								itemsToDelete: itemsToDelete
							)
							.ToList();

						totalCount += persistedResults.Count;
						totalSize += persistedResults.Sum(x => x.Size);

						if (Log.IsDebugEnabled)
						{
							if (persistedResults.Count > 0)
								Log.Debug(() => string.Format("Found {0} results for keys [{1}] for index {2} at level {3} in {4}",
								persistedResults.Count, string.Join(", ", persistedResults.Select(x => x.ReduceKey).Distinct()), indexToWorkOn.IndexName, level, sp.Elapsed));
							else
								Log.Debug("No reduce keys found for {0}", indexToWorkOn.IndexName);
						}

						context.CancellationToken.ThrowIfCancellationRequested();

						var requiredReduceNextTime = persistedResults.Select(x => new ReduceKeyAndBucket(x.Bucket, x.ReduceKey))
							.OrderBy(x=>x.Bucket)
							.Distinct()
							.ToArray();
						foreach (var mappedResultInfo in requiredReduceNextTime)
						{
							actions.MapReduce.RemoveReduceResults(indexToWorkOn.IndexName, level + 1, mappedResultInfo.ReduceKey, mappedResultInfo.Bucket);
						}
						if (level != 2)
						{
							var reduceKeysAndBukcets = requiredReduceNextTime
								.Select(x => new ReduceKeyAndBucket(x.Bucket / 1024, x.ReduceKey))
								.Distinct()
								.ToArray();
							actions.MapReduce.ScheduleReductions(indexToWorkOn.IndexName, level + 1, reduceKeysAndBukcets);
						}

						var results = persistedResults
							.Where(x => x.Data != null)
							.GroupBy(x => x.Bucket, x => JsonToExpando.Convert(x.Data))
							.ToArray();
						var reduceKeys = new HashSet<string>(persistedResults.Select(x => x.ReduceKey),
															 StringComparer.InvariantCultureIgnoreCase);
						context.ReducedPerSecIncreaseBy(results.Length);

						context.CancellationToken.ThrowIfCancellationRequested();
						sp = Stopwatch.StartNew();
						context.IndexStorage.Reduce(indexToWorkOn.IndexName, viewGenerator, results, level, context, actions, reduceKeys);
						Log.Debug("Indexed {0} reduce keys in {1} with {2} results for index {3} in {4}", reduceKeys.Count, sp.Elapsed,
										results.Length, indexToWorkOn.IndexName, sp.Elapsed);
					});
				}
				reduceDuration = sw.Elapsed;
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
						var latest= actions.MapReduce.DeleteScheduledReduction(itemsToDelete);

						if(latest == null)
							return;
						actions.Indexing.UpdateLastReduced(indexToWorkOn.IndexName, latest.Etag, latest.Timestamp);
					});
					autoTuner.AutoThrottleBatchSize(totalCount, totalSize, reduceDuration);
				}
			}
		}


		private static MappedResultInfo GetLastByTimestamp(ICollection<MappedResultInfo> reduceKeyAndEtags)
		{
			if (reduceKeyAndEtags == null || reduceKeyAndEtags.Count == 0)
				return null;

			return reduceKeyAndEtags.OrderByDescending(x => x.Timestamp).First();
		}

		protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions)
		{
			return actions.Staleness.IsReduceStale(indexesStat.Name);
		}

		protected override Task GetApplicableTask(IStorageActionsAccessor actions)
		{
			return null;
			//return actions.Tasks.GetMergedTask<ReduceTask>();
		}

		protected override void FlushAllIndexes()
		{
			context.IndexStorage.FlushReduceIndexes();
		}

		protected override IndexToWorkOn GetIndexToWorkOn(IndexStats indexesStat)
		{
			return new IndexToWorkOn
			{
				IndexName = indexesStat.Name,
				LastIndexedEtag = Guid.Empty
			};
		}

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			BackgroundTaskExecuter.Instance.ExecuteAll(context, indexesToWorkOn, (indexToWorkOn, l) => HandleReduceForIndex(indexToWorkOn));
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Name);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}
