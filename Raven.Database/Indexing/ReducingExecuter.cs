using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Task = Raven.Database.Tasks.Task;

namespace Raven.Database.Indexing
{
	public class ReducingExecuter : AbstractIndexingExecuter
	{
		public ReducingExecuter(ITransactionalStorage transactionalStorage, WorkContext context, TaskScheduler scheduler)
			: base(transactionalStorage, context, scheduler)
		{
			autoTuner = new ReduceBatchSizeAutoTuner(context);
		}

		protected void HandleReduceForIndex(IndexToWorkOn indexToWorkOn)
		{
			var viewGenerator = context.IndexDefinitionStorage.GetViewGenerator(indexToWorkOn.IndexName);
			if(viewGenerator == null)
				return;
			TimeSpan reduceDuration = TimeSpan.Zero;
			List<MappedResultInfo> persistedResults = null;
			bool operationCanceled = false;
			try
			{
				var sw = Stopwatch.StartNew();
				for (int level = 0; level < 3; level++)
				{
					transactionalStorage.Batch(actions =>
					{
						context.CancellationToken.ThrowIfCancellationRequested();
						
						var sp = Stopwatch.StartNew();
						persistedResults = actions.MapRduce.GetItemsToReduce
							(
								take: autoTuner.NumberOfItemsToIndexInSingleBatch,
								level: 1,
								index: indexToWorkOn.IndexName
							)
							.ToList();

						if (log.IsDebugEnabled)
						{
							if (persistedResults.Count > 0)
								log.Debug(() => string.Format("Found {0} results for keys [{1}] for index {2} at level {3} in {4}",
								persistedResults.Count, string.Join(", ", persistedResults.Select(x => x.ReduceKey).Distinct()), indexToWorkOn.IndexName, level, sp.Elapsed));
							else
								log.Debug("No reduce keys found for {0}", indexToWorkOn.IndexName);
						}

						context.CancellationToken.ThrowIfCancellationRequested();


						var results = persistedResults
							.Where(x=>x.Data != null)
							.GroupBy(x => x.Bucket, x=> JsonToExpando.Convert(x.Data))
							.ToArray();
						var reduceKeys = persistedResults.Select(x => x.ReduceKey).ToArray();
						context.ReducedPerSecIncreaseBy(results.Length);

						context.CancellationToken.ThrowIfCancellationRequested();
						sp = Stopwatch.StartNew();
						context.IndexStorage.Reduce(indexToWorkOn.IndexName, viewGenerator, results, level, context, actions, reduceKeys);
						log.Debug("Indexed {0} reduce keys in {1} with {2} results for index {3} in {4}", reduceKeys.Length, sp.Elapsed,
										results.Length, indexToWorkOn.IndexName, sp.Elapsed);
					});
				}
				reduceDuration = sw.Elapsed;
			}
			catch(OperationCanceledException)
			{
				operationCanceled = true;
			}
			finally
			{
				if (operationCanceled == false && persistedResults != null && persistedResults.Count > 0)
				{
					var lastByEtag = GetLastByEtag(persistedResults);
					var lastEtag = lastByEtag.Etag;

					var lastIndexedEtag = new ComparableByteArray(lastEtag.ToByteArray());
					// whatever we succeeded in indexing or not, we have to update this
					// because otherwise we keep trying to re-index failed mapped results
					transactionalStorage.Batch(actions =>
					{
						if (new ComparableByteArray(indexToWorkOn.LastIndexedEtag.ToByteArray()).CompareTo(lastIndexedEtag) <= 0)
						{
							actions.Indexing.UpdateLastReduced(indexToWorkOn.IndexName, lastByEtag.Etag, lastByEtag.Timestamp);
						}
					});

					autoTuner.AutoThrottleBatchSize(persistedResults.Count, persistedResults.Sum(x => x.Size), reduceDuration);
				}
			}
		}


// ReSharper disable ParameterTypeCanBeEnumerable.Local
		private static MappedResultInfo GetLastByEtag(List<MappedResultInfo> reduceKeyAndEtags)
// ReSharper restore ParameterTypeCanBeEnumerable.Local
		{
			// the last item is either the first or the last

			var first = reduceKeyAndEtags.First();
			var last = reduceKeyAndEtags.Last();

			if (new ComparableByteArray(first.Etag.ToByteArray()).CompareTo(new ComparableByteArray(last.Etag.ToByteArray())) < 0)
				return last;
			return first;
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
				LastIndexedEtag = indexesStat.LastReducedEtag ?? Guid.Empty
			};
		}

		protected override void ExecuteIndexingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			BackgroundTaskExecuter.Instance.ExecuteAll(context.Configuration, scheduler, context, indexesToWorkOn, (indexToWorkOn, l) => HandleReduceForIndex(indexToWorkOn));
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Name);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}
