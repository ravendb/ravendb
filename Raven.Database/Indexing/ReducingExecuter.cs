using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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
		}

		protected void HandleReduceForIndex(IndexToWorkOn indexToWorkOn)
		{
			List<MappedResultInfo> reduceKeyAndEtags = null;
			try
			{
				transactionalStorage.Batch(actions =>
				{
					reduceKeyAndEtags = actions.MappedResults.GetMappedResultsReduceKeysAfter
						(
							indexToWorkOn.IndexName,
							indexToWorkOn.LastIndexedEtag,
							loadData: false,
							// for reduce operations, we use the smaller value, rather than tuning stuff on the fly
							// the reason for that is that we may have large number of map values to reduce anyway, 
							// so we don't want to try to load too much all at once.
							take: context.Configuration.InitialNumberOfItemsToIndexInSingleBatch
						)
						.ToList();

					if(log.IsDebugEnabled)
					{
						if (reduceKeyAndEtags.Count > 0)
							log.Debug(() => string.Format("Found {0} mapped results for keys [{1}] for index {2}", reduceKeyAndEtags.Count, string.Join(", ", reduceKeyAndEtags.Select(x => x.ReduceKey).Distinct()), indexToWorkOn.IndexName));
						else
							log.Debug("No reduce keys found for {0}", indexToWorkOn.IndexName);
					}

					new ReduceTask
					{
						Index = indexToWorkOn.IndexName,
						ReduceKeys = reduceKeyAndEtags.Select(x => x.ReduceKey).Distinct().ToArray(),
					}.Execute(context);
				});
			}
			finally
			{
				if (reduceKeyAndEtags != null && reduceKeyAndEtags.Count > 0)
				{
					var lastByEtag = GetLastByEtag(reduceKeyAndEtags);
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
			return actions.Tasks.GetMergedTask<ReduceTask>();
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

		protected override void ExecuteIndxingWork(IList<IndexToWorkOn> indexesToWorkOn)
		{
			BackgroundTaskExecuter.Instance.ExecuteAll(context.Configuration, scheduler, indexesToWorkOn, (indexToWorkOn, l) => HandleReduceForIndex(indexToWorkOn));
		}

		protected override bool IsValidIndex(IndexStats indexesStat)
		{
			var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Name);
			return indexDefinition != null && indexDefinition.IsMapReduce;
		}
	}
}