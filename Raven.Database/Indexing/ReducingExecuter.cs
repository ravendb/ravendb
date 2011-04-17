using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Storage;
using Raven.Database.Tasks;

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
                    reduceKeyAndEtags = actions.MappedResults.GetMappedResultsReduceKeysAfter(indexToWorkOn.IndexName,
                                                                                           indexToWorkOn.LastIndexedEtag)
                        .ToList();

                    if(log.IsDebugEnabled)
                    {
                        log.DebugFormat("Found {0} reduce keys [{1}]",reduceKeyAndEtags.Count, string.Join(", ", reduceKeyAndEtags.Select(x=>x.ReduceKey)));
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


        private MappedResultInfo GetLastByEtag(List<MappedResultInfo> reduceKeyAndEtags)
        {
            // the last item is either the first or the last

            var first = reduceKeyAndEtags.First();
            var last = reduceKeyAndEtags.Last();

            if (new ComparableByteArray(first.Etag.ToByteArray()).CompareTo(new ComparableByteArray(last.Etag.ToByteArray())) > 0)
                return last;
            return first;
        }

        protected override bool IsIndexStale(IndexStats indexesStat, IStorageActionsAccessor actions)
        {
            return actions.Staleness.IsReduceStale(indexesStat.Name);
        }

        protected override void ExecuteIndexingWorkOnMultipleThreads(IEnumerable<IndexToWorkOn> indexesToWorkOn)
        {
            Parallel.ForEach(indexesToWorkOn, new ParallelOptions
            {
                MaxDegreeOfParallelism = context.Configuration.MaxNumberOfParallelIndexTasks,
                TaskScheduler = scheduler
            }, HandleReduceForIndex);
        }

        protected override void ExecuteIndexingWorkOnSingleThread(IEnumerable<IndexToWorkOn> indexesToWorkOn)
        {
            foreach (var indexToWorkOn in indexesToWorkOn)
            {
                HandleReduceForIndex(indexToWorkOn);
            }
        }

        protected override bool IsValidIndex(IndexStats indexesStat)
        {
            var indexDefinition = context.IndexDefinitionStorage.GetIndexDefinition(indexesStat.Name);
            return indexDefinition != null && indexDefinition.IsMapReduce;
        }
    }
}