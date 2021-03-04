using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes
{
    public class LiveIndexingPerformanceCollector : DatabaseAwareLivePerformanceCollector<IndexPerformanceStats>
    {
        private readonly ConcurrentDictionary<string, IndexAndPerformanceStatsList> _perIndexStats =
            new ConcurrentDictionary<string, IndexAndPerformanceStatsList>();

        public LiveIndexingPerformanceCollector(DocumentDatabase documentDatabase, IEnumerable<string> indexNames)
            : base(documentDatabase)
        {
            foreach (var indexName in indexNames)
            {
                _perIndexStats.TryAdd(indexName, new IndexAndPerformanceStatsList(indexName));
            }

            Start();
        }

        protected override async Task StartCollectingStats()
        {
            Database.Changes.OnIndexChange += OnIndexChange;

            try
            {
                // This is done this way in order to avoid locking _perIndexStats
                // for fetching .Values
                List<IndexPerformanceStats> stats = new List<IndexPerformanceStats>();
                foreach (var x in Client.Extensions.EnumerableExtension.ForceEnumerateInThreadSafeManner(_perIndexStats))
                {
                    var index = Database.IndexStore.GetIndex(x.Value.Handler);
                    if (index != null)
                    {
                        stats.Add(new IndexPerformanceStats
                        {
                            Name = index.Name,
                            Performance = index.GetIndexingPerformance()
                        });
                    }
                }

                Stats.Enqueue(stats);

                await RunInLoop();
            }
            finally
            {
                Database.Changes.OnIndexChange -= OnIndexChange;
            }
        }

        protected override List<IndexPerformanceStats> PreparePerformanceStats()
        {
            var preparedStats = new List<IndexPerformanceStats>(_perIndexStats.Count);

            foreach (var keyValue in _perIndexStats)
            {
                // This is done this way instead of using
                // _perIndexStats.Values because .Values locks the entire
                // dictionary.
                var indexAndPerformanceStatsList = keyValue.Value;
                var indexName = indexAndPerformanceStatsList.Handler;
                var performance = indexAndPerformanceStatsList.Performance;

                var itemsToSend = new List<IndexingStatsAggregator>(performance.Count);

                while (performance.TryTake(out IndexingStatsAggregator stat))
                    itemsToSend.Add(stat);

                // if index still exists let's fetch latest stats from live instance
                var index = Database.IndexStore.GetIndex(indexName);

                var latestStats = index?.GetLatestIndexingStat();
                if (latestStats != null &&
                    latestStats.Completed == false &&
                    itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    preparedStats.Add(new IndexPerformanceStats
                    {
                        Name = indexName,
                        Performance = itemsToSend.Select(item => item.ToIndexingPerformanceLiveStatsWithDetails()).ToArray()
                    });
                }
            }
            return preparedStats;
        }

        protected override void WriteStats(List<IndexPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WritePerformanceStats(context, stats);
        }

        private void OnIndexChange(IndexChange change)
        {
            IndexAndPerformanceStatsList indexAndPerformanceStats;
            switch (change.Type)
            {
                case IndexChangeTypes.IndexRemoved:
                    _perIndexStats.TryRemove(change.Name, out indexAndPerformanceStats);
                    return;

                case IndexChangeTypes.Renamed:
                    var indexRenameChange = change as IndexRenameChange;
                    Debug.Assert(indexRenameChange != null);
                    _perIndexStats.TryRemove(indexRenameChange.OldIndexName, out indexAndPerformanceStats);
                    break;
            }

            if (change.Type != IndexChangeTypes.BatchCompleted && change.Type != IndexChangeTypes.IndexPaused)
                return;

            if (_perIndexStats.TryGetValue(change.Name, out indexAndPerformanceStats) == false)
            {
                var index = Database.IndexStore.GetIndex(change.Name);
                if (index == null)
                    return;

                indexAndPerformanceStats = new IndexAndPerformanceStatsList(change.Name);
                _perIndexStats.TryAdd(change.Name, indexAndPerformanceStats);
            }

            var indexInstance = Database.IndexStore.GetIndex(indexAndPerformanceStats.Handler);
            var latestStat = indexInstance?.GetLatestIndexingStat();
            if (latestStat != null)
                indexAndPerformanceStats.Performance.Add(latestStat);
        }

        private class IndexAndPerformanceStatsList : HandlerAndPerformanceStatsList<string, IndexingStatsAggregator>
        {
            public IndexAndPerformanceStatsList(string indexName) : base(indexName)
            {
            }
        }
    }
}
