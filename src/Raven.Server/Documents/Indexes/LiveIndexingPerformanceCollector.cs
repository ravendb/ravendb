using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Sparrow.Collections;

namespace Raven.Server.Documents.Indexes
{
    public class LiveIndexingPerformanceCollector : IDisposable
    {
        private readonly DocumentsChanges _changes;
        private readonly Dictionary<string, IndexAndPerformanceStatsList> _perIndexStats;
        private readonly CancellationToken _resourceShutdown;
        private readonly CancellationTokenSource _cts;

        public LiveIndexingPerformanceCollector(DocumentsChanges changes, CancellationToken resourceShutdown, IEnumerable<Index> indexes)
        {
            _changes = changes;
            _resourceShutdown = resourceShutdown;
            _perIndexStats = indexes.ToDictionary(x => x.Name, x => new IndexAndPerformanceStatsList(x));
            _cts = new CancellationTokenSource();

            Task.Run(StartWritingStats);
        }

        public void Dispose()
        {
            _changes.OnIndexChange -= OnIndexChange;
            _cts.Cancel();
            _cts.Dispose();
        }

        public AsyncQueue<IndexPerformanceStats[]> Queue { get; } = new AsyncQueue<IndexPerformanceStats[]>();

        public async Task StartWritingStats()
        {
            _changes.OnIndexChange += OnIndexChange;

            var stats = _perIndexStats.Values
                    .Select(x => new IndexPerformanceStats
                    {
                        IndexName = x.Index.Name,
                        IndexId = x.Index.IndexId,
                        Performance = x.Index.GetIndexingPerformance()
                    })
                    .ToArray();

            Queue.Enqueue(stats);

            using (var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(_resourceShutdown, _cts.Token))
            {
                var token = linkedToken.Token;

                while (token.IsCancellationRequested == false)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), token);

                    if (token.IsCancellationRequested)
                        break;

                    var performanceStats = PreparePerformanceStats();

                    if (performanceStats.Length > 0)
                    {
                        Queue.Enqueue(performanceStats);
                    }
                }
            }
            
        }

        private IndexPerformanceStats[] PreparePerformanceStats()
        {
            return _perIndexStats.Values.Select(x =>
            {
                var index = x.Index;
                var performance = x.Performance;
                var itemsToSend = new List<IndexingStatsAggregator>(performance.Count);

                IndexingStatsAggregator stat;
                while (performance.TryTake(out stat))
                    itemsToSend.Add(stat);

                var latestStats = index.GetLatestIndexingStat();

                if (latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                return new IndexPerformanceStats
                {
                    IndexName = index.Name,
                    IndexId = index.IndexId,
                    Performance = itemsToSend.Select(item => item.ToIndexingPerformanceLiveStatsWithDetails()).ToArray()
                };
            })
            .Where(x => x.Performance.Length > 0)
            .ToArray();
        }

        private void OnIndexChange(IndexChange change)
        {
            if (change.Type != IndexChangeTypes.BatchCompleted)
                return;

            IndexAndPerformanceStatsList indexAndPerformanceStats;
            if (_perIndexStats.TryGetValue(change.Name, out indexAndPerformanceStats) == false)
                return;

            var latestStat = indexAndPerformanceStats.Index.GetLatestIndexingStat();
            if (latestStat != null)
                indexAndPerformanceStats.Performance.Add(latestStat, _resourceShutdown);
        }

        private class IndexAndPerformanceStatsList
        {
            public readonly Index Index;

            public readonly BlockingCollection<IndexingStatsAggregator> Performance;

            public IndexAndPerformanceStatsList(Index index)
            {
                Index = index;
                Performance = new BlockingCollection<IndexingStatsAggregator>();
            }
        }
    }
   
}