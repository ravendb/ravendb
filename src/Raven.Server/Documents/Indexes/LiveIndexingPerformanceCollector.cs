using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Sparrow.Collections;

namespace Raven.Server.Documents.Indexes
{
    public class LiveIndexingPerformanceCollector : IDisposable
    {
        private readonly DocumentsChanges _changes;
        private readonly IndexStore _indexStorage;
        private readonly ConcurrentDictionary<string, IndexAndPerformanceStatsList> _perIndexStats = 
            new ConcurrentDictionary<string, IndexAndPerformanceStatsList>();
        private readonly CancellationToken _resourceShutdown;
        private readonly CancellationTokenSource _cts;

        public LiveIndexingPerformanceCollector(DocumentDatabase documentDatabase, CancellationToken resourceShutdown, IEnumerable<Index> indexes)
        {
            _changes = documentDatabase.Changes;
            _indexStorage = documentDatabase.IndexStore;
            _resourceShutdown = resourceShutdown;
            foreach (var index in indexes)
            {
                _perIndexStats.TryAdd(index.Name, new IndexAndPerformanceStatsList(index));
            }
            _cts = new CancellationTokenSource();

            Task.Run(StartCollectingStats);
        }

        public void Dispose()
        {
            _changes.OnIndexChange -= OnIndexChange;
            _cts.Cancel();
            _cts.Dispose();
        }

        public AsyncQueue<List<IndexPerformanceStats>> Stats { get; } = new AsyncQueue<List<IndexPerformanceStats>>();

        private async Task StartCollectingStats()
        {
            _changes.OnIndexChange += OnIndexChange;

            var stats = _perIndexStats.Values
                    .Select(x => new IndexPerformanceStats
                    {
                        Name = x.Index.Name,
                        Etag = x.Index.Etag,
                        Performance = x.Index.GetIndexingPerformance()
                    })
                    .ToList();

            Stats.Enqueue(stats);

            using (var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(_resourceShutdown, _cts.Token))
            {
                var token = linkedToken.Token;

                while (token.IsCancellationRequested == false)
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), token);

                    if (token.IsCancellationRequested)
                        break;

                    var performanceStats = PreparePerformanceStats();

                    if (performanceStats.Count > 0)
                    {
                        Stats.Enqueue(performanceStats);
                    }
                }
            }
        }

        private List<IndexPerformanceStats> PreparePerformanceStats()
        {
            var preparedStats = new List<IndexPerformanceStats>(_perIndexStats.Count);

            foreach (var indexAndPerformanceStatsList in _perIndexStats.Values)
            {
                var index = indexAndPerformanceStatsList.Index;
                var performance = indexAndPerformanceStatsList.Performance;

                var itemsToSend = new List<IndexingStatsAggregator>(performance.Count);

                IndexingStatsAggregator stat;
                while (performance.TryTake(out stat))
                    itemsToSend.Add(stat);

                var latestStats = index.GetLatestIndexingStat();

                if (latestStats.Completed == false && itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    preparedStats.Add(new IndexPerformanceStats
                    {
                        Name = index.Name,
                        Etag = index.Etag,
                        Performance = itemsToSend.Select(item => item.ToIndexingPerformanceLiveStatsWithDetails()).ToArray()
                    });
                }
            }
            return preparedStats;
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
                var index = _indexStorage.GetIndex(change.Name);
                if (index == null)
                    return;

                indexAndPerformanceStats = new IndexAndPerformanceStatsList(index);
                _perIndexStats.TryAdd(change.Name, indexAndPerformanceStats);
            }

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