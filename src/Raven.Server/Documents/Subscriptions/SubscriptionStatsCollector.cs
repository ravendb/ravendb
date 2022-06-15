using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;

namespace Raven.Server.Documents.Subscriptions;

public class SubscriptionStatsCollector : IDisposable
{
    public readonly SubscriptionConnectionMetrics Metrics;
    public readonly int ConnectionStatsIdForConnection;
    private static int _connectionStatsId;
    private static int _batchStatsId;

    private SubscriptionConnectionStatsScope _connectionScope;
    public SubscriptionConnectionStatsScope ConnectionScope => _connectionScope;

    private SubscriptionConnectionStatsScope _pendingConnectionScope;
    public SubscriptionConnectionStatsScope PendingConnectionScope => _pendingConnectionScope;

    private SubscriptionConnectionStatsScope _activeConnectionScope;
    public SubscriptionConnectionStatsScope ActiveConnectionScope => _activeConnectionScope;

    private SubscriptionConnectionStatsAggregator _lastConnectionStats; // inProgress connection data
    public SubscriptionConnectionStatsAggregator LastConnectionStats => _lastConnectionStats;

    private SubscriptionBatchStatsAggregator _lastBatchStats; // inProgress batch data
    public SubscriptionBatchStatsAggregator GetBatchPerformanceStats => _lastBatchStats;

    private readonly ConcurrentQueue<SubscriptionBatchStatsAggregator> _lastBatchesStats = new ConcurrentQueue<SubscriptionBatchStatsAggregator>(); // batches history
    public List<SubscriptionBatchStatsAggregator> GetBatchesPerformanceStats => _lastBatchesStats.ToList();

    public SubscriptionStatsCollector()
    {
        Metrics = new SubscriptionConnectionMetrics();
        ConnectionStatsIdForConnection = Interlocked.Increment(ref _connectionStatsId);
    }

    public void Initialize()
    {
        _lastConnectionStats = new SubscriptionConnectionStatsAggregator(ConnectionStatsIdForConnection, null);
        _connectionScope = _lastConnectionStats.CreateScope();
        _pendingConnectionScope = _pendingConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionPending);
    }

    public void CreateActiveConnectionScope() => _activeConnectionScope = ConnectionScope.For(SubscriptionOperationScope.ConnectionActive);

    public SubscriptionBatchStatsAggregator CreateInProgressBatchStats() => _lastBatchStats = new SubscriptionBatchStatsAggregator(Interlocked.Increment(ref _batchStatsId), _lastBatchStats);

    public SubscriptionBatchStatsAggregator UpdateBatchPerformanceStats(long batchSize, bool anyDocumentsSent = true)
    {
        _lastBatchStats.Complete();

        if (anyDocumentsSent)
        {
            _connectionScope.RecordBatchCompleted(batchSize);
            AddBatchPerformanceStatsToBatchesHistory(_lastBatchStats);
        }

        var last = _lastBatchStats;
        _lastBatchStats = null;
        return last;
    }

    private void AddBatchPerformanceStatsToBatchesHistory(SubscriptionBatchStatsAggregator batchStats)
    {
        _lastBatchesStats.Enqueue(batchStats); // add to batches history

        while (_lastBatchesStats.Count > 25)
            _lastBatchesStats.TryDequeue(out batchStats);
    }

    public void Dispose()
    {
        Metrics.Dispose();
        ActiveConnectionScope?.Dispose();
        ConnectionScope.Dispose();
    }
}
