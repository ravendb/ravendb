// -----------------------------------------------------------------------
//  <copyright file="LiveSubscriptionPerformanceCollector.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Subscriptions
{
    public sealed class LiveSubscriptionPerformanceCollector : DatabaseAwareLivePerformanceCollector<SubscriptionTaskPerformanceStats>
    {
        private readonly ConcurrentDictionary<string, SubscriptionAndPerformanceConnectionStatsList> _perSubscriptionConnectionStats
            = new ConcurrentDictionary<string, SubscriptionAndPerformanceConnectionStatsList>();
        
        private readonly ConcurrentDictionary<string, SubscriptionAndPerformanceBatchStatsList> _perSubscriptionBatchStats
            = new ConcurrentDictionary<string, SubscriptionAndPerformanceBatchStatsList>();
        
        public LiveSubscriptionPerformanceCollector(DocumentDatabase database) : base(database)
        {
            var initialStats = PrepareInitialPerformanceStats().ToList();
            if (initialStats.Count > 0)
            {
                Stats.Enqueue(initialStats);
            }
            
            Start();
        }

        private IEnumerable<SubscriptionTaskPerformanceStats> PrepareInitialPerformanceStats()
        {
            var results = new List<SubscriptionTaskPerformanceStats>();
            
            using (Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptions = Database.SubscriptionStorage.GetAllSubscriptions(context, false, 0, int.MaxValue);
                foreach (var subscription in subscriptions)
                {
                    var subscriptionName = subscription.SubscriptionName;
                    var subscriptionId = subscription.SubscriptionId;
                    
                    results.Add(new SubscriptionTaskPerformanceStats
                    {
                        TaskName = subscriptionName,
                        TaskId = subscriptionId
                    });
                    
                    _perSubscriptionConnectionStats[subscriptionName] = new SubscriptionAndPerformanceConnectionStatsList(subscription);
                    _perSubscriptionBatchStats[subscriptionName] = new SubscriptionAndPerformanceBatchStatsList(subscription);
                }

                foreach (var kvp in _perSubscriptionConnectionStats)
                {
                    var subscriptionConnections = Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, kvp.Value.Handler.SubscriptionName);
                    if (subscriptionConnections != null)
                    {
                        var connectionAggregators =
                            new List<SubscriptionConnectionStatsAggregator>();

                        // add history aggregators
                        connectionAggregators.AddRange(subscriptionConnections.RecentConnections.Select(x => x.LastConnectionStats));
                        connectionAggregators.AddRange(subscriptionConnections.RecentRejectedConnections.Select(x => x.LastConnectionStats));
                        connectionAggregators.AddRange(subscriptionConnections.PendingConnections.Select(x => x.LastConnectionStats));

                        foreach (var currentConnection in subscriptionConnections.GetConnections())
                        {
                            // add inProgress aggregator
                            connectionAggregators.Add(currentConnection.Stats.LastConnectionStats);
                            
                            // add connection stats to results 
                            var subscriptionItem = results.Find(x => x.TaskId == kvp.Value.Handler.SubscriptionId);
                            if (subscriptionItem != null)
                            {
                                var connectionPerformance = connectionAggregators.Select(x => x.ToConnectionPerformanceLiveStatsWithDetails());
                                subscriptionItem.ConnectionPerformance = connectionPerformance.ToArray();
                            }
                        }
                    }
                }

                foreach (var kvp in _perSubscriptionBatchStats)
                {
                    var subscriptionConnections = Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, kvp.Value.Handler.SubscriptionName);
                    if (subscriptionConnections != null)
                    {
                        var batchesAggregators = new List<SubscriptionBatchStatsAggregator>();
                        foreach (var currentConnection in subscriptionConnections.GetConnections())
                        {
                            // add batches history for inProgress connection
                            batchesAggregators.AddRange(currentConnection.Stats.GetBatchesPerformanceStats);
                        }

                        // add batches history for previous connections
                        foreach (var recentConnection in subscriptionConnections.RecentConnections)
                        {
                            batchesAggregators.AddRange(recentConnection.LastBatchesStats);
                        }
                        foreach (var recentRejectedConnection in subscriptionConnections.RecentRejectedConnections)
                        {
                            batchesAggregators.AddRange(recentRejectedConnection.LastBatchesStats);
                        }
                        // add batch stats to results
                        var subscriptionItem = results.Find(x => x.TaskId == kvp.Value.Handler.SubscriptionId);
                        if (subscriptionItem != null)
                        {
                            subscriptionItem.BatchPerformance = batchesAggregators.Select(x => x.ToBatchPerformanceLiveStatsWithDetails()).ToArray();
                        }
                    }
                }
            }
            
            return results;
        }

        protected override async Task StartCollectingStats()
        {
            Database.SubscriptionStorage.OnEndConnection += OnEndConnection;
            Database.SubscriptionStorage.OnEndBatch += OnEndBatch;
            Database.SubscriptionStorage.OnAddTask += OnAddSubscriptionTask;
            Database.SubscriptionStorage.OnRemoveTask += OnRemoveSubscriptionTask;

            try
            {
                await RunInLoop();
            }
            finally
            {
                Database.SubscriptionStorage.OnEndConnection -= OnEndConnection;
                Database.SubscriptionStorage.OnEndBatch -= OnEndBatch;
                Database.SubscriptionStorage.OnAddTask -= OnAddSubscriptionTask;
                Database.SubscriptionStorage.OnRemoveTask -= OnRemoveSubscriptionTask;
            }
        }
        
        // Get periodic info for ws
        protected override List<SubscriptionTaskPerformanceStats> PreparePerformanceStats()
        {
            using (Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var preparedStats = new List<SubscriptionTaskPerformanceStats>();

                foreach (var kvp in _perSubscriptionConnectionStats)
                {
                    var connectionPerformance = new List<SubscriptionConnectionPerformanceStats>();

                    var subscriptionAndPerformanceConnectionStatsList = kvp.Value;
                    var subscriptionName = subscriptionAndPerformanceConnectionStatsList.Handler.SubscriptionName;
                    var subscriptionId = subscriptionAndPerformanceConnectionStatsList.Handler.SubscriptionId;
                    var performance = subscriptionAndPerformanceConnectionStatsList.Performance;

                    var connectionsAggregators = new List<SubscriptionConnectionStatsAggregator>(performance.Count);

                    while (performance.TryTake(out SubscriptionConnectionStatsAggregator stat))
                    {
                        connectionsAggregators.Add(stat);
                    }

                    using (context.OpenReadTransaction())
                    {
                        // check for 'in progress' connection info
                        var subscriptionConnections = Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionName);
                        if (subscriptionConnections != null)
                        {
                            foreach (var connection in subscriptionConnections.GetConnections())
                            {
                                var inProgressStats = connection.Stats.LastConnectionStats;

                                if (inProgressStats?.Completed == false &&
                                    connectionsAggregators.Contains(inProgressStats) == false)
                                {
                                    connectionsAggregators.Add(inProgressStats);
                                }
                            }
                            
                            // ... and for any pending connections (waiting for free, etc)
                            foreach (SubscriptionConnectionInfo pendingConnection in subscriptionConnections.PendingConnections)
                            {
                                var pendingConnectionStats = pendingConnection.LastConnectionStats;
                                if (connectionsAggregators.Contains(pendingConnectionStats) == false)
                                {
                                    connectionsAggregators.Add(pendingConnectionStats);
                                }
                            }
                        }
                    }

                    connectionPerformance.AddRange(connectionsAggregators.Select(x => x.ToConnectionPerformanceLiveStatsWithDetails()));
                    preparedStats.Add(new SubscriptionTaskPerformanceStats
                    {
                        TaskName = subscriptionName, TaskId = subscriptionId, ConnectionPerformance = connectionPerformance.ToArray()
                    });
                }

                foreach (var kvp in _perSubscriptionBatchStats)
                {
                    var batchPerformance = new List<SubscriptionBatchPerformanceStats>();

                    var subscriptionAndPerformanceBatchStatsList = kvp.Value;
                    var subscriptionName = subscriptionAndPerformanceBatchStatsList.Handler.SubscriptionName;
                    var performance = subscriptionAndPerformanceBatchStatsList.Performance;

                    var batchAggregators = new List<SubscriptionBatchStatsAggregator>(performance.Count);

                    // 1. get 'ended' batches info
                    while (performance.TryTake(out SubscriptionBatchStatsAggregator stat))
                    {
                        batchAggregators.Add(stat);
                    }

                    // 2. get 'inProgress' batch info
                    using (context.OpenReadTransaction())
                    {
                        var inProgressConnections = Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionName);
                        if (inProgressConnections != null)
                        {
                        foreach (var connection in inProgressConnections.GetConnections())
                        {
                            var inProgressBatchStats = connection.Stats.GetBatchPerformanceStats;

                            if (inProgressBatchStats?.Completed == false &&
                                inProgressBatchStats.ToBatchPerformanceLiveStatsWithDetails().NumberOfDocuments > 0 &&
                                batchAggregators.Contains(inProgressBatchStats) == false)
                            {
                                batchAggregators.Add(inProgressBatchStats);
                            }
                        }
                    }
                    }

                    // 3. add to results
                    batchPerformance.AddRange(batchAggregators.Select(x => x.ToBatchPerformanceLiveStatsWithDetails()));

                    var subscriptionItem = preparedStats.Find(x => x.TaskName == kvp.Key);
                    if (subscriptionItem != null)
                    {
                        subscriptionItem.BatchPerformance = batchPerformance.ToArray();
                    }
                }

                return preparedStats;
            }
        }
        
        private sealed class SubscriptionAndPerformanceConnectionStatsList
            : HandlerAndPerformanceStatsList<SubscriptionStorage.SubscriptionGeneralDataAndStats, SubscriptionConnectionStatsAggregator>
        {
            public SubscriptionAndPerformanceConnectionStatsList(SubscriptionStorage.SubscriptionGeneralDataAndStats subscription) : base(subscription)
            {
            }
        }
        
        private sealed class SubscriptionAndPerformanceBatchStatsList
            : HandlerAndPerformanceStatsList<SubscriptionStorage.SubscriptionGeneralDataAndStats, SubscriptionBatchStatsAggregator>
        {
            public SubscriptionAndPerformanceBatchStatsList(SubscriptionStorage.SubscriptionGeneralDataAndStats subscription) : base(subscription)
            {
            }
        }
        
        private void OnEndConnection(SubscriptionConnection connection)
        {
            var subscriptionName = connection.SubscriptionState.SubscriptionName;

            if (_perSubscriptionConnectionStats.TryGetValue(subscriptionName, out var subscriptionAndStats))
            {
                var aggregatorToAdd = connection.Stats.LastConnectionStats;
                subscriptionAndStats.Performance.TryAdd(aggregatorToAdd);
            }
        }
        
        private void OnAddSubscriptionTask(string subscriptionName)
        {
            if (_perSubscriptionConnectionStats.ContainsKey(subscriptionName))
                return;

            using (Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = Database.SubscriptionStorage.GetSubscriptionWithDataByNameFromServerStore(context, subscriptionName, history: false, running: false);
                _perSubscriptionConnectionStats.TryAdd(subscriptionName, new SubscriptionAndPerformanceConnectionStatsList(subscription));
                
                if (_perSubscriptionBatchStats.ContainsKey(subscriptionName))
                    return;

                _perSubscriptionBatchStats.TryAdd(subscriptionName, new SubscriptionAndPerformanceBatchStatsList(subscription));
            }
        }
        
        private void OnRemoveSubscriptionTask(string subscriptionName)
        {
            _perSubscriptionConnectionStats.Remove(subscriptionName, out _);
            _perSubscriptionBatchStats.Remove(subscriptionName, out _);
        }
        
        private void OnEndBatch(string subscriptionName, SubscriptionBatchStatsAggregator batchAggregator)
        {
            if (_perSubscriptionBatchStats.TryGetValue(subscriptionName, out var subscriptionAndStats))
            {
                subscriptionAndStats.Performance.TryAdd(batchAggregator);
            }
        }

        protected override void WriteStats(List<SubscriptionTaskPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteSubscriptionTaskPerformanceStats(context, stats);
        }
    }
}
