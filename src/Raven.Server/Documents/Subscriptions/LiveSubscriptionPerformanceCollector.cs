// -----------------------------------------------------------------------
//  <copyright file="LiveSubscriptionPerformanceCollector.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
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
    public class LiveSubscriptionPerformanceCollector : DatabaseAwareLivePerformanceCollector<SubscriptionTaskPerformanceStats>
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

        protected IEnumerable<SubscriptionTaskPerformanceStats> PrepareInitialPerformanceStats()
        {
            var results = new List<SubscriptionTaskPerformanceStats>();
            
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                    var connectionState = Database.SubscriptionStorage.GetSubscriptionConnection(context, kvp.Value.Handler.SubscriptionName);
                    if (connectionState != null)
                    {
                        var currentConnection = connectionState.Connection;
                        var connectionAggregators = new List<SubscriptionConnectionStatsAggregator>();

                        if (currentConnection != null)
                        {
                            // add inProgress aggregator
                            connectionAggregators.Add(currentConnection.GetPerformanceStats());
                        }

                        // add history aggregators
                        connectionAggregators.AddRange(connectionState.RecentConnections.Select(x => x.GetPerformanceStats()));
                        connectionAggregators.AddRange(connectionState.RecentRejectedConnections.Select(x => x.GetPerformanceStats()));

                        // add connection stats to results 
                        var subscriptionItem = results.Find(x => x.TaskId == kvp.Value.Handler.Connection?.SubscriptionId);
                        if (subscriptionItem != null)
                        {
                            var connectionPerformance = connectionAggregators.Select(x => x.ToConnectionPerformanceLiveStatsWithDetails());
                            subscriptionItem.ConnectionPerformance = connectionPerformance.ToArray();
                        }
                    }
                }

                foreach (var kvp in _perSubscriptionBatchStats)
                {
                    var connectionState = Database.SubscriptionStorage.GetSubscriptionConnection(context, kvp.Value.Handler.SubscriptionName);
                    if (connectionState != null)
                    {
                        var currentConnection = connectionState.Connection;
                        var batchesAggregators = new List<SubscriptionBatchStatsAggregator>();

                        if (currentConnection != null)
                        {
                           // add batches history for inProgress connection
                           batchesAggregators.AddRange(currentConnection.GetBatchesPerformanceStats());
                        }
                       
                        // add batches history for previous connections
                        foreach (var recentConnection in connectionState.RecentConnections)
                        {
                            batchesAggregators.AddRange(recentConnection.GetBatchesPerformanceStats());
                        }
                        foreach (var recentRejectedConnection in connectionState.RecentRejectedConnections)
                        {
                            batchesAggregators.AddRange(recentRejectedConnection.GetBatchesPerformanceStats());
                        }
                       
                        // add batch stats to results
                        var subscriptionItem = results.Find(x => x.TaskId == kvp.Value.Handler.Connection?.SubscriptionId);
                        if (subscriptionItem != null)
                        {
                            var batchPerformance = batchesAggregators.Select(x => x.ToBatchPerformanceLiveStatsWithDetails());
                            subscriptionItem.BatchPerformance = batchesAggregators.Select(x => x.ToBatchPerformanceLiveStatsWithDetails()).ToArray();
                        }
                    }
                }
            }
            
            return results;
        }

        protected override async Task StartCollectingStats()
        {
            Database.SubscriptionStorage.OnEndConnectionEvent += OnEndConnection;
            Database.SubscriptionStorage.OnEndBatchEvent += OnEndBatch;
            Database.SubscriptionStorage.OnTaskAddedEvent += OnSubscriptionTaskAdded;
            Database.SubscriptionStorage.OnTaskRemovedEvent += OnSubscriptionTaskRemoved;

            try
            {
                await RunInLoop();
            }
            finally
            {
                Database.SubscriptionStorage.OnEndConnectionEvent -= OnEndConnection;
                Database.SubscriptionStorage.OnEndBatchEvent -= OnEndBatch;
                Database.SubscriptionStorage.OnTaskAddedEvent -= OnSubscriptionTaskAdded;
                Database.SubscriptionStorage.OnTaskRemovedEvent -= OnSubscriptionTaskRemoved;
            }
        }
        
        // Get periodic info for ws
        protected override List<SubscriptionTaskPerformanceStats> PreparePerformanceStats()
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

                // 1. get 'closed' connections info
                while (performance.TryTake(out SubscriptionConnectionStatsAggregator stat))
                {
                    connectionsAggregators.Add(stat);
                }
                
                // 2. get 'inProgress' connection info
                using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var inProgressConnection = Database.SubscriptionStorage.GetSubscriptionConnection(context, subscriptionName);
                    var inProgressStats = inProgressConnection?.Connection?.GetPerformanceStats();
                    
                    if (inProgressStats != null &&
                        inProgressStats.Completed == false &&
                        connectionsAggregators.Contains(inProgressStats) == false)
                    {
                       connectionsAggregators.Add(inProgressStats);
                    }
                }
                
                // 3. add to results
                connectionPerformance.AddRange(connectionsAggregators.Select(x => x.ToConnectionPerformanceLiveStatsWithDetails()));
                preparedStats.Add(new SubscriptionTaskPerformanceStats
                {
                    TaskName = subscriptionName,
                    TaskId = subscriptionId,
                    ConnectionPerformance = connectionPerformance.ToArray()
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
                using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var inProgressConnection = Database.SubscriptionStorage.GetSubscriptionConnection(context, subscriptionName);
                    var inProgressBatchStats = inProgressConnection?.Connection?.GetBatchPerformanceStats();
                    
                    if (inProgressBatchStats != null &&
                        inProgressBatchStats.Completed == false &&
                        inProgressBatchStats.ToBatchPerformanceLiveStatsWithDetails().DocumentsCount > 0 &&
                        batchAggregators.Contains(inProgressBatchStats) == false)
                    {
                        batchAggregators.Add(inProgressBatchStats);
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
        
        private class SubscriptionAndPerformanceConnectionStatsList
            : HandlerAndPerformanceStatsList<SubscriptionStorage.SubscriptionGeneralDataAndStats, SubscriptionConnectionStatsAggregator>
        {
            public SubscriptionAndPerformanceConnectionStatsList(SubscriptionStorage.SubscriptionGeneralDataAndStats subscription) : base(subscription)
            {
            }
        }
        
        private class SubscriptionAndPerformanceBatchStatsList
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
                var aggregatorToAdd = connection.GetPerformanceStats();
                subscriptionAndStats.Performance.TryAdd(aggregatorToAdd);
            }
        }
        
        private void OnSubscriptionTaskAdded(string subscriptionName)
        {
            if (_perSubscriptionConnectionStats.ContainsKey(subscriptionName))
                return;

            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = Database.SubscriptionStorage.GetSubscription(context, null, subscriptionName, true);
                _perSubscriptionConnectionStats.TryAdd(subscriptionName, new SubscriptionAndPerformanceConnectionStatsList(subscription));
                
                if (_perSubscriptionBatchStats.ContainsKey(subscriptionName))
                    return;

                _perSubscriptionBatchStats.TryAdd(subscriptionName, new SubscriptionAndPerformanceBatchStatsList(subscription));
            }
        }
        
        private void OnSubscriptionTaskRemoved(string subscriptionName)
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
