// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionStatsAggregator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionStatsAggregator : StatsAggregator<SubscriptionConnectionRunStats, SubscriptionConnectionStatsScope>
    {
        private volatile SubscriptionConnectionPerformanceStats _connectionPerformanceStats;
        
        public SubscriptionConnectionStatsAggregator(int connectionId, SubscriptionConnectionStatsAggregator lastStats) : base(connectionId, lastStats)
        {
        }

        public override SubscriptionConnectionStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);
            return Scope = new SubscriptionConnectionStatsScope(Stats);
        }
        
        public SubscriptionConnectionPerformanceStats ToConnectionPerformanceStats()
        {
            if (_connectionPerformanceStats != null)
                return _connectionPerformanceStats;
        
            lock (Stats)
            {
                if (_connectionPerformanceStats != null)
                    return _connectionPerformanceStats;
        
                return _connectionPerformanceStats = CreateConnectionPerformanceStats(completed: true);
            }
        }
        
        private SubscriptionConnectionPerformanceStats CreateConnectionPerformanceStats(bool completed) 
        {
            return new SubscriptionConnectionPerformanceStats()
            {
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                
                ConnectionId = Id,
                BatchCount = Stats.BatchCount,

                ClientUri = Stats.ClientUri,
                Script = Stats.Script,
                Exception = Stats.Exception
            };
        }
        
        public SubscriptionConnectionPerformanceStats ToConnectionPerformanceLiveStatsWithDetails()
        {
            if (_connectionPerformanceStats != null)
                return _connectionPerformanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToConnectionPerformanceStats();

            return CreateConnectionPerformanceStats(completed: false);
        }
    }
}
