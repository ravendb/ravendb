// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionStatsScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionStatsScope : StatsScope<SubscriptionConnectionRunStats, SubscriptionConnectionStatsScope>
    {
        private readonly SubscriptionConnectionRunStats _stats;

        public SubscriptionConnectionStatsScope(SubscriptionConnectionRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        protected override SubscriptionConnectionStatsScope OpenNewScope(SubscriptionConnectionRunStats stats, bool start)
        {
            return new SubscriptionConnectionStatsScope(stats, start);
        }

        public void RecordConnectionInfo(SubscriptionState subscriptionState, string clientUri, SubscriptionOpeningStrategy strategy)
        {
            _stats.TaskId = subscriptionState.SubscriptionId;
            _stats.TaskName = subscriptionState.SubscriptionName;
            _stats.ClientUri = clientUri;
            _stats.Strategy = strategy;
        }
        
        public void RecordException(SubscriptionError errorType, string exception)
        {
            _stats.ErrorType = errorType;
            _stats.Exception = exception;
        }

        public void RecordBatchCompleted(long batchSize)
        {
            _stats.BatchCount++;
            _stats.TotalBatchSizeInBytes += batchSize;
        }
        
        public SubscriptionConnectionPerformanceOperation ToPerformanceOperation(string name)
        {
            var operation = new SubscriptionConnectionPerformanceOperation(Duration)
            {
                Name = name
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => x.Value.ToPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }
    }
}
