// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionStatsScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

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

        public void RecordConnectionInfo(long taskId, string taskName, string clientUri, string script)
        {
            _stats.TaskId = taskId;
            _stats.TaskName = taskName;
            _stats.ClientUri = clientUri;
            _stats.Script = script;
        }
        
        public void RecordException(string exception)
        {
            _stats.Exception = exception;
        }

        public void RecordBatchCount()
        {
            _stats.BatchCount++;
        }
    }
}
