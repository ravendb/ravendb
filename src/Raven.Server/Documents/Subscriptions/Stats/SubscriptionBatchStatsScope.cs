// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchStatsScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.Util;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionBatchStatsScope : StatsScope<SubscriptionBatchRunStats, SubscriptionBatchStatsScope>
    {
        private readonly SubscriptionBatchRunStats _stats;
        
        public SubscriptionBatchStatsScope(SubscriptionBatchRunStats stats, bool start = true) : base(stats, start)
        {
            _stats = stats;
        }

        protected override SubscriptionBatchStatsScope OpenNewScope(SubscriptionBatchRunStats stats, bool start)
        {
            return new SubscriptionBatchStatsScope(stats, start);
        }
        
        public void RecordBatchInfo(long taskId, string taskName, long connectionId, long batchId)
        {
            _stats.TaskId = taskId;
            _stats.TaskName = taskName;
            
            _stats.ConnectionId = connectionId;
            _stats.BatchId = batchId;
        }
        
        public void RecordDocumentsInfo(long documentsCount, long documentsSize)
        {
            _stats.NumberOfDocuments = documentsCount;
            _stats.SizeOfDocuments = documentsSize;
        }
        
        public void RecordStartWaitingForClientAck()
        {
            _stats.StartWaitingForClientAck = SystemTime.UtcNow;
        }
        
        public void RecordClientAckTime(DateTime clientAckTime)
        {
            _stats.ClientAckTime = clientAckTime;
        }
        
        public void RecordException(string exceptionMsg)
        {
            _stats.Exception = exceptionMsg;
        }
    }
}
