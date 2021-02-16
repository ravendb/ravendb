// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionBatchPerformanceStats
    {
        public long BatchId { get; set; }
        public long ConnectionId { get; set; }
        
        public long DocumentsCount { get; set; }
        public long DocumentsSize { get; set; }
        
        public long IncludedDocumentsCount { get; set; }
        public long IncludedDocumentsSize { get; set; }
        
        public long IncludedCountersCount { get; set; }
        public long IncludedTimeSeriesEntriesCount { get; set; }
        
        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
        
        public DateTime? StartWaitingForClientAck { get; set; }
        public DateTime? ClientAckTime { get; set; }
        
        public string Exception { get; set; }
    }
}
