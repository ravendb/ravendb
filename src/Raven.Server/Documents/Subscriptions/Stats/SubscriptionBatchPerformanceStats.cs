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
        
        public long NumberOfIncludedDocuments { get; set; }
        public long SizeOfIncludedDocuments { get; set; }
        
        public long NumberOfIncludedCounters { get; set; }
        public long NumberOfIncludedTimeSeriesEntries { get; set; }
        
        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
        
        public DateTime? StartWaitingForClientAck { get; set; }
        public DateTime? ClientAckTime { get; set; }
        
        public string Exception { get; set; }
    }
}
