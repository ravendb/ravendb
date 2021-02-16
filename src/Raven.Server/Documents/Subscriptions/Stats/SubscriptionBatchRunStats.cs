// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchRunStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionBatchRunStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }
        
        public long ConnectionId { get; set; }
        public long BatchId { get; set; }
        
        public long NumberOfDocuments { get; set; }
        public long SizeOfDocuments { get; set; }
        
        public long NumberOfIncludedDocuments { get; set; }
        public long SizeOfIncludedDocuments { get; set; }
                
        public long NumberOfIncludedCounters { get; set; }
        public long NumberOfIncludedTimeSeriesEntries { get; set; }
        
        public DateTime? StartWaitingForClientAck  { get; set; }
        public DateTime? ClientAckTime { get; set; }
        
        public string Exception { get; set; }
    }
}
