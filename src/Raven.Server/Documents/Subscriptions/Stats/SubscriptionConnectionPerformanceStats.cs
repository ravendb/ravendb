// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionPerformanceStats
    {        
        public long ConnectionId { get; set; }
        public long BatchCount { get; set; }

        public string ClientUri { get; set; }

        public string Exception { get; set; }

        public DateTime Started { get; set; } // started is for when pending starts
        public DateTime? Completed { get; set; }
        
        public DateTime? ConnectedAt { get; set; } // actual connection started, pending has ended
        
        public double DurationInMs { get; } // this duration includes the pending time 
        
        public SubscriptionConnectionPerformanceOperation Details { get; set; }
        
        public SubscriptionConnectionPerformanceStats(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        }
    }
}
