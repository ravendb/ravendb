// -----------------------------------------------------------------------
//  <copyright file="SubscriptionTaskPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Documents.Subscriptions.Stats;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionTaskPerformanceStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }

        public SubscriptionConnectionPerformanceStats[] ConnectionPerformance { get; set; }
        public SubscriptionBatchPerformanceStats[] BatchPerformance { get; set; }

        public SubscriptionTaskPerformanceStats()
        {
            ConnectionPerformance = Array.Empty<SubscriptionConnectionPerformanceStats>();
            BatchPerformance = Array.Empty<SubscriptionBatchPerformanceStats>();
        }
    }
}
