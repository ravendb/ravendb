// -----------------------------------------------------------------------
//  <copyright file="SubscriptionTaskPerformanceStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Server.Documents.Subscriptions.Stats;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionTaskPerformanceStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }

        public SubscriptionConnectionPerformanceStats[] ConnectionPerformance { get; set; }
        public SubscriptionBatchPerformanceStats[] BatchPerformance { get; set; }
    }
}
