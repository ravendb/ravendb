// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchPerformanceOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionBatchPerformanceOperation
    {
        public SubscriptionBatchPerformanceOperation(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
            Operations = new SubscriptionBatchPerformanceOperation[0];
        }

        public string Name { get; set; }

        public double DurationInMs { get; }

        public SubscriptionBatchPerformanceOperation[] Operations { get; set; }
    }
}
