// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionPerformanceOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionPerformanceOperation
    {
        public SubscriptionConnectionPerformanceOperation(TimeSpan duration)
        {
            DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
            Operations = new SubscriptionConnectionPerformanceOperation[0];
        }

        public string Name { get; set; }

        public double DurationInMs { get; }

        public SubscriptionConnectionPerformanceOperation[] Operations { get; set; }
    }
}
