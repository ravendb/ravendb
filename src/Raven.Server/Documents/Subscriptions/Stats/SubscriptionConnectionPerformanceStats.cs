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
        public string Script { get; set; }

        public string Exception { get; set; }

        public DateTime Started { get; set; }
        public DateTime? Completed { get; set; }
    }
}
