// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionRunStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionRunStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }
        
        public string ClientUri { get; set; }
        public DateTime ConnectedAt { get; set; }

        public string Exception { get; set; }
        public long BatchCount { get; set; }
    }
}
