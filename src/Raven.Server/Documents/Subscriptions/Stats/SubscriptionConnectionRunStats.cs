// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionRunStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;

namespace Raven.Server.Documents.Subscriptions.Stats
{
    public class SubscriptionConnectionRunStats
    {
        public long TaskId { get; set; }
        public string TaskName { get; set; }
        
        public string ClientUri { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }

        public string Exception { get; set; }
        public SubscriptionError ErrorType { get; set; }

        public long BatchCount { get; set; }
        public long TotalBatchSizeInBytes { get; set; }
    }
}
