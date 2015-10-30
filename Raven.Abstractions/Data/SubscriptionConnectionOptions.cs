// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Util;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionOptions
    {
        private static int connectionCounter;

        public SubscriptionConnectionOptions()
        {
            ConnectionId = Interlocked.Increment(ref connectionCounter) + "/" + Base62Util.Base62Random();
            BatchOptions = new SubscriptionBatchOptions();
            ClientAliveNotificationInterval = TimeSpan.FromMinutes(2);
            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(15);
            PullingRequestTimeout = TimeSpan.FromMinutes(5);
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
        }

        public string ConnectionId { get; private set; }

        public SubscriptionBatchOptions BatchOptions { get; set; }

        public TimeSpan TimeToWaitBeforeConnectionRetry { get; set; }

        public TimeSpan ClientAliveNotificationInterval { get; set; }

        public TimeSpan PullingRequestTimeout { get; set; }

        public bool IgnoreSubscribersErrors { get; set; }

        public SubscriptionOpeningStrategy Strategy { get; set; }
    }

    public class SubscriptionBatchOptions
    {
        public SubscriptionBatchOptions()
        {
            MaxDocCount = 4096;
            AcknowledgmentTimeout = TimeSpan.FromMinutes(1);
        }

        public int? MaxSize { get; set; }

        public int MaxDocCount { get; set; }

        public TimeSpan AcknowledgmentTimeout { get; set; }
    }
}
