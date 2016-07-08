// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Util;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionOptions
    {
        public long SubscriptionId { get; set; }

        public SubscriptionConnectionOptions()
        {
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
        }

        [JsonIgnore] public CancellationTokenSource CancellationTokenSource;
        [JsonIgnore] public IDisposable DisposeOnDisconnect;

        public int TimeToWaitBeforeConnectionRetryMilliseconds { get; set; } = 5000;

        public bool IgnoreSubscribersErrors { get; set; }

        public SubscriptionOpeningStrategy Strategy { get; set; }

        public int? MaxBatchSize { get; set; }

        public int MaxDocsPerBatch { get; set; }
    }        
}