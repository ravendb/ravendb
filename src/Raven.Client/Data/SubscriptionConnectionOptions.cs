// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionOptions
    {
        private static int _connectionCounter;
        private string _connectionId;
        public long SubscriptionId { get; set; }

        public SubscriptionConnectionOptions()
        {
            _timeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(15);
            TimeToWaitBeforeConnectionRetry = _timeToWaitBeforeConnectionRetry.Ticks;
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
        }

        public string ConnectionId
        {
            get
            {
                if (_connectionId == null)
                {
                    _connectionId = Interlocked.Increment(ref _connectionCounter) + "/" + Base62Util.Base62Random();
                }
                return _connectionId;
            }
            set { _connectionId = value; }
        }

        private TimeSpan _timeToWaitBeforeConnectionRetry;

        [JsonIgnore] public CancellationTokenSource CancellationTokenSource;
        [JsonIgnore] public IDisposable DisposeOnDisconnect;

        [JsonIgnore]
        public TimeSpan TimeToWaitBeforeConnectionRetryTimespan
        {
            get
            {
                if (_timeToWaitBeforeConnectionRetry.Ticks != TimeToWaitBeforeConnectionRetry)
                    _timeToWaitBeforeConnectionRetry = new TimeSpan(TimeToWaitBeforeConnectionRetry);
                return _timeToWaitBeforeConnectionRetry;
            }
        }

        public long TimeToWaitBeforeConnectionRetry { get; set; }

        public long ClientAliveNotificationInterval { get; set; }

        public bool IgnoreSubscribersErrors { get; set; }

        public SubscriptionOpeningStrategy Strategy { get; set; }

        public int? MaxBatchSize { get; set; }

        public int MaxDocsPerBatch { get; set; }
    }        
}