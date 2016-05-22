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
        private static int connectionCounter;

        public SubscriptionConnectionOptions()
        {
            ConnectionId = Interlocked.Increment(ref connectionCounter) + "/" + Base62Util.Base62Random();
            _clientAliveNotificationInterval = TimeSpan.FromMinutes(2);
            ClientAliveNotificationInterval = _clientAliveNotificationInterval.Ticks;
            _timeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(15);
            TimeToWaitBeforeConnectionRetry = _timeToWaitBeforeConnectionRetry.Ticks;
            _pullingRequestTimeout = TimeSpan.FromMinutes(5);
            PullingRequestTimeout = _pullingRequestTimeout.Ticks;
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocCount = 4096;
            AcknowledgmentTimeout = TimeSpan.FromMinutes(1).Ticks;

        }

        public string ConnectionId { get; private set; }

        private TimeSpan _timeToWaitBeforeConnectionRetry;

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

        private TimeSpan _clientAliveNotificationInterval;
        [JsonIgnore]
        public TimeSpan ClientAliveNotificationIntervalTimespan
        {
            get
            {
                if (_clientAliveNotificationInterval.Ticks != ClientAliveNotificationInterval)
                    _clientAliveNotificationInterval = new TimeSpan(ClientAliveNotificationInterval);
                return _clientAliveNotificationInterval;
            }
        }

        public long PullingRequestTimeout { get; set; }

        private TimeSpan _pullingRequestTimeout;

        [JsonIgnore]
        public TimeSpan PullingRequestTimeoutTimespan
        {
            get
            {
                if (_pullingRequestTimeout.Ticks != PullingRequestTimeout)
                    _pullingRequestTimeout = new TimeSpan(PullingRequestTimeout);
                return _pullingRequestTimeout;
            }
        }

        public bool IgnoreSubscribersErrors { get; set; }

        public SubscriptionOpeningStrategy Strategy { get; set; }

        public int? MaxSize { get; set; }

        public int MaxDocCount { get; set; }

        public long AcknowledgmentTimeout { get; set; }
    }

    public class SubscriptionBatchOptions
    {
        public SubscriptionBatchOptions()
        {
            MaxDocCount = 4096;
            AcknowledgmentTimeout = TimeSpan.FromMinutes(1).Ticks;
        }

        public int? MaxSize { get; set; }

        public int MaxDocCount { get; set; }

        public long AcknowledgmentTimeout { get; set; }
    }
}