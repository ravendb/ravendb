// -----------------------------------------------------------------------
//  <copyright file="FailureCounters.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions;
using Raven.NewClient.Abstractions.Util;

namespace Raven.NewClient.Client.Connection.Request
{
    public class FailureCounters
    {
        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };

        public readonly ConcurrentDictionary<string, FailureCounter> FailureCounts = new ConcurrentDictionary<string, FailureCounter>();

        /// <summary>
        /// Get the current failure count for the url
        /// </summary>
        public long GetFailureCount(string operationUrl)
        {
            return GetHolder(operationUrl).Value;
        }

        /// <summary>
        /// Get failure last check time for the url
        /// </summary>
        public DateTime GetFailureLastCheck(string operationUrl)
        {
            return GetHolder(operationUrl).LastCheck;
        }

        internal FailureCounter GetHolder(string operationUrl)
        {
            return FailureCounts.GetOrAdd(operationUrl, new FailureCounter());
        }

        /// <summary>
        /// Determines whether this is the first failure on the specified operation URL.
        /// </summary>
        /// <param name="operationUrl">The operation URL.</param>
        public bool IsFirstFailure(string operationUrl)
        {
            FailureCounter value = GetHolder(operationUrl);
            return value.Value == 0;
        }

        /// <summary>
        /// Increments the failure count for the specified operation URL
        /// </summary>
        /// <param name="operationUrl">The operation URL.</param>
        public void IncrementFailureCount(string operationUrl)
        {
            var value = GetHolder(operationUrl);

            if (value.Increment() == 1)// first failure
            {
                FailoverStatusChanged(this, new FailoverStatusChangedEventArgs
                {
                    Url = operationUrl,
                    Failing = true
                });
            }
        }

        /// <summary>
        /// Resets the failure count for the specified URL
        /// </summary>
        /// <param name="operationUrl">The operation URL.</param>
        public virtual void ResetFailureCount(string operationUrl)
        {
            var value = GetHolder(operationUrl);
            if (value.Reset() != 0)
            {
                FailoverStatusChanged(this,
                    new FailoverStatusChangedEventArgs
                    {
                        Url = operationUrl,
                        Failing = false
                    });
            }
        }

        public void ForceCheck(string primaryUrl, bool shouldForceCheck)
        {
            var failureCounter = GetHolder(primaryUrl);
            failureCounter.ForceCheck = shouldForceCheck;
        }
    }

    public class FailureCounter
    {
        public long Value;
        public DateTime LastCheck;
        public bool ForceCheck;

        public Task CheckDestination = new CompletedTask();

        public long Increment()
        {
            ForceCheck = false;
            LastCheck = SystemTime.UtcNow;
            return Interlocked.Increment(ref Value);
        }

        public long Reset()
        {
            var oldVal = Interlocked.Exchange(ref Value, 0);
            LastCheck = SystemTime.UtcNow;
            ForceCheck = false;
            return oldVal;
        }
    }
}
