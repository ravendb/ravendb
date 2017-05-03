// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    internal class SubscriptionConnectionClientMessage
    {
        internal enum MessageType
        {
            None,
            Acknowledge,
            DisposedNotification
        }

        public MessageType Type { get; set; }
        public ChangeVectorEntry[] ChangeVector { get; set; }
    }

    internal class SubscriptionConnectionServerMessage
    {
        internal enum MessageType
        {
            None,
            CoonectionStatus,
            EndOfBatch,
            Data,
            Confirm,
            Error
        }

        internal enum ConnectionStatus
        {
            None,
            Accepted,
            InUse,
            Closed,
            NotFound
        }

        public MessageType Type { get; set; }
        public ConnectionStatus Status { get; set; }
        public BlittableJsonReaderObject Data { get; set; }
        public string Exception { get; set; }
    }

    public class SubscriptionConnectionOptions
    {
        private SubscriptionConnectionOptions()
        {
            // for deserialization
        }

        public SubscriptionConnectionOptions(long subscriptionId)
        {
            if (subscriptionId <= 0)
                throw new ArgumentOutOfRangeException(nameof(subscriptionId));

            SubscriptionId = subscriptionId;
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
            TimeToWaitBeforeConnectionRetryMilliseconds = 5000;
        }

        public long SubscriptionId { get; private set; }
        public int TimeToWaitBeforeConnectionRetryMilliseconds { get; set; }
        public bool IgnoreSubscriberErrors { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }
        public int MaxDocsPerBatch { get; set; }
    }
}