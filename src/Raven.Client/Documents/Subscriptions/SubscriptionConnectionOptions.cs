// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Sparrow.Json;

namespace Raven.Client.Documents.Subscriptions
{
    internal class SubscriptionConnectionClientMessage
    {
        internal enum MessageType
        {
            None,
            Acknowledge
        }

        public MessageType Type { get; set; }
        public long Etag { get; set; }
    }

    internal class SubscriptionConnectionServerMessage : IDisposable
    {
        internal BlittableJsonReaderObject ParentObjectToDispose;

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

        public void Dispose()
        {
            ParentObjectToDispose?.Dispose();
            ParentObjectToDispose = null;
        }
    }

    public class SubscriptionConnectionOptions
    {
        public SubscriptionConnectionOptions(long subscriptionId)
        {
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
            SubscriptionId = subscriptionId;
        }

        public readonly long SubscriptionId;
        public int TimeToWaitBeforeConnectionRetryMilliseconds { get; set; } = 5000;
        public bool IgnoreSubscribersErrors { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }
        public int MaxDocsPerBatch { get; set; }
    }
}