// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
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
        public string ChangeVector { get; set; }
    }

    internal class SubscriptionConnectionServerMessage
    {
        internal enum MessageType
        {
            None,
            ConnectionStatus,
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
            NotFound,
            Redirect,
            ForbiddenReadOnly,
            Forbidden,
            Invalid
        }

        internal class SubscriptionRedirectData
        {
            public string CurrentTag;
            public string RedirectedTag;
        }

        public MessageType Type { get; set; }
        public ConnectionStatus Status { get; set; }
        public BlittableJsonReaderObject Data { get; set; }
        public string Exception { get; set; }
        public string Message { get; set; }
    }

    public class SubscriptionConnectionOptions
    {
        private SubscriptionConnectionOptions()
        {
            // for deserialization
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
            TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(5000);
        }

        public SubscriptionConnectionOptions(string subscriptionName) : this()
        {
            if (string.IsNullOrEmpty(subscriptionName)) 
                throw new ArgumentException("Value cannot be null or empty.", nameof(subscriptionName));
            
            SubscriptionName = subscriptionName;
        }

        public string SubscriptionName { get; set; }
        public TimeSpan TimeToWaitBeforeConnectionRetry { get; set; }
        public bool IgnoreSubscriberErrors { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }
        public int MaxDocsPerBatch { get; set; }
    }
}