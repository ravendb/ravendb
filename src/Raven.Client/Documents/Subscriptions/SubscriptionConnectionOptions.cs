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
            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5);
            MaxErrorousPeriod = TimeSpan.FromMinutes(5);
        }

        public SubscriptionConnectionOptions(string subscriptionName) : this()
        {
            if (string.IsNullOrEmpty(subscriptionName)) 
                throw new ArgumentException("Value cannot be null or empty.", nameof(subscriptionName));
            
            SubscriptionName = subscriptionName;
        }

        /// <summary>
        /// Subscription name as received from CreateSubscription
        /// </summary>
        public string SubscriptionName { get; set; }

        /// <summary>
        /// Cooldown time between connection retry. Default: 5 seconds
        /// </summary>
        public TimeSpan TimeToWaitBeforeConnectionRetry { get; set; }

        /// <summary>
        /// Whether subscriber error should halt the subscription processing or not. Default: false
        /// </summary>
        public bool IgnoreSubscriberErrors { get; set; }

        /// <summary>
        /// How connection attempt handle existing\incoming connection. Default: OpenIfFree
        /// </summary>
        public SubscriptionOpeningStrategy Strategy { get; set; }

        /// <summary>
        /// Max amount that the server will try to retriev and send to client. Default: 4096
        /// </summary>
        public int MaxDocsPerBatch { get; set; }
        
        /// <summary>
        /// Maximum amount of time during which a subscription connection may be in errorrous state. Default: 5 minutes
        /// </summary>
        public TimeSpan MaxErrorousPeriod { get; set; }
    }
}
