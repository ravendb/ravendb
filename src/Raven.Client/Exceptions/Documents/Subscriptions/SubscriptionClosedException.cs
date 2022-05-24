// -----------------------------------------------------------------------
//  <copyright file="SubscriptionClosedException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionClosedException : SubscriptionException
    {
        internal bool CanReconnect { get; set; }

        public SubscriptionClosedException(string message) : base(message)
        {
        }

        public SubscriptionClosedException(string message, bool canReconnect) : base(message)
        {
            CanReconnect = canReconnect;
        }

        public SubscriptionClosedException(string message, Exception inner) : base(message, inner)
        {
        }
        public SubscriptionClosedException(string message, bool canReconnect, Exception inner) : base(message, inner)
        {
            CanReconnect = canReconnect;
        }
    }
}
