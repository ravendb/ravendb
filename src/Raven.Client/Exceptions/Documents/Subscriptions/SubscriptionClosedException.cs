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
        public SubscriptionClosedException(string message) : base(message)
        {
        }

        public SubscriptionClosedException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
