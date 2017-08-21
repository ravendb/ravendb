// -----------------------------------------------------------------------
//  <copyright file="SubscriptionAlreadyInUseException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionInUseException : SubscriptionException
    {
        public SubscriptionInUseException(string message) : base(message)
        {
        }

        public SubscriptionInUseException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
