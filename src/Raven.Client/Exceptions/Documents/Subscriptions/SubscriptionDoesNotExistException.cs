// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionDoesNotExistException : SubscriptionException
    {
        public SubscriptionDoesNotExistException(string message) : base(message)
        {
        }

        public SubscriptionDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
