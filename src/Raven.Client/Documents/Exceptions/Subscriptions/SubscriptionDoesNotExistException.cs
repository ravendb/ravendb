// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;

namespace Raven.Client.Documents.Exceptions.Subscriptions
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
