using System;
using System.Net;

namespace Raven.Client.Documents.Exceptions.Subscriptions
{
    public class SubscriptionInvalidStateException : SubscriptionException
    {
        public SubscriptionInvalidStateException(string message) : base(message)
        {
        }

        public SubscriptionInvalidStateException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}