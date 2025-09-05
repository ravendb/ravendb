using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public sealed class SubscriptionDoesNotExistException : SubscriptionException
    {
        public SubscriptionDoesNotExistException(string message) : base(message)
        {
        }

        public SubscriptionDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}