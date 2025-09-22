using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public sealed class SubscriptionInUseException : SubscriptionException
    {
        public SubscriptionInUseException(string message) : base(message)
        {
        }

        public SubscriptionInUseException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}