using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionMessageTypeException : SubscriptionException
    {
        public SubscriptionMessageTypeException(string message) : base(message)
        {
        }

        public SubscriptionMessageTypeException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
