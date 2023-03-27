using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionCreationException : SubscriptionException
    {
        public SubscriptionCreationException(string message) : base(message)
        {
        }

        public SubscriptionCreationException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
