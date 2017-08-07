using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriberErrorException: SubscriptionException
    {
        public SubscriberErrorException(string message) : base(message)
        {
        }

        public SubscriberErrorException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
