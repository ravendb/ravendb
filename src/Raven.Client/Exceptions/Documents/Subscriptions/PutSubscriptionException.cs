using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class PutSubscriptionException : SubscriptionException
    {
        public PutSubscriptionException(string message) : base(message)
        {
        }

        public PutSubscriptionException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
