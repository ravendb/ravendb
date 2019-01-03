using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionDoesNotBelongToNodeException : SubscriptionException
    {
        public string AppropriateNode;

        public SubscriptionDoesNotBelongToNodeException(string message) : base(message)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
