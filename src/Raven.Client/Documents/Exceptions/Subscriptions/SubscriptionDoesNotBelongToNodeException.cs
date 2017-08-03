using System;
using System.Net;

namespace Raven.Client.Documents.Exceptions.Subscriptions
{
    public class SubscriptionDoesNotBelongToNodeException:SubscriptionException
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
