using System;
using System.Collections.Generic;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionDoesNotBelongToNodeException : SubscriptionException
    {
        public string AppropriateNode;
        public Dictionary<string, string> Reasons = new Dictionary<string, string>();

        public SubscriptionDoesNotBelongToNodeException(string message) : base(message)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
