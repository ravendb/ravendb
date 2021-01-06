using System;
using System.Collections.Generic;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionDoesNotBelongToNodeException : SubscriptionException
    {
        public string AppropriateNode;
        internal long Index;
        public readonly Dictionary<string, string> Reasons = new Dictionary<string, string>();

        public SubscriptionDoesNotBelongToNodeException(string message) : base(message)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message, Exception inner) : base(message, inner)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message, Exception inner, string appropriateNode, Dictionary<string, string> reasons) : base(message, inner)
        {
            AppropriateNode = appropriateNode;
            Reasons = reasons;            
        }

        internal SubscriptionDoesNotBelongToNodeException(string message, string appropriateNode, Dictionary<string, string> reasons, long index) : base(message)
        {
            AppropriateNode = appropriateNode;
            Reasons = reasons;
            Index = index;
        }
    }
}
