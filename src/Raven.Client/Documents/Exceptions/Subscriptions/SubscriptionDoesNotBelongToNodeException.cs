using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Raven.Client.Documents.Exceptions.Subscriptions
{
    public class SubscriptionDoesNotBelongToNodeException:SubscriptionException
    {
        public string AppropriateNode;
        public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.Redirect;

        public SubscriptionDoesNotBelongToNodeException() : base(RelevantHttpStatusCode)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message) : base(message, RelevantHttpStatusCode)
        {
        }

        public SubscriptionDoesNotBelongToNodeException(string message, Exception inner) : base(message, inner, RelevantHttpStatusCode)
        {
        }
    }
}
