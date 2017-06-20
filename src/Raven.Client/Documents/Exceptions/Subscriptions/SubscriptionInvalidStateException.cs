using System;
using System.Net;

namespace Raven.Client.Documents.Exceptions.Subscriptions
{
    public class SubscriptionInvalidStateException : SubscriptionException
    {
        public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.BadRequest;

        public SubscriptionInvalidStateException ()
            : base(RelevantHttpStatusCode)
        {
        }

        public SubscriptionInvalidStateException (string message)
            : base(message, RelevantHttpStatusCode)
        {
        }

        public SubscriptionInvalidStateException (string message, Exception inner)
            : base(message, inner, RelevantHttpStatusCode)
        {
        }

    }
}