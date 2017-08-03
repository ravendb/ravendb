using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Exceptions.Subscriptions
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
