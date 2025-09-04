using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public sealed class SubscriptionConnectionDownException : SubscriptionException
    {
        public SubscriptionConnectionDownException(string message) : base(message)
        {
        }
        
        public SubscriptionConnectionDownException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}