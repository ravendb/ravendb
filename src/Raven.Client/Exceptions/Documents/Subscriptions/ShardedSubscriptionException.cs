using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class ShardedSubscriptionException : AggregateException
    {
        public ShardedSubscriptionException(string message) : base(message) { }
        public ShardedSubscriptionException(string message, params Exception[] innerExceptions) : base(message, innerExceptions) { }
        public ShardedSubscriptionException(string message, System.Collections.Generic.IEnumerable<Exception> innerExceptions) : base(message, innerExceptions) { }
    }
}
