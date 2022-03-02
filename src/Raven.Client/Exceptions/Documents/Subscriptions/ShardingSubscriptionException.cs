using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class ShardingSubscriptionException : AggregateException
    {
        public ShardingSubscriptionException(string message) : base(message) { }
        public ShardingSubscriptionException(string message, params Exception[] innerExceptions) : base(message, innerExceptions) { }
        public ShardingSubscriptionException(string message, System.Collections.Generic.IEnumerable<Exception> innerExceptions) : base(message, innerExceptions) { }
    }
}
