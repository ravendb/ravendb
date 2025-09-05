using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public sealed class DocumentUnderActiveMigrationException : SubscriptionException
    {
        public DocumentUnderActiveMigrationException(string message) : base(message)
        {
        }

        public DocumentUnderActiveMigrationException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}