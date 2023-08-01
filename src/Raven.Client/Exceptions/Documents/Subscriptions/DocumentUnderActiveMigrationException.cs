// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

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
