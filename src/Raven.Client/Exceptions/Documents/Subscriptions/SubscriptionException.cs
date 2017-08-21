// -----------------------------------------------------------------------
//  <copyright file="SubscriptionException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public abstract class SubscriptionException : RavenException
    {
        protected SubscriptionException(string message)
            : base(message)
        {
        }

        protected SubscriptionException(string message, Exception inner)
            : base(message, inner)
        {
        }
        
    }
}
