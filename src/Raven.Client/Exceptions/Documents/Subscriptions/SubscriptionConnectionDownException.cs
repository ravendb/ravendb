// -----------------------------------------------------------------------
//  <copyright file="SubscriptionConnectionDownException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Subscriptions
{
    public class SubscriptionConnectionDownException : SubscriptionException
    {
        public SubscriptionConnectionDownException(string message) : base(message)
        {
        }
        
        public SubscriptionConnectionDownException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
