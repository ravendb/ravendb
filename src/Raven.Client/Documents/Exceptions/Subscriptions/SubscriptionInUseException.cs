// -----------------------------------------------------------------------
//  <copyright file="SubscriptionAlreadyInUseException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;

namespace Raven.Client.Documents.Exceptions.Subscriptions
{
    public class SubscriptionInUseException : SubscriptionException
    {
        public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.Gone;

        public SubscriptionInUseException() : base(RelevantHttpStatusCode)
        {
        }

        public SubscriptionInUseException(string message)
            : base(message, RelevantHttpStatusCode)
        {
        }

        public SubscriptionInUseException(string message, Exception inner)
            : base(message, inner, RelevantHttpStatusCode)
        {
        }

    }
}
