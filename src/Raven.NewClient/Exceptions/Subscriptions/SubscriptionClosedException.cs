// -----------------------------------------------------------------------
//  <copyright file="SubscriptionClosedException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;

namespace Raven.NewClient.Client.Exceptions.Subscriptions
{
    public class SubscriptionClosedException : SubscriptionException
    {
        public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.Unused;

        public SubscriptionClosedException()
            : base(RelevantHttpStatusCode)
        {
        }

        public SubscriptionClosedException(string message)
            : base(message, RelevantHttpStatusCode)
        {
        }

        public SubscriptionClosedException(string message, Exception inner)
            : base(message, inner, RelevantHttpStatusCode)
        {
        }

    }
}
