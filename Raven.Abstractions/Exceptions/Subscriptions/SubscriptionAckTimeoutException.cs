// -----------------------------------------------------------------------
//  <copyright file="SubscriptionAckTimeoutException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions.Subscriptions
{
    [Serializable]
    public class SubscriptionAckTimeoutException : SubscriptionException
    {
        public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.RequestTimeout;

        public SubscriptionAckTimeoutException()
            : base(RelevantHttpStatusCode)
        {
        }

        public SubscriptionAckTimeoutException(string message)
            : base(message, RelevantHttpStatusCode)
        {
        }

        public SubscriptionAckTimeoutException(string message, Exception inner)
            : base(message, inner, RelevantHttpStatusCode)
        {
        }

#if !DNXCORE50
        protected SubscriptionAckTimeoutException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
#endif
    }
}