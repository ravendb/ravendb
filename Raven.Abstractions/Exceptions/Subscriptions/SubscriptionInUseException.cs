// -----------------------------------------------------------------------
//  <copyright file="SubscriptionAlreadyInUseException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions.Subscriptions
{
    [Serializable]
    public class SubscriptionInUseException : SubscriptionException
    {
        public static HttpStatusCode RelavantHttpStatusCode = HttpStatusCode.Gone;

        public SubscriptionInUseException() : base(RelavantHttpStatusCode)
        {
        }

        public SubscriptionInUseException(string message)
            : base(message, RelavantHttpStatusCode)
        {
        }

        public SubscriptionInUseException(string message, Exception inner)
            : base(message, inner, RelavantHttpStatusCode)
        {
        }

        protected SubscriptionInUseException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
    }
}
