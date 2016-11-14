// -----------------------------------------------------------------------
//  <copyright file="SubscriptionException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Runtime.Serialization;

namespace Raven.NewClient.Abstractions.Exceptions.Subscriptions
{
    public abstract class SubscriptionException : Exception
    {
        protected SubscriptionException(HttpStatusCode httpResponseCode)
        {
            ResponseStatusCode = httpResponseCode;
        }

        protected SubscriptionException(string message, HttpStatusCode httpResponseCode)
            : base(message)
        {
            ResponseStatusCode = httpResponseCode;
        }

        protected SubscriptionException(string message, Exception inner, HttpStatusCode httpResponseCode)
            : base(message, inner)
        {
            ResponseStatusCode = httpResponseCode;
        }

        public HttpStatusCode ResponseStatusCode { get; private set; }
    }
}
