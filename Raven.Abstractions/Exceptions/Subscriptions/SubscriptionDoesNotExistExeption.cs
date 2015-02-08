// -----------------------------------------------------------------------
//  <copyright file="SubscriptionDoesNotExistExeption.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions.Subscriptions
{
	[Serializable]
	public class SubscriptionDoesNotExistExeption : SubscriptionException
	{
		public static HttpStatusCode RelevantHttpStatusCode = HttpStatusCode.NotFound;

		public SubscriptionDoesNotExistExeption() : base(RelevantHttpStatusCode)
		{
		}

		public SubscriptionDoesNotExistExeption(string message)
			: base(message, RelevantHttpStatusCode)
		{
		}

		public SubscriptionDoesNotExistExeption(string message, Exception inner)
			: base(message, inner, RelevantHttpStatusCode)
		{
		}

		protected SubscriptionDoesNotExistExeption(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}