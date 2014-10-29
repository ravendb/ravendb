// -----------------------------------------------------------------------
//  <copyright file="QuotaException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Voron.Exceptions
{
	[Serializable]
	public class QuotaException : Exception
	{
		public QuotaException()
		{

		}

		public QuotaException(string message)
			: base(message)
		{
		}

		public QuotaException(string message, Exception inner)
			: base(message, inner)
		{

		}

		public QuotaException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{

		}
	}
}

