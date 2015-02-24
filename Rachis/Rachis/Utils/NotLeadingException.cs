// -----------------------------------------------------------------------
//  <copyright file="NotLeadingException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Rachis.Utils
{
	[Serializable]
	public class NotLeadingException : Exception
	{
		public string CurrentLeader { get; set; }

		public NotLeadingException()
		{
		}

		public NotLeadingException(string message) : base(message)
		{
		}

		public NotLeadingException(string message, Exception inner) : base(message, inner)
		{
		}

		protected NotLeadingException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}