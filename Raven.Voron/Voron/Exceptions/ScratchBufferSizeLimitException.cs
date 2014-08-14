// -----------------------------------------------------------------------
//  <copyright file="ScratchBufferSizeLimitException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Voron.Exceptions
{

	[Serializable]
	public class ScratchBufferSizeLimitException : Exception
	{
		public ScratchBufferSizeLimitException()
		{
		}

		public ScratchBufferSizeLimitException(string message) : base(message)
		{
		}

		public ScratchBufferSizeLimitException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ScratchBufferSizeLimitException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}