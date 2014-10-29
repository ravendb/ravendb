// -----------------------------------------------------------------------
//  <copyright file="VoronUnrecoverableErrorException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Voron.Exceptions
{
	[Serializable]
	public class VoronUnrecoverableErrorException : Exception
	{
		 public VoronUnrecoverableErrorException()
		{
		}

		public VoronUnrecoverableErrorException(string message) 
			: base(message)
		{
		}

		public VoronUnrecoverableErrorException(string message, Exception inner) 
			: base(message, inner)
		{
		}

		protected VoronUnrecoverableErrorException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
	}
}