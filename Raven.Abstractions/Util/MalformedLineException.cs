using System.ComponentModel;
using System.Security.Permissions;
using System.Runtime.Serialization;
using System;

namespace Raven.Abstractions.Util 
{
	[Serializable]
	public class MalformedLineException : Exception 
	{
		public MalformedLineException()
		{
		}

		public MalformedLineException(string message) : base(message)
		{
		}

		public MalformedLineException(string message, Exception inner)
		{
		}
	}
}

