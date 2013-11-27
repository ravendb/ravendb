// -----------------------------------------------------------------------
//  <copyright file="QuotaException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Voron.Exceptions
{
	public class QuotaException : Exception
	{
		public enum Caller
		{
			None = 1,
			Pager,
			WriteAheadJournal
		}

		public QuotaException(string message, Caller caller)
			: base(message)
		{
			CallerInstance = caller;
		}

		public Caller CallerInstance { get; set; }
	}
}