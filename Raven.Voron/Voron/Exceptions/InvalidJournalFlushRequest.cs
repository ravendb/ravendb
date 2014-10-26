// -----------------------------------------------------------------------
//  <copyright file="InvalidJournalFlushRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Voron.Exceptions
{
	public class InvalidJournalFlushRequest : Exception
	{
		public InvalidJournalFlushRequest(string message) : base(message)
		{
			
		}
	}
}