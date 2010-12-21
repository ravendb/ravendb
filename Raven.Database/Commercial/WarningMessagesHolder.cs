//-----------------------------------------------------------------------
// <copyright file="WarningMessagesHolder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Commercial
{
	public class WarningMessagesHolder
	{
		public List<string> Messages
		{
			get; set;
		}

		public WarningMessagesHolder()
		{
			Messages = new List<string>();
		}
	}
}
