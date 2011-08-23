//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions
{
	public static class SystemTime
	{
		public static Func<DateTime> UtcDateTime;

		public static DateTime Now
		{
			get
			{
				if(UtcDateTime == null)
					return DateTime.Now;
				return UtcDateTime().ToLocalTime();
			}
		}

		public static DateTime UtcNow
		{
			get
			{
				if (UtcDateTime == null)
					return DateTime.UtcNow;
				return UtcDateTime();
			}
		}
	}
}