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

		public static DateTime UtcNow
		{
			get
			{
				var temp = UtcDateTime;
				return temp == null ? DateTime.UtcNow : temp();
			}
		}
	}
}