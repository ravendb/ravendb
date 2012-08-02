using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Extensions
{

	static class DateTimeExtensions
	{
		public static long ToUnixTime(this DateTime time)
		{
			return (long)time.ToUniversalTime().Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
		}
	}
}
