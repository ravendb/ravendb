// -----------------------------------------------------------------------
//  <copyright file="RavenJsonConvert.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Json.Linq
{
	public class RavenJsonConvert
	{
		internal static DateTime EnsureDateTime(DateTime value, DateTimeZoneHandling timeZone)
		{
			switch (timeZone)
			{
				case DateTimeZoneHandling.Local:
					value = SwitchToLocalTime(value);
					break;
				case DateTimeZoneHandling.Utc:
					value = SwitchToUtcTime(value);
					break;
				case DateTimeZoneHandling.Unspecified:
					value = new DateTime(value.Ticks, DateTimeKind.Unspecified);
					break;
				case DateTimeZoneHandling.RoundtripKind:
					break;
				default:
					throw new ArgumentException("Invalid date time handling value.");
			}

			return value;
		}


		private static DateTime SwitchToLocalTime(DateTime value)
		{
			switch (value.Kind)
			{
				case DateTimeKind.Unspecified:
					return new DateTime(value.Ticks, DateTimeKind.Local);

				case DateTimeKind.Utc:
					return value.ToLocalTime();

				case DateTimeKind.Local:
					return value;
			}
			return value;
		}

		private static DateTime SwitchToUtcTime(DateTime value)
		{
			switch (value.Kind)
			{
				case DateTimeKind.Unspecified:
					return new DateTime(value.Ticks, DateTimeKind.Utc);

				case DateTimeKind.Utc:
					return value;

				case DateTimeKind.Local:
					return value.ToUniversalTime();
			}
			return value;
		}
	}
}