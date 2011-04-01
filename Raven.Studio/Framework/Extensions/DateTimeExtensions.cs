namespace Raven.Studio.Framework.Extensions
{
	using System;

	public static class DateTimeExtensions
	{
		public static string HowLongSince(this DateTime start)
		{
            if (start == default(DateTime)) return "unknown";

			var values = start.TimePassedSince();

			if (values[0] > 0) return FormatHowLongSince("{0} year", values[0]);
			if (values[1] > 0) return FormatHowLongSince("{0} month", values[1]);
			if (values[2] > 0) return FormatHowLongSince("{0} day", values[2]);
			if (values[3] > 0) return FormatHowLongSince("{0} hour", values[3]);
			if (values[4] > 0) return FormatHowLongSince("{0} minute", values[4]);
			return "just now";
		}

		static string FormatHowLongSince(string text, int num)
		{
			if (num > 1) text += "s";
			return string.Format(text + " ago", num);
		}

		public static int[] TimePassedSince(this DateTime start)
		{
			//http://dotnet.org.za/hiltong/archive/2005/07/25/40283.aspx
			var end = DateTime.UtcNow;
			var startUtc = start.ToUniversalTime();

			int years = end.Year - startUtc.Year;
			int months = 0;
			int days = 0;
			int hours = 0;
			int minutes = 0;

			// was the last year was a full year.
			if (end < startUtc.AddYears(years) && years != 0)
			{
				--years;
			}

			startUtc = startUtc.AddYears(years);

			// startUtc <= end and the diff between them is < 1 year.
			if (startUtc.Year == end.Year)
			{
				months = end.Month - startUtc.Month;
			}
			else
			{
				months = (12 - startUtc.Month) + end.Month;
			}

			// Check if the last month was a full month.
			if (end < startUtc.AddMonths(months) && months != 0)
			{
				--months;
			}

			startUtc = startUtc.AddMonths(months);
			// startUtc < end and is within 1 month of each other.
			days = (end - startUtc).Days;

			var span = end - startUtc;
			hours = span.Hours;
			minutes = span.Minutes;

			return new[] {years, months, days, hours, minutes};
		}
	}
}