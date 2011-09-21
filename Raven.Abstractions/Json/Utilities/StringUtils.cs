using System;

namespace Raven.Json.Utilities
{
	internal static class StringUtils
	{
		public static string FormatWith(this string format, IFormatProvider provider, params object[] args)
		{
			return string.Format(provider, format, args);
		}
	}
}