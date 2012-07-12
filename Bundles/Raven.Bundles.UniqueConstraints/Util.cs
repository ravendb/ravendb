using System;
using System.Text;

namespace Raven.Bundles.UniqueConstraints
{
	internal static class Util
	{
		public static string EscapeUniqueValue(object value)
		{
			var stringToEscape = value.ToString();
			var escapeDataString = Uri.EscapeDataString(stringToEscape);
			if (stringToEscape == escapeDataString)
				return stringToEscape;
			// to avoid issues with ids, we encode the entire thing as safe Base64
			return Convert.ToBase64String(Encoding.UTF8.GetBytes(stringToEscape));
		}
	}
}