using System;
using System.IO;
using System.Text.RegularExpressions;

namespace Raven.Studio.Infrastructure
{
	public static class StringExtensions
	{
		public static string ReplaceSingle(this string str, string toReplace, string newString)
		{
			var index = str.IndexOf(toReplace, StringComparison.Ordinal);
			if (index == -1)
				return newString;

			return str.Remove(index, toReplace.Length).Insert(index, newString);
		}

		public static string ShortViewOfString(this string str, int margin, string replacement = " ... ")
		{
			if (str.Length <= margin*2 + replacement.Length)
				return str;

			return str.Substring(0, margin) + replacement + str.Substring(str.Length - margin - 1);
		}

		private static readonly Regex RegexWhitespaces = new Regex(@"\s", RegexOptions.Multiline | RegexOptions.CultureInvariant);

		public static string ReplaceRegexWhitespacesWithSpace(this string str)
		{
			if (str == null)
				return null;

			return RegexWhitespaces.Replace(str, " ").Trim();
		}
	}
}