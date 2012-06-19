using System;
using System.Text.RegularExpressions;

namespace Raven.Studio.Extensions
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

        public static string TrimmedViewOfString(this string str, int maxWidth, string replacement = " ... ")
        {
            if (str.Length <= maxWidth)
            {
                return str;
            }

            var pieceLength = (maxWidth - replacement.Length)/2;
            return str.Substring(0, pieceLength) + replacement + str.Substring(str.Length - pieceLength);
        }

		private static readonly Regex RegexWhitespaces = new Regex(@"\s+", RegexOptions.Multiline | RegexOptions.CultureInvariant );

		public static string NormalizeWhitespace(this string str)
		{
			if (str == null)
				return null;

			return RegexWhitespaces.Replace(str, " ").Trim();
		}

        public static bool IsValidRegex(this string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return false;
            }

            try
            {
                Regex.IsMatch(" ", value);
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }
	}
}