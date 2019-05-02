using System.Globalization;

namespace Raven.Client.Extensions
{
    internal static class StringExtensions
    {
        public static string ToWebSocketPath(this string path)
        {
            return path
                .Replace("http://", "ws://")
                .Replace("https://", "wss://")
                .Replace(".fiddler", "");
        }

        public static string ToInvariantString(this int value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static string ToInvariantString(this float value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static string ToInvariantString(this double value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static string ToInvariantString(this decimal value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static string ToInvariantString(this long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public static bool IsIdentifier(string token)
        {
            return IsIdentifier(token, 0, token.Length);
        }

        public static bool IsIdentifier(string token, int start, int length)
        {
            if (length == 0 || length > 256)
                return false;

            if (!char.IsLetter(token[start+0]) && token[start+0] != '_')
                return false;

            for (int i = 1; i < length; i++)
            {
                if (!char.IsLetterOrDigit(token[start + i]) && token[start + i] != '_')
                    return false;
            }

            return true;
        }
    }
}
