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
    }
}
