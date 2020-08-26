using System;
using System.Globalization;
using System.Text;

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
        
        private static readonly char[] LiteralSymbolsToEscape = { '\'', '\"', '\\', '\a', '\b', '\f', '\n', '\r', '\t', '\v' };
        private static readonly string[] LiteralEscapedSymbols = { @"\'", @"\""", @"\\", @"\a", @"\b", @"\f", @"\n", @"\r", @"\t", @"\v" };

        public static string EscapeString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;
            
            var builder = new StringBuilder(6 * value.Length);
            EscapeStringInternal(builder, value);
            return builder.ToString();
        }
        
        public static void EscapeString(StringBuilder builder, string value)
        {
            if (string.IsNullOrEmpty(value))
                return;

            EscapeStringInternal(builder, value);
        }

        private static void EscapeStringInternal(StringBuilder builder, string value)
        {
            foreach (var c in value)
            {
                EscapeChar(builder, c);
            }
        }

        public static void EscapeChar(StringBuilder builder, char c)
        {
            var index = Array.IndexOf(LiteralSymbolsToEscape, c);

            if (index != -1)
            {
                builder.Append(LiteralEscapedSymbols[index]);
                return;
            }

            if (char.IsLetterOrDigit(c) == false
                && char.IsWhiteSpace(c) == false
                && char.IsSymbol(c) == false
                && char.IsPunctuation(c) == false)
            {
                builder.AppendFormat(@"\u{0:x4}", (int)c);
                return;
            }

            builder.Append(c);
        }
    }
}
