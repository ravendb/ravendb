using System.Text.RegularExpressions;

namespace Raven.Server.Integrations.PostgreSQL
{
    public static class StringExtensions
    {
        public static string Replace(this string input, Regex regex, string groupName, string replacement)
        {
            return regex.Replace(input, m => ReplaceNamedGroup(input, groupName, replacement, m));
        }

        private static string ReplaceNamedGroup(string input, string groupName, string replacement, Match m)
        {
            var capture = m.Value;
            capture = capture.Remove(m.Groups[groupName].Index - m.Index, m.Groups[groupName].Length);
            capture = capture.Insert(m.Groups[groupName].Index - m.Index, replacement);
            return capture;
        }

        public static string NormalizeLineEndings(this string input)
        {
            return input.Replace("\r\n", "\n").Replace("\r", "\n");
        }
    }
}
