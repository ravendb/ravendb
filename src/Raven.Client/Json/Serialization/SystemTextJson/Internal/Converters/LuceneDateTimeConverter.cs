using System;
using System.Text.RegularExpressions;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal static class LuceneDateTimeConverter
    {
        private static readonly Regex LuceneDateTimePattern = new Regex(@"^\d{17}$", RegexOptions.Compiled);

        internal static DateTime TryParseLucene(string input)
        {
            if (input != null && LuceneDateTimePattern.IsMatch(input))
                return DateTools.StringToDate(input);

            return default;
        }
    }
}
