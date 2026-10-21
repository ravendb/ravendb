using System;
using System.Text.RegularExpressions;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

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
