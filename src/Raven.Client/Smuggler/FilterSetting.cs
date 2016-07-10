// -----------------------------------------------------------------------
//  <copyright file="FilterSettings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Smuggler
{
    public class FilterSetting
    {
        public string Path { get; set; }
        public List<string> Values { get; set; }
        public bool ShouldMatch { get; set; }

        public FilterSetting()
        {
            Values = new List<string>();
        }

        private static readonly Regex Regex = new Regex(@"('[^']+'|[^,]+)");

        public static List<string> ParseValues(string value)
        {
            var results = new List<string>();

            if (string.IsNullOrEmpty(value))
                return results;

            var matches = Regex.Matches(value);
            for (var i = 0; i < matches.Count; i++)
            {
                var match = matches[i].Value;

                if (match.StartsWith("'") && match.EndsWith("'"))
                    match = match.Substring(1, match.Length - 2);

                results.Add(match);
            }

            return results;
        }
    }
}
