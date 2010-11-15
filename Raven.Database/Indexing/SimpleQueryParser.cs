using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Database.Indexing
{
    public class SimpleQueryParser
    {
        static readonly Regex QueryTerms = new Regex(@"([^\s\(\+\-][\w._,]+)\:", RegexOptions.Compiled);

        public static HashSet<string> GetFields(string query)
        {
            var fields = new HashSet<string>();
            if(query == null)
                return fields;
            var queryTermMatches = QueryTerms.Matches(query);
            for (int x = 0; x < queryTermMatches.Count; x++)
            {
                Match match = queryTermMatches[x];
                String field = match.Groups[1].Value;
                fields.Add(field);
            }
            return fields;
        }
    }
}