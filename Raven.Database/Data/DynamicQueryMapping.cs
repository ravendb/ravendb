using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.QueryParsers;
using System.Collections;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Analysis;
using Raven.Database.Indexing;
using System.Text.RegularExpressions;

namespace Raven.Database.Data
{
    public class DynamicQueryMapping
    {
        static readonly Regex queryTerms = new Regex(@"([^\s\(\+\-][\w._]+)\:", RegexOptions.Compiled);

        public DynamicQueryMappingItem[] Items
        {
            get;
            set;
        }

        public static DynamicQueryMapping Create(string query)
        {
            var queryTermMatches = queryTerms.Matches(query);
             var fields = new HashSet<string>();
            for (int x = 0; x < queryTermMatches.Count; x++)
            {
                Match match = queryTermMatches[x];
                String field = match.Groups[1].Value;
                fields.Add(field);
            }
            
            return new DynamicQueryMapping()
            {
                Items = fields.Select(x => new DynamicQueryMappingItem()
                {
                    From = x,
                    To = x.Replace(".", "") // for now
                }).ToArray()
            };
                      
        }
    }
}
