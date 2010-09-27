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

namespace Raven.Database.Data
{
    public class DynamicQueryMapping
    {
        public DynamicQueryMappingItem[] Items
        {
            get;
            set;
        }

        public static DynamicQueryMapping Create(string query)
        {
            var standardAnalyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29);
            var perKeywordAnalyzer = new PerFieldAnalyzerWrapper(standardAnalyzer);

            try
            {
                var parsedQuery = QueryBuilder.BuildQuery(query, perKeywordAnalyzer);

                var terms = new Hashtable();
                parsedQuery.ExtractTerms(terms);
                var fields = new HashSet<string>();
                foreach (Term term in terms.Keys)
                {
                    fields.Add(term.Field());
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
            finally
            {
                perKeywordAnalyzer.Close();
                standardAnalyzer.Close();
            }

          
        }
    }
}
