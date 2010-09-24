using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.QueryParsers;
using System.Collections;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;

namespace Raven.Bundles.DynamicQueries.Data
{
    public class DynamicQueryMapping
    {
        public DynamicQueryMappingItem[] Items
        {
            get;
            set;
        }

        public DynamicQueryMapping()
        {

        }

        public static DynamicQueryMapping Create(string query)
        {
            var parsedQuery = new QueryParser(Lucene.Net.Util.Version.LUCENE_29, "", new
StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29)).Parse(query);
            var terms = new Hashtable();
            parsedQuery.ExtractTerms(terms);
            var fields = new HashSet<string>();
            foreach (Term term in terms.Keys)
            {
                fields.Add(term.Field());
            }

            return new DynamicQueryMapping()
            {
                Items = fields.Select(x=> new DynamicQueryMappingItem(){
                    From = x,
                    To = x.Replace(".", "") // for now
                }).ToArray()
            };
        }
    }
}
