using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Queries
{
    public class TermsQueryRunner
    {
        private readonly DocumentDatabase database;

        public TermsQueryRunner(DocumentDatabase database)
        {
            this.database = database;
        }

        public ISet<string> GetTerms(string index, string field, string fromValue, int pageSize)
        {
            if(field == null) throw new ArgumentNullException("field");
            if(index == null) throw new ArgumentNullException("index");
            
            var result = new HashSet<string>();
            var currentIndexSearcher = database.IndexStorage.GetCurrentIndexSearcher(index);
            IndexSearcher searcher;
            using(currentIndexSearcher.Use(out searcher))
            {
                var termEnum = searcher.GetIndexReader().Terms(new Term(field, fromValue ?? string.Empty));
                if (string.IsNullOrEmpty(fromValue) == false)// need to skip this value
                {
                    while(fromValue.Equals(termEnum.Term().Text()))
                    {
                        if (termEnum.Next() == false)
                            return result;
                    }
                }
                while (field.Equals(termEnum.Term().Field()))
                {
                    result.Add(termEnum.Term().Text());

                    if (result.Count >= pageSize)
                        break;

                    if (termEnum.Next() == false)
                        break;
                }
            }

            return result;
        }
    }
}