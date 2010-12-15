using System.Collections.Generic;
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

        public IDictionary<string, HashSet<string>> GetTerms(string index, string field)
        {
            var result = new Dictionary<string, HashSet<string>>();
            var currentIndexSearcher = database.IndexStorage.GetCurrentIndexSearcher(index);
            IndexSearcher searcher;
            using(currentIndexSearcher.Use(out searcher))
            {
                TermEnum termEnum;
                if(field != null)
                {
                    termEnum = searcher.GetIndexReader().Terms(new Term(field));
                }
                else
                {
                    termEnum = searcher.GetIndexReader().Terms();
                    if (termEnum.Next() == false)
                        return result;
                }
                var term = termEnum.Term();
                while (field == null || field.Equals(term.Field()))
                {
                    bool breakOuterLoop = false;
                    while(term.Field().StartsWith("__")) // reserved, usually a unique item that we have N off
                    {
                        if (termEnum.Next() == false)
                        {
                            breakOuterLoop = true;
                            break;
                        }
                        term = termEnum.Term();
                    }
                    if (breakOuterLoop)
                        break;

                    HashSet<string> set;
                    if (result.TryGetValue(term.Field(), out set) == false)
                        result[term.Field()] = set = new HashSet<string>();

                    set.Add(term.Text());

                    if (termEnum.Next() == false)
                        break;
                    term = termEnum.Term();
                }
            }

            return result;
        }
    }
}