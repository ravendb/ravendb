using System;
using Voron.Data.Fixed;

namespace Raven.Server.Indexing.Corax.Queries
{
    public class TermQuery : Query
    {
        private readonly string _field;
        private readonly string _term;

        public TermQuery(string field, string term)
        {
            _field = field;
            _term = term;
        }

        protected override void Init()
        {

        }

        public override QueryMatch[] Execute()
        {
            var property = Context.Transaction.InnerTransaction.ReadTree(_field);
            if (property == null)
                return Array.Empty<QueryMatch>();


            var fixedSizeTree = new FixedSizeTree(Context.Transaction.InnerTransaction.LowLevelTransaction, property, _term, 0);
            using (var it = fixedSizeTree.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    return Array.Empty<QueryMatch>();
                var termFreqInDocs = fixedSizeTree.NumberOfEntries;
                var numberOfDocs = IndexEntries.NumberOfEntries;
                var idf = IndexingConventions.Idf(termFreqInDocs, numberOfDocs);
                var weight = (idf * idf) * Boost;
                var results = new QueryMatch[fixedSizeTree.NumberOfEntries];
                int index = 0;
                do
                {
                    if (it.Seek(long.MinValue) == false)
                        throw new InvalidOperationException("Inconsistent number of results with can't seek to first");

                    results[index++] = new QueryMatch
                    {
                        DocumentId = (it.CurrentKey),
                        Score = weight
                    };
                } while (it.MoveNext());
                return results;
            }

        }

        public override string ToString()
        {
            return _field + ":" + _term;
        }
    }
}