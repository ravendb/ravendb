using System;
using Lucene.Net.Index;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing.Sorting.Custom
{
    public abstract class IndexEntriesToComparablesGenerator
    {
        protected IndexQuery IndexQuery;

        protected IndexEntriesToComparablesGenerator(IndexQuery indexQuery)
        {
            IndexQuery = indexQuery;
        }

        public abstract IComparable Generate(IndexReader reader, int doc);
    }
}
