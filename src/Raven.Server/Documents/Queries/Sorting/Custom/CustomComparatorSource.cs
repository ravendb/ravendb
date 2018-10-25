using System;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Sorting;

namespace Raven.Server.Documents.Queries.Sorting.Custom
{
    public class CustomComparatorSource : FieldComparatorSource
    {
        private readonly Func<string, int, int, bool, FieldComparator> _activator;

        public CustomComparatorSource(string sorterName, string databaseName)
        {
            _activator = SorterCompilationCache.GetSorter(sorterName, databaseName);
        }

        public override FieldComparator NewComparator(string fieldName, int numHits, int sortPos, bool reversed)
        {
            return _activator(fieldName, numHits, sortPos, reversed);
        }
    }
}
