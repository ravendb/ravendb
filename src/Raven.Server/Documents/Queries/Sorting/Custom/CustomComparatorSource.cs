using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Sorting;

namespace Raven.Server.Documents.Queries.Sorting.Custom
{
    public class CustomComparatorSource : FieldComparatorSource
    {
        private readonly IndexQueryServerSide _query;
        private readonly CreateSorter _activator;

        public CustomComparatorSource(string sorterName, string databaseName, IndexQueryServerSide query)
        {
            _query = query;
            _activator = SorterCompilationCache.GetSorter(sorterName, databaseName);
        }

        public override FieldComparator NewComparator(string fieldName, int numHits, int sortPos, bool reversed)
        {
            var instance = _activator(fieldName, numHits, sortPos, reversed, _query.Diagnostics);
            if (_query.Diagnostics == null)
                return instance;

            return new TestFieldComparator(instance, _query.Diagnostics);
        }
    }
}
