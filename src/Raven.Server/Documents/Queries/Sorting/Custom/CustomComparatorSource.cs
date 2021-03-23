using Lucene.Net.Search;
using Raven.Client.Exceptions.Documents.Sorters;
using Raven.Server.Documents.Indexes.Sorting;

namespace Raven.Server.Documents.Queries.Sorting.Custom
{
    public class CustomComparatorSource : FieldComparatorSource
    {
        private readonly IndexQueryServerSide _query;
        private readonly SorterFactory _factory;

        public CustomComparatorSource(string sorterName, string databaseName, IndexQueryServerSide query)
        {
            _query = query;
            _factory = SorterCompilationCache.Instance.GetItemType(sorterName, databaseName);
            if (_factory == null)
                SorterDoesNotExistException.ThrowFor(sorterName);
        }

        public override FieldComparator NewComparator(string fieldName, int numHits, int sortPos, bool reversed)
        {
            var instance = _factory.CreateInstance(fieldName, numHits, sortPos, reversed, _query.Diagnostics);
            if (_query.Diagnostics == null)
                return instance;

            return new TestFieldComparator(instance, _query.Diagnostics);
        }
    }
}
