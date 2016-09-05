using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        private string _normalizedName;

        public DynamicQueryMappingItem(string name, FieldMapReduceOperation mapReduceOperation)
        {
            Name = name;
            MapReduceOperation = mapReduceOperation;
        }

        public string Name { get; }

        public string NormalizedName => _normalizedName ?? (_normalizedName = IndexField.ReplaceInvalidCharactersInFieldName(Name));

        public FieldMapReduceOperation MapReduceOperation { get; }
    }
}