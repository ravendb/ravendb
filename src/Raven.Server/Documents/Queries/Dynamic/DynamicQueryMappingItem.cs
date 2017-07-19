using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        private string _normalizedName;

        public DynamicQueryMappingItem(string name, AggregationOperation aggregationOperation)
        {
            Name = name;
            AggregationOperation = aggregationOperation;
        }

        public string Name { get; }

        public string NormalizedName => _normalizedName ?? (_normalizedName = IndexField.ReplaceInvalidCharactersInFieldName(Name));

        public AggregationOperation AggregationOperation { get; set; }
    }
}