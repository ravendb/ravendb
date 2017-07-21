using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        public DynamicQueryMappingItem(string name, AggregationOperation aggregationOperation)
        {
            Name = name;
            AggregationOperation = aggregationOperation;
        }

        public readonly string Name;

        public AggregationOperation AggregationOperation { get; set; }
    }
}