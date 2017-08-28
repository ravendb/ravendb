using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        private DynamicQueryMappingItem(string name, AggregationOperation aggregationOperation, bool isSpecifiedInWhere, bool isFullTextSearch)
        {
            Name = name;
            AggregationOperation = aggregationOperation;
            IsSpecifiedInWhere = isSpecifiedInWhere;
            IsFullTextSearch = isFullTextSearch;
        }

        public readonly string Name;

        public readonly bool IsFullTextSearch;

        public readonly bool IsSpecifiedInWhere;

        public AggregationOperation AggregationOperation { get; private set; }

        public static DynamicQueryMappingItem Create(string name)
        {
            return new DynamicQueryMappingItem(name, AggregationOperation.None, false, false);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation)
        {
            return new DynamicQueryMappingItem(name, aggregation, false, false);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation, bool isFullTextSearch)
        {
            return new DynamicQueryMappingItem(name, aggregation, false, isFullTextSearch);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation, Dictionary<string, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, aggregation, true, whereField.IsFullTextSearch);

            return new DynamicQueryMappingItem(name, aggregation, false, false);
        }

        public void SetAggregation(AggregationOperation aggregation)
        {
            AggregationOperation = aggregation;
        }
    }
}
