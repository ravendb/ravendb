using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
    {
        private DynamicQueryMappingItem(string name, AggregationOperation aggregationOperation, bool isSpecifiedInWhere, bool isFullTextSearch, bool isExactSearch, AutoSpatialOptions spatial)
        {
            Name = name;
            AggregationOperation = aggregationOperation;
            IsSpecifiedInWhere = isSpecifiedInWhere;
            IsFullTextSearch = isFullTextSearch;
            IsExactSearch = isExactSearch;
            Spatial = spatial;
        }

        public readonly string Name;

        public readonly bool IsFullTextSearch;

        public readonly bool IsExactSearch;

        public readonly bool IsSpecifiedInWhere;

        public readonly AutoSpatialOptions Spatial;

        public AggregationOperation AggregationOperation { get; private set; }

        public static DynamicQueryMappingItem Create(string name)
        {
            return new DynamicQueryMappingItem(name, AggregationOperation.None, false, false, false, null);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation)
        {
            return new DynamicQueryMappingItem(name, aggregation, false, false, false, null);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation, bool isFullTextSearch, bool isExactSearch, AutoSpatialOptions spatial)
        {
            return new DynamicQueryMappingItem(name, aggregation, false, isFullTextSearch, isExactSearch, spatial);
        }

        public static DynamicQueryMappingItem Create(string name, AggregationOperation aggregation, Dictionary<string, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, aggregation, true, whereField.IsFullTextSearch, whereField.IsExactSearch, whereField.Spatial);

            return new DynamicQueryMappingItem(name, aggregation, false, false, false, null);
        }

        public void SetAggregation(AggregationOperation aggregation)
        {
            AggregationOperation = aggregation;
        }
    }
}
