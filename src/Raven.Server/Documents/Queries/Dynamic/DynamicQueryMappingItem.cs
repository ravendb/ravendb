using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public sealed class DynamicQueryMappingItem
    {
        private DynamicQueryMappingItem(
            QueryFieldName name,
            AggregationOperation aggregationOperation,
            GroupByArrayBehavior groupByArrayBehavior,
            bool isSpecifiedInWhere,
            bool isFullTextSearch,
            bool isExactSearch,
            bool hasHighlighting,
            bool hasSuggestions,
            AutoSpatialOptions spatial,
            AutoVectorOptions vector)
        {
            Name = name;
            AggregationOperation = aggregationOperation;
            GroupByArrayBehavior = groupByArrayBehavior;
            HasHighlighting = hasHighlighting;
            HasSuggestions = hasSuggestions;
            IsSpecifiedInWhere = isSpecifiedInWhere;
            IsFullTextSearch = isFullTextSearch;
            IsExactSearch = isExactSearch;
            Spatial = spatial;
            Vector = vector;
        }

        public readonly QueryFieldName Name;

        public readonly bool IsFullTextSearch;

        public readonly bool IsExactSearch;

        public bool HasSuggestions;

        public bool HasHighlighting;
        
        public readonly bool IsSpecifiedInWhere;

        public readonly AutoSpatialOptions Spatial;
        public readonly AutoVectorOptions Vector;

        public AggregationOperation AggregationOperation { get; private set; }

        public GroupByArrayBehavior GroupByArrayBehavior { get; }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation)
        {
            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, false, false, false, false, null, null);
        }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation, bool isFullTextSearch, bool isExactSearch, bool hasHighlighting, bool hasSuggestions, AutoSpatialOptions spatial, AutoVectorOptions vector)
        {
            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, isFullTextSearch, isExactSearch, hasHighlighting, hasSuggestions, spatial, vector);
        }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation, Dictionary<QueryFieldName, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, true, whereField.IsFullTextSearch, whereField.IsExactSearch, false, false, whereField.Spatial, whereField.Vector);

            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, false, false, false, false, null, null);
        }

        public static DynamicQueryMappingItem CreateGroupBy(QueryFieldName name, GroupByArrayBehavior groupByArrayBehavior, Dictionary<QueryFieldName, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, true, whereField.IsFullTextSearch, whereField.IsExactSearch, false, false, whereField.Spatial, whereField.Vector);

            return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, false, false, false, false, false, null, null);
        }

        public static DynamicQueryMappingItem CreateGroupBy(QueryFieldName name, GroupByArrayBehavior groupByArrayBehavior, bool isSpecifiedInWhere, bool isFullTextSearch, bool isExactSearch)
        {
            return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, isSpecifiedInWhere: isSpecifiedInWhere, isFullTextSearch: isFullTextSearch, isExactSearch: isExactSearch, hasHighlighting: false, hasSuggestions: false, spatial: null, vector: null);
        }

        public void SetAggregation(AggregationOperation aggregation)
        {
            AggregationOperation = aggregation;
        }
    }
}
