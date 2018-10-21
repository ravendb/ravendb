using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class DynamicQueryMappingItem
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
            AutoSpatialOptions spatial)
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
        }

        public readonly QueryFieldName Name;

        public readonly bool IsFullTextSearch;

        public readonly bool IsExactSearch;

        public bool HasSuggestions;

        public bool HasHighlighting;

        public readonly bool IsSpecifiedInWhere;

        public readonly AutoSpatialOptions Spatial;

        public AggregationOperation AggregationOperation { get; private set; }

        public GroupByArrayBehavior GroupByArrayBehavior { get; }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation)
        {
            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, false, false, false, false, null);
        }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation, bool isFullTextSearch, bool isExactSearch, bool hasHighlighting, bool hasSuggestions, AutoSpatialOptions spatial)
        {
            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, isFullTextSearch, isExactSearch, hasHighlighting, hasSuggestions, spatial);
        }

        public static DynamicQueryMappingItem Create(QueryFieldName name, AggregationOperation aggregation, Dictionary<QueryFieldName, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, true, whereField.IsFullTextSearch, whereField.IsExactSearch, false, false, whereField.Spatial);

            return new DynamicQueryMappingItem(name, aggregation, GroupByArrayBehavior.NotApplicable, false, false, false, false, false, null);
        }

        public static DynamicQueryMappingItem CreateGroupBy(QueryFieldName name, GroupByArrayBehavior groupByArrayBehavior, Dictionary<QueryFieldName, WhereField> whereFields)
        {
            if (whereFields.TryGetValue(name, out var whereField))
                return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, true, whereField.IsFullTextSearch, whereField.IsExactSearch, false, false, whereField.Spatial);

            return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, false, false, false, false, false, null);
        }

        public static DynamicQueryMappingItem CreateGroupBy(QueryFieldName name, GroupByArrayBehavior groupByArrayBehavior, bool isSpecifiedInWhere, bool isFullTextSearch, bool isExactSearch)
        {
            return new DynamicQueryMappingItem(name, AggregationOperation.None, groupByArrayBehavior, isSpecifiedInWhere: isSpecifiedInWhere, isFullTextSearch: isFullTextSearch, isExactSearch: isExactSearch, hasHighlighting: false, hasSuggestions: false, spatial: null);
        }

        public void SetAggregation(AggregationOperation aggregation)
        {
            AggregationOperation = aggregation;
        }
    }
}
