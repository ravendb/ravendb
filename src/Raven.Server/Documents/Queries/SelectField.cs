using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public readonly string Name;

        public readonly string Alias;

        public readonly string SourceAlias;

        public readonly AggregationOperation AggregationOperation;

        public readonly bool IsGroupByKey;

        public readonly string[] GroupByKeys;

        private SelectField(string name, string alias, string sourceAlias)
        {
            Name = name;
            Alias = alias;
            SourceAlias = sourceAlias;
        }

        private SelectField(string name, string alias, AggregationOperation aggregationOperation)
        {
            Name = name;
            Alias = alias;
            AggregationOperation = aggregationOperation;
        }

        private SelectField(string alias, string[] groupByKeys)
        {
            Alias = alias;
            IsGroupByKey = true;
            GroupByKeys = groupByKeys;
        }

        public static SelectField Create(string name, string alias, string sourceAlias)
        {
            return new SelectField(name, alias, sourceAlias);
        }

        public static SelectField CreateGroupByAggregation(string name, string alias, AggregationOperation aggregation)
        {
            return new SelectField(name, alias, aggregation);
        }

        public static SelectField CreateGroupByKeyField(string alias, params string[] groupByKeys)
        {
            return new SelectField(alias, groupByKeys);
        }
    }
}
