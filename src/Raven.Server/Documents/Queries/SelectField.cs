using System;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public readonly ValueTokenType? ValueTokenType;

        public readonly string Name;

        public readonly string Alias;

        public readonly object Value;

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

        public SelectField(object value, string alias, ValueTokenType type)
        {
            ValueTokenType = type;
            Value = value;
            Alias = alias;
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

        public static SelectField Create(string name)
        {
            return new SelectField(name, null, null);
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

        public static SelectField CreateValue(string val, string alias, ValueTokenType type)
        {
            object finalVal = val;
            switch (type)
            {
                case Parser.ValueTokenType.Long:
                    finalVal = long.Parse(val);
                    break;
                case Parser.ValueTokenType.Double:
                    finalVal = double.Parse(val);
                    break;
                case Parser.ValueTokenType.True:
                    finalVal = true;
                    break;
                case Parser.ValueTokenType.False:
                    finalVal = false;
                    break;
                case Parser.ValueTokenType.Null:
                    finalVal = null;
                    break;
            }

            return new SelectField(finalVal, alias, type);
        }
    }
}
