using System;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public ValueTokenType? ValueTokenType;

        public string Name;

        public string Alias;

        public object Value;

        public string SourceAlias;

        public string Format;

        public SelectField[] FormatArguments;

        public AggregationOperation AggregationOperation;

        public bool IsGroupByKey;

        public string[] GroupByKeys;
        public bool SourceIsArray;

        private SelectField()
        {
            
        }

        public static SelectField Create(string name)
        {
            return new SelectField
            {
                Name = name
            };
        }

        public static SelectField CreateFormat(string alias, string format, SelectField[] args)
        {
            return new SelectField
            {
                Alias = alias,
                Format = format,
                FormatArguments = args
            };
        }

        public static SelectField Create(string name, string alias, string sourceAlias, bool array)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias,
                SourceAlias = sourceAlias,
                SourceIsArray = array
            };
        }

        public static SelectField CreateGroupByAggregation(string name, string alias, AggregationOperation aggregation)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias,
                AggregationOperation = aggregation
            };
        }

        public static SelectField CreateGroupByKeyField(string alias, params string[] groupByKeys)
        {
            return new SelectField
            {
                Alias = alias,
                GroupByKeys = groupByKeys,
                IsGroupByKey = true
            };
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

            return new SelectField
            {
                Value = finalVal,
                Alias = alias,
                ValueTokenType = type
            };
        }
    }
}
