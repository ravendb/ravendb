using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public ValueTokenType? ValueTokenType;

        public string Name;

        public string Alias;

        public object Value;

        public string SourceAlias;

        public AggregationOperation AggregationOperation;

        public bool IsGroupByKey;

        public string[] GroupByKeys;

        public string Function;

        public bool SourceIsArray;

        public SelectField[] FunctionArgs;

        public bool HasSourceAlias;

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

        public static SelectField Create(string name, string alias, string sourceAlias, bool array, bool hasSourceAlias)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias,
                SourceAlias = sourceAlias,
                SourceIsArray = array,
                HasSourceAlias = hasSourceAlias
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

        public static SelectField CreateMethodCall(string methodName, string alias, SelectField[] args)
        {
            return new SelectField
            {
                Alias = alias,
                Name = methodName,
                Function = methodName,
                FunctionArgs = args
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
