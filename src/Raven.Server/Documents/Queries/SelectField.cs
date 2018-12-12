using System;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public ValueTokenType? ValueTokenType;

        public QueryFieldName Name;

        public string Alias;

        public object Value;

        public string SourceAlias;

        public bool IsEdge;

        public bool IsVertex;

        public bool IsParameter;

        public bool IsQuoted;

        public string LoadFromAlias;

        public AggregationOperation AggregationOperation;

        public bool IsGroupByKey;

        public GroupByField[] GroupByKeys;

        public string[] GroupByKeyNames;

        public string Function;

        public bool SourceIsArray;

        public SelectField[] FunctionArgs;

        public bool HasSourceAlias;

        public bool IsFacet;

        public bool IsSuggest;

        public bool IsCounter;

        public FieldExpression ExpressionField;

        protected SelectField()
        {

        }

        public static SelectField Create(QueryFieldName name, string alias = null)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias
            };
        }

        public static SelectField Create(
            QueryFieldName name, 
            string alias, 
            string sourceAlias, 
            bool array, 
            bool hasSourceAlias, 
            bool isParameter = false, 
            bool isQuoted = false, 
            string loadFromAlias = null)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias,
                SourceAlias = sourceAlias,
                SourceIsArray = array,
                HasSourceAlias = hasSourceAlias,
                IsParameter = isParameter,
                IsQuoted = isQuoted,
                LoadFromAlias = loadFromAlias
            };
        }

        public static SelectField CreateGroupByAggregation(QueryFieldName name, string alias, AggregationOperation aggregation)
        {
            return new SelectField
            {
                Name = name,
                Alias = alias,
                AggregationOperation = aggregation
            };
        }

        public static SelectField CreateGroupByKeyField(string alias, params GroupByField[] groupByKeys)
        {
            return new SelectField
            {
                Alias = alias,
                GroupByKeys = groupByKeys,
                GroupByKeyNames = groupByKeys.Select(x => x.Name.Value).ToArray(),
                IsGroupByKey = true
            };
        }

        public static SelectField CreateMethodCall(string methodName, string alias, SelectField[] args)
        {
            return new SelectField
            {
                Alias = alias,
                Name = new QueryFieldName(methodName, false),
                Function = methodName,
                FunctionArgs = args
            };
        }

        public static SelectField CreateCounterField(string alias, SelectField[] args)
        {
            string sourceAlias = null;
            if (args.Length == 2)
            {
                sourceAlias = args[0].SourceAlias;
            }

            return new SelectField
            {
                Alias = alias,
                Name = new QueryFieldName(args[args.Length -1].Value?.ToString() ?? 
                                          args[args.Length -1].Name?.Value, false),
                IsCounter = true,
                SourceAlias = sourceAlias,
                HasSourceAlias = sourceAlias != null,
                IsParameter = args[args.Length - 1].ValueTokenType == AST.ValueTokenType.Parameter,

            };
        }

        public static SelectField CreateValue(string val, string alias, ValueTokenType type)
        {
            object finalVal = val;
            switch (type)
            {
                case AST.ValueTokenType.Long:
                    finalVal = QueryBuilder.ParseInt64WithSeparators(val);
                    break;
                case AST.ValueTokenType.Double:
                    finalVal = double.Parse(val, CultureInfo.InvariantCulture);
                    break;
                case AST.ValueTokenType.True:
                    finalVal = true;
                    break;
                case AST.ValueTokenType.False:
                    finalVal = false;
                    break;
                case AST.ValueTokenType.Null:
                    finalVal = null;
                    break;
            }

            return new SelectField
            {
                Value = finalVal,
                Alias = alias ?? val,
                ValueTokenType = type
            };
        }
    }
}
