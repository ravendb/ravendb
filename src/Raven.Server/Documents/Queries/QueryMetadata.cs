using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadata
    {
        public QueryMetadata(string query, BlittableJsonReaderObject parameters)
        {
            var qp = new QueryParser();
            qp.Init(query);
            Query = qp.Parse();

            IsDynamic = Query.From.Index == false;

            var fromToken = Query.From.From;
            if (IsDynamic)
                CollectionName = QueryExpression.Extract(Query.QueryText, fromToken);
            else
                IndexName = QueryExpression.Extract(Query.QueryText, fromToken.TokenStart + 1, fromToken.TokenLength - 2, fromToken.EscapeChars);

            Build(parameters);
        }

        public readonly bool IsDynamic;

        public readonly string CollectionName;

        public readonly string IndexName;

        public readonly Query Query;

        public readonly HashSet<string> AllFieldNames = new HashSet<string>();

        public readonly Dictionary<string, ValueTokenType> Fields = new Dictionary<string, ValueTokenType>(StringComparer.OrdinalIgnoreCase);

        public List<(string Name, OrderByFieldType OrderingType, bool Ascending)> OrderBy;

        public void AddEmptyField(string fieldName)
        {
            AllFieldNames.Add(fieldName);
        }

        public void AddField(string fieldName, ValueTokenType value)
        {
            AllFieldNames.Add(fieldName);
            Fields[fieldName] = value;
        }

        private void Build(BlittableJsonReaderObject parameters)
        {
            if (Query.Where != null)
            {
                new FillFieldsAndParametersVisitor(this, Query.QueryText).Visit(Query.Where, parameters);
            }

            if (Query.OrderBy == null)
                return;

            OrderBy = new List<(string Name, OrderByFieldType OrderingType, bool Ascending)>(Query.OrderBy.Count);
            foreach (var fieldInfo in Query.OrderBy)
                OrderBy.Add((QueryExpression.Extract(Query.QueryText, fieldInfo.Field), fieldInfo.FieldType, fieldInfo.Ascending));
        }

        private static void ThrowIncompatibleTypesOfVariables(string fieldName, params ValueToken[] valueTokens)
        {
            throw new InvalidOperationException("Incompatible types of variables in WHERE clause");
            //TODO arek
            //throw new InvalidOperationException($"Incompatible types of variables in WHERE clause on '{ExtractFieldName(fieldName)}' field: " +
            //                                    $"{string.Join(",", valueTokens.Select(x => $"{ExtractTokenValue(x)}({x.Type})"))}");
        }

        private static void ThrowIncompatibleTypesOfParameters(string fieldName, params ValueToken[] valueTokens)
        {
            throw new InvalidOperationException("Incompatible types of parameters in WHERE clause");
            //TODO arek
            //throw new InvalidOperationException($"Incompatible types of variables in WHERE clause on '{ExtractFieldName(fieldName)}' field: " +
            //                                    $"{string.Join(",", valueTokens.Select(x => $"{ExtractTokenValue(x)}({x.Type})"))}");
        }

        private class FillFieldsAndParametersVisitor : WhereExpressionVisitor
        {
            private readonly QueryMetadata _metadata;

            public FillFieldsAndParametersVisitor(QueryMetadata metadata, string queryText) : base(queryText)
            {
                _metadata = metadata;
            }

            public override void VisitFieldToken(string fieldName, ValueToken value, BlittableJsonReaderObject parameters)
            {
                _metadata.AddField(fieldName, GetValueTokenType(parameters, value, unwrapArrays: false));
            }

            public override void VisitFieldTokens(string fieldName, ValueToken firstValue, ValueToken secondValue, BlittableJsonReaderObject parameters)
            {
                if (firstValue.Type != secondValue.Type)
                    ThrowIncompatibleTypesOfVariables(fieldName, firstValue, secondValue);

                var valueType1 = GetValueTokenType(parameters, firstValue, unwrapArrays: false);
                var valueType2 = GetValueTokenType(parameters, secondValue, unwrapArrays: false);

                if (valueType1 != valueType2)
                    ThrowIncompatibleTypesOfParameters(fieldName, firstValue, secondValue);

                _metadata.AddField(fieldName, valueType1);
            }

            public override void VisitFieldTokens(string fieldName, List<ValueToken> values, BlittableJsonReaderObject parameters)
            {
                if (values.Count == 0)
                    return;

                var previousType = ValueTokenType.Null;
                for (var i = 0; i < values.Count; i++)
                {
                    var value = values[i];
                    if (i > 0 && value.Type != values[i - 1].Type)
                        ThrowIncompatibleTypesOfVariables(fieldName, values.ToArray());

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: true);
                    if (i > 0 && previousType != valueType)
                        ThrowIncompatibleTypesOfParameters(fieldName, values.ToArray());

                    previousType = valueType;
                }

                _metadata.AddField(fieldName, previousType);
            }

            public override void VisitMethodTokens(QueryExpression expression, BlittableJsonReaderObject parameters)
            {
                var arguments = expression.Arguments;
                if (arguments.Count == 0)
                    return;

                var firstArgument = arguments[0];
                var firstArgumentAsExpression = firstArgument as QueryExpression;
                if (firstArgumentAsExpression != null)
                {
                    VisitMethodTokens(firstArgumentAsExpression, parameters);
                    return;
                }

                var firstArgumentAsField = (FieldToken)firstArgument;
                var fieldName = QueryExpression.Extract(_metadata.Query.QueryText, firstArgumentAsField);

                if (arguments.Count == 1)
                {
                    _metadata.AddEmptyField(fieldName);
                    return;
                }

                var previousType = ValueTokenType.Null;
                for (var i = 1; i < arguments.Count; i++)
                {
                    var value = (ValueToken)arguments[i];
                    if (i > 1 && value.Type != previousType)
                        ThrowIncompatibleTypesOfVariables(fieldName, arguments.Skip(1).Cast<ValueToken>().ToArray());

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: false);

                    if (i > 1 && previousType != valueType)
                        ThrowIncompatibleTypesOfParameters(fieldName, arguments.Skip(1).Cast<ValueToken>().ToArray());

                    previousType = valueType;
                }

                _metadata.AddField(fieldName, previousType);
            }
        }
    }
}