using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        public void AddField(string fieldName, ValueTokenType value)
        {
            AllFieldNames.Add(fieldName);
            Fields[fieldName] = value;
        }

        private void Build(BlittableJsonReaderObject parameters)
        {
            if (Query.Where != null)
                FillFieldsAndParameters(parameters, Query.Where);

            if (Query.OrderBy == null)
                return;

            OrderBy = new List<(string Name, OrderByFieldType OrderingType, bool Ascending)>(Query.OrderBy.Count);
            foreach (var fieldInfo in Query.OrderBy)
                OrderBy.Add((QueryExpression.Extract(Query.QueryText, fieldInfo.Field), fieldInfo.FieldType, fieldInfo.Ascending));
        }

        private void FillFieldsAndParameters(BlittableJsonReaderObject parameters, QueryExpression expression)
        {
            if (expression.Field == null)
            {
                FillFieldsAndParameters(parameters, expression.Left);
                FillFieldsAndParameters(parameters, expression.Right);
            }

            Debug.Assert(expression.Field != null);

            switch (expression.Type)
            {
                case OperatorType.Equal:
                case OperatorType.LessThen:
                case OperatorType.GreaterThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    FillFieldAndParameter(parameters, QueryExpression.Extract(Query.QueryText, expression.Field), expression.Value ?? expression.First);
                    return;
                case OperatorType.Between:
                    FillFieldAndParameters(parameters, QueryExpression.Extract(Query.QueryText, expression.Field), expression.First, expression.Second);
                    return;
                case OperatorType.In:
                    FillFieldAndParameters(parameters, QueryExpression.Extract(Query.QueryText, expression.Field), expression.Values);
                    return;
                default:
                    throw new ArgumentException(expression.Type.ToString());
            }
        }

        private void FillFieldAndParameter(BlittableJsonReaderObject parameters, string fieldName, ValueToken value)
        {
            AddField(fieldName, GetValueTokenType(parameters, value, unwrapArrays: false));
        }

        private void FillFieldAndParameters(BlittableJsonReaderObject parameters, string fieldName, List<ValueToken> values)
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

            AddField(fieldName, previousType);
        }

        private void FillFieldAndParameters(BlittableJsonReaderObject parameters, string fieldName, ValueToken firstValue, ValueToken secondValue)
        {
            if (firstValue.Type != secondValue.Type)
                ThrowIncompatibleTypesOfVariables(fieldName, firstValue, secondValue);

            var valueType1 = GetValueTokenType(parameters, firstValue, unwrapArrays: false);
            var valueType2 = GetValueTokenType(parameters, secondValue, unwrapArrays: false);

            if (valueType1 != valueType2)
                ThrowIncompatibleTypesOfParameters(fieldName, firstValue, secondValue);

            AddField(fieldName, valueType1);
        }

        private ValueTokenType GetValueTokenType(BlittableJsonReaderObject parameters, ValueToken value, bool unwrapArrays)
        {
            var valueType = value.Type;

            if (valueType == ValueTokenType.Parameter)
            {
                var parameterName = QueryExpression.Extract(Query.QueryText, value);

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                valueType = QueryBuilder.GetValueTokenType(parameterValue, unwrapArrays);
            }

            return valueType;
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
    }
}