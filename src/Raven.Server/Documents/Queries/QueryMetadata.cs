using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadata
    {
        private const string CountFieldName = "Count";

        private readonly Dictionary<string, string> _aliasToName = new Dictionary<string, string>();

        public QueryMetadata(string query, BlittableJsonReaderObject parameters)
        {
            var qp = new QueryParser();
            qp.Init(query);
            Query = qp.Parse();

            QueryText = Query.QueryText;

            IsDynamic = Query.From.Index == false;
            IsDistinct = Query.IsDistinct;
            IsGroupBy = Query.GroupBy != null;

            var fromToken = Query.From.From;

            if (IsDynamic)
                CollectionName = QueryExpression.Extract(Query.QueryText, fromToken);
            else
                IndexName = QueryExpression.Extract(Query.QueryText, fromToken);

            Build(parameters);

            CanCache = true;
            foreach (var kvp in WhereFields)
            {
                if (kvp.Value.Type != ValueTokenType.Null)
                    continue;

                CanCache = false;
                break;
            }
        }

        public readonly bool IsDistinct;

        public readonly bool IsDynamic;

        public readonly bool IsGroupBy;

        public readonly string CollectionName;

        public readonly string IndexName;

        public readonly Query Query;

        public readonly string QueryText;

        public readonly HashSet<string> IndexFieldNames = new HashSet<string>();

        public readonly Dictionary<string, WhereField> WhereFields = new Dictionary<string, WhereField>(StringComparer.OrdinalIgnoreCase);

        public string[] GroupBy;

        public (string Name, OrderByFieldType OrderingType, bool Ascending)[] OrderBy;

        public SelectField[] SelectFields;

        public readonly bool CanCache;

        private void AddSearchField(string fieldName, ValueTokenType value)
        {
            var indexFieldName = GetIndexFieldName(fieldName);

            IndexFieldNames.Add(indexFieldName);
            WhereFields[indexFieldName] = new WhereField(value, isFullTextSearch: true);
        }

        private void AddExistField(string fieldName)
        {
            IndexFieldNames.Add(GetIndexFieldName(fieldName));
        }

        private void AddWhereField(string fieldName, ValueTokenType value)
        {
            var indexFieldName = GetIndexFieldName(fieldName);

            IndexFieldNames.Add(indexFieldName);
            WhereFields[indexFieldName] = new WhereField(value, isFullTextSearch: false);
        }

        private void Build(BlittableJsonReaderObject parameters)
        {
            if (Query.GroupBy != null)
            {
                GroupBy = new string[Query.GroupBy.Count];

                for (var i = 0; i < Query.GroupBy.Count; i++)
                    GroupBy[i] = QueryExpression.Extract(QueryText, Query.GroupBy[i]);
            }

            if (Query.Select != null)
                FillSelectFields(parameters);
            else
            {
                if (IsGroupBy)
                    ThrowMissingSelectClauseInGroupByQuery(QueryText, parameters);
            }

            if (Query.Where != null)
                new FillWhereFieldsAndParametersVisitor(this, QueryText).Visit(Query.Where, parameters);

            if (Query.OrderBy != null)
            {
                OrderBy = new(string Name, OrderByFieldType OrderingType, bool Ascending)[Query.OrderBy.Count];

                for (var i = 0; i < Query.OrderBy.Count; i++)
                {
                    var order = Query.OrderBy[i];
                    var indexFieldName = GetIndexFieldName(QueryExpression.Extract(QueryText, order.Expression.Field));

                    switch (order.Expression.Type)
                    {
                        case OperatorType.Method:
                            OrderBy[i] = ExtractOrderByFromMethod(order, indexFieldName, parameters);
                            break;
                        case OperatorType.Field:
                            OrderBy[i] = (indexFieldName, order.FieldType, order.Ascending);
                            break;
                        default:
                            ThrowInvalidOperatorTypeInOrderBy(order.Expression.Type, QueryText, parameters);
                            break;
                    }
                }
            }
        }

        private (string Name, OrderByFieldType OrderingType, bool Ascending) ExtractOrderByFromMethod(
            (QueryExpression Expression, OrderByFieldType FieldType, bool Ascending) order, string method, BlittableJsonReaderObject parameters)
        {
            if (string.Equals("random", method, StringComparison.OrdinalIgnoreCase))
            {
                if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                    return (null, OrderByFieldType.Random, order.Ascending);

                if (order.Expression.Arguments.Count > 1)
                    throw new InvalidQueryException("Invalid ORDER BY random call, expected zero to one arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);

                var token = order.Expression.Arguments[0] as ValueToken;
                if (token == null)
                    throw new InvalidQueryException("Invalid ORDER BY random call, expected value token , got " + order.Expression.Arguments[0], QueryText, parameters);

                var arg = QueryExpression.Extract(QueryText, token);

                return (arg, OrderByFieldType.Random, order.Ascending);
            }

            if (string.Equals("score", method, StringComparison.OrdinalIgnoreCase))
            {
                if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                    return (null, OrderByFieldType.Score, order.Ascending);

                throw new InvalidQueryException("Invalid ORDER BY score call, expected zero arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);
            }

            throw new InvalidQueryException("Invalid ORDER BY method call " + method, QueryText, parameters);
        }

        private void FillSelectFields(BlittableJsonReaderObject parameters)
        {
            var fields = new List<SelectField>(Query.Select.Count);

            foreach (var fieldInfo in Query.Select)
            {
                string alias = null;

                if (fieldInfo.Alias != null)
                    alias = QueryExpression.Extract(QueryText, fieldInfo.Alias);

                var expression = fieldInfo.Expression;

                switch (expression.Type)
                {
                    case OperatorType.Field:
                        var name = QueryExpression.Extract(QueryText, expression.Field);
                        fields.Add(SelectField.Create(name, alias));
                        break;
                    case OperatorType.Method:
                        var methodName = QueryExpression.Extract(QueryText, expression.Field);

                        if (IsGroupBy == false)
                            ThrowMethodsAreNotSupportedInSelect(methodName, QueryText, parameters);

                        if (Enum.TryParse(methodName, true, out AggregationOperation aggregation) == false)
                        {
                            switch (methodName)
                            {
                                case "key":
                                    fields.Add(SelectField.CreateGroupByKeyField(alias, GroupBy));
                                    break;
                                default:
                                    ThrowUnknownAggregationMethodInSelectOfGroupByQuery(methodName, QueryText, parameters);
                                    break;
                            }
                        }
                        else
                        {
                            string fieldName = null;

                            switch (aggregation)
                            {
                                case AggregationOperation.Count:
                                    fieldName = CountFieldName;
                                    break;
                                case AggregationOperation.Sum:
                                    if (expression.Arguments == null)
                                        ThrowMissingFieldNameArgumentOfSumMethod(QueryText, parameters);
                                    if (expression.Arguments.Count != 1)
                                        ThrowIncorrectNumberOfArgumentsOfSumMethod(expression.Arguments.Count, QueryText, parameters);

                                    var sumFieldToken = expression.Arguments[0] as FieldToken;

                                    fieldName = QueryExpression.Extract(Query.QueryText, sumFieldToken);
                                    break;
                            }

                            Debug.Assert(fieldName != null);

                            fields.Add(SelectField.CreateGroupByAggregation(fieldName, alias, aggregation));
                        }

                        break;
                    default:
                        ThrowUnhandledExpressionTypeInSelect(expression.Type, QueryText, parameters);
                        break;
                }
            }

            SelectFields = new SelectField[fields.Count];

            for (var i = 0; i < fields.Count; i++)
            {
                var field = fields[i];

                SelectFields[i] = field;

                if (field.Alias != null)
                {
                    if (field.IsGroupByKey == false)
                        _aliasToName[field.Alias] = field.Name;
                    else
                    {
                        if (field.GroupByKeys.Length == 1)
                            _aliasToName[field.Alias] = field.GroupByKeys[0];
                    }
                }
            }
        }

        public string GetIndexFieldName(string fieldNameOrAlias)
        {
            if (_aliasToName.TryGetValue(fieldNameOrAlias, out var indexFieldName))
                return indexFieldName;

            return fieldNameOrAlias;
        }

        private static void ThrowIncompatibleTypesOfVariables(string fieldName, string queryText, BlittableJsonReaderObject parameters, params ValueToken[] valueTokens)
        {
            throw new InvalidQueryException($"Incompatible types of variables in WHERE clause on '{fieldName}' field. It got values of the following types: " +
                                                $"{string.Join(",", valueTokens.Select(x => x.Type.ToString()))}", queryText, parameters);
        }

        private static void ThrowIncompatibleTypesOfParameters(string fieldName, string queryText, BlittableJsonReaderObject parameters, params ValueToken[] valueTokens)
        {
            throw new InvalidQueryException($"Incompatible types of parameters in WHERE clause on '{fieldName}' field. It got parameters of the following types:   " +
                                                $"{string.Join(",", valueTokens.Select(x => x.Type.ToString()))}", queryText, parameters);
        }

        private static void ThrowUnknownAggregationMethodInSelectOfGroupByQuery(string methodName, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unknown aggregation method in SELECT clause of the group by query: '{methodName}'", queryText, parameters);
        }

        private static void ThrowMissingFieldNameArgumentOfSumMethod(string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Missing argument of sum() method. You need to specify the name of a field e.g. sum(Age)", queryText, parameters);
        }

        private static void ThrowIncorrectNumberOfArgumentsOfSumMethod(int count, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"sum() method expects exactly one argument but got {count}", queryText, parameters);
        }

        private static void ThrowMethodsAreNotSupportedInSelect(string methodName, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Method calls are not supported in SELECT clause while you tried to use '{methodName}' method", queryText, parameters);
        }

        private static void ThrowUnhandledExpressionTypeInSelect(OperatorType expressionType, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unhandled expression of type {expressionType} in SELECT clause", queryText, parameters);
        }

        private static void ThrowMissingSelectClauseInGroupByQuery(string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Query having GROUP BY needs to have at least one aggregation operation defined in SELECT such as count() or sum()", queryText, parameters);
        }

        private static void ThrowInvalidOperatorTypeInOrderBy(OperatorType type, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Invalid type of operator in ORDER BY clause. Operator: {type}", queryText, parameters);
        }

        private class FillWhereFieldsAndParametersVisitor : WhereExpressionVisitor
        {
            private readonly QueryMetadata _metadata;

            public FillWhereFieldsAndParametersVisitor(QueryMetadata metadata, string queryText) : base(queryText)
            {
                _metadata = metadata;
            }

            public override void VisitFieldToken(string fieldName, ValueToken value, BlittableJsonReaderObject parameters)
            {
                _metadata.AddWhereField(fieldName, GetValueTokenType(parameters, value, unwrapArrays: false));
            }

            public override void VisitFieldTokens(string fieldName, ValueToken firstValue, ValueToken secondValue, BlittableJsonReaderObject parameters)
            {
                if (firstValue.Type != secondValue.Type)
                    ThrowIncompatibleTypesOfVariables(fieldName, QueryText, parameters, firstValue, secondValue);

                var valueType1 = GetValueTokenType(parameters, firstValue, unwrapArrays: false);
                var valueType2 = GetValueTokenType(parameters, secondValue, unwrapArrays: false);

                if (QueryBuilder.AreValueTokenTypesValid(valueType1, valueType2) == false)
                    ThrowIncompatibleTypesOfParameters(fieldName, QueryText, parameters, firstValue, secondValue);

                _metadata.AddWhereField(fieldName, valueType1);
            }

            public override void VisitFieldTokens(string fieldName, List<ValueToken> values, BlittableJsonReaderObject parameters)
            {
                if (values.Count == 0)
                    return;

                var previousType = ValueTokenType.Null;
                for (var i = 0; i < values.Count; i++)
                {
                    var value = values[i];
                    if (i > 0)
                    {
                        var previousValue = values[i - 1];

                        if (QueryBuilder.AreValueTokenTypesValid(previousValue.Type, value.Type) == false)
                            ThrowIncompatibleTypesOfVariables(fieldName, QueryText, parameters, values.ToArray());
                    }

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: true);
                    if (i > 0 && QueryBuilder.AreValueTokenTypesValid(previousType, valueType) == false)
                        ThrowIncompatibleTypesOfParameters(fieldName, QueryText, parameters, values.ToArray());

                    if (valueType != ValueTokenType.Null)
                        previousType = valueType;
                }

                _metadata.AddWhereField(fieldName, previousType);
            }

            public override void VisitMethodTokens(QueryExpression expression, BlittableJsonReaderObject parameters)
            {
                var arguments = expression.Arguments;
                if (arguments.Count == 0)
                    return;

                string fieldName = null;
                var previousType = ValueTokenType.Null;

                for (var i = 0; i < arguments.Count; i++)
                {
                    var argument = arguments[i];

                    if (argument is QueryExpression expressionArgument)
                    {
                        Visit(expressionArgument, parameters);
                        continue;
                    }

                    if (i == 0)
                    {
                        if (argument is FieldToken fieldTokenArgument)
                            fieldName = QueryExpression.Extract(_metadata.Query.QueryText, fieldTokenArgument);

                        continue;
                    }

                    if (arguments.Count == 3 && i == 2 && argument is FieldToken)
                        continue; // e.g. search(FieldName, 'sth', AND)

                    // validation of parameters

                    var value = (ValueToken)argument;

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: true);
                    if (i > 0 && QueryBuilder.AreValueTokenTypesValid(previousType, valueType) == false)
                        ThrowIncompatibleTypesOfParameters(fieldName, QueryText, parameters, arguments.Skip(1).Cast<ValueToken>().ToArray());

                    if (valueType != ValueTokenType.Null)
                        previousType = valueType;
                }

                if (fieldName == null)
                {
                    // we can have null field name here e.g. boost(search(Tags, :p1), 20), intersect(Age > 20, Name = 'Joe')
                    return;
                }

                var methodName = QueryExpression.Extract(_metadata.Query.QueryText, expression.Field);

                if("exists".Equals(methodName, StringComparison.OrdinalIgnoreCase))
                    _metadata.AddExistField(fieldName);
                else if ("search".Equals(methodName, StringComparison.OrdinalIgnoreCase))
                    _metadata.AddSearchField(fieldName, previousType);
                else
                    _metadata.AddWhereField(fieldName, previousType);
            }
        }

    }
}