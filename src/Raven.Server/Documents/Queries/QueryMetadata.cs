using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Primitives;
using Org.BouncyCastle.Crypto;
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

        public QueryMetadata(string query, BlittableJsonReaderObject parameters, ulong cacheKey)
        {
            CacheKey = cacheKey;

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

            CanCache = cacheKey != 0;
        }

        public readonly bool IsDistinct;

        public readonly bool IsDynamic;

        public readonly bool IsGroupBy;

        public readonly string CollectionName;

        public readonly string IndexName;

        public string DynamicIndexName;

        public readonly Query Query;

        public readonly string QueryText;

        public readonly HashSet<string> IndexFieldNames = new HashSet<string>();

        public readonly Dictionary<string, WhereField> WhereFields = new Dictionary<string, WhereField>(StringComparer.OrdinalIgnoreCase);

        public string[] GroupBy;

        public (string Name, OrderByFieldType OrderingType, bool Ascending)[] OrderBy;

        public SelectField[] SelectFields;

        public readonly ulong CacheKey;

        public readonly bool CanCache;

        public string[] Includes;

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

            if (Query.With != null)
            {
                foreach (var with in Query.With)
                {
                    if (with.Expression.Type != OperatorType.Method)
                        ThrowInvalidWith(with, "WITH clause support only method calls, but got: ");
                    var method = QueryExpression.Extract(QueryText, with.Expression.Field);
                    if ("load".Equals(method, StringComparison.OrdinalIgnoreCase))
                    {
                        
                    }
                    else if ("include".Equals(method, StringComparison.OrdinalIgnoreCase))
                    {

                    }
                    else
                    {
                        ThrowInvalidWith(with, "WITH clause had an invalid method call, got: ");
                    }
                }
            }

            if (Query.Select != null)
                FillSelectFields(parameters);

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

        private void ThrowInvalidWith((QueryExpression Expression, FieldToken Alias) with, string msg)
        {
            var writer = new StringWriter();
            writer.Write(msg);
            with.Expression.ToString(QueryText, writer);
            throw new InvalidQueryException(writer.GetStringBuilder().ToString());
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

            if (IsGroupBy)
            {
                if (string.Equals("count", method, StringComparison.OrdinalIgnoreCase))
                {
                    if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                        return (CountFieldName, OrderByFieldType.Long, order.Ascending);

                    throw new InvalidQueryException("Invalid ORDER BY count() call, expected zero arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);
                }

                if (string.Equals("sum", method, StringComparison.OrdinalIgnoreCase))
                {
                    if (order.Expression.Arguments == null)
                        throw new InvalidQueryException("Invalid ORDER BY sum() call, expected one argument but didn't get any", QueryText, parameters);

                    if (order.Expression.Arguments.Count != 1)
                        throw new InvalidQueryException("Invalid ORDER BY sum() call, expected one argument, got " + order.Expression.Arguments.Count, QueryText, parameters);

                    var sumFieldToken = order.Expression.Arguments[0] as FieldToken;

                    var fieldName = QueryExpression.Extract(Query.QueryText, sumFieldToken);

                    var orderingType = order.FieldType;

                    if (orderingType == OrderByFieldType.Implicit)
                    {
                        if (WhereFields.TryGetValue(fieldName, out var whereField))
                        {
                            switch (whereField.Type)
                            {
                                case ValueTokenType.Double:
                                    orderingType = OrderByFieldType.Double;
                                    break;
                                case ValueTokenType.Long:
                                    orderingType = OrderByFieldType.Long;
                                    break;
                                default:
                                    throw new InvalidQueryException(
                                        $"Invalid query due to invalid value type of '{fieldName}' in WHERE clause, expected {nameof(ValueTokenType.Double)} or {nameof(ValueTokenType.Long)} because it's the argument of sum() function, got the value of {whereField.Type} type",
                                        QueryText, parameters);
                            }
                        }
                        else
                            orderingType = OrderByFieldType.Double;
                    }

                    return (fieldName, orderingType, order.Ascending);
                }
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

                string fieldName;

                var methodName = QueryExpression.Extract(_metadata.Query.QueryText, expression.Field);

                var methodType = QueryMethod.GetMethodType(methodName);

                switch (methodType)
                {
                    case MethodType.StartsWith:
                    case MethodType.EndsWith:
                    case MethodType.Search:
                    case MethodType.Lucene:
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName);

                        if (arguments.Count == 1)
                            throw new InvalidQueryException($"Method {methodName}() expects second argument to be provided", QueryText, parameters);

                        var valueToken = arguments[1] as ValueToken;

                        if (valueToken == null)
                            throw new InvalidQueryException($"Method {methodName}() expects value token as second argument, got {arguments[1]} type", QueryText, parameters);

                        var valueType = GetValueTokenType(parameters, valueToken, unwrapArrays: true);

                        if (methodType == MethodType.Search)
                            _metadata.AddSearchField(fieldName, valueType);
                        else
                            _metadata.AddWhereField(fieldName, valueType);
                        break;
                    case MethodType.Exists:
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName);
                        _metadata.AddExistField(fieldName);
                        break;
                    case MethodType.Boost:
                        var firstArg = arguments[0] as QueryExpression;

                        if (firstArg == null)
                            throw new InvalidQueryException($"Method {methodName}() expects expression , got {arguments[0]}", QueryText, parameters);

                        Visit(firstArg, parameters);
                        break;
                    case MethodType.Intersect:
                    case MethodType.Exact:
                        for (var i = 0; i < arguments.Count; i++)
                    {
                            var expressionArgument = arguments[i] as QueryExpression;
                        Visit(expressionArgument, parameters);
                    }
                        return;
                    case MethodType.Count:
                        HandleCount(methodName, expression.Field, arguments, parameters);
                        return;
                    case MethodType.Sum:
                        HandleSum(arguments, parameters);
                        return;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }
            }

            private void HandleCount(string providedCountMethodName, FieldToken methodNameToken, List<object> arguments, BlittableJsonReaderObject parameters)
                    {
                if (arguments.Count != 1)
                    throw new InvalidQueryException("Method count() expects no argument", QueryText, parameters);

                if (arguments[0] is QueryExpression countExpression)
                {
                    _metadata._aliasToName[providedCountMethodName] = CountFieldName;
                    countExpression.Field = methodNameToken;

                    Visit(countExpression, parameters);
                    }
                else
                    throw new InvalidQueryException($"Method count() expects expression after its invocation, got {arguments[0]}", QueryText, parameters);
            }

            private void HandleSum(List<object> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 2)
                    throw new InvalidQueryException("Method sum() expects one argument and operator after its invocation", QueryText, parameters);

                var fieldToken = arguments[0] as FieldToken;

                if (fieldToken == null)
                    throw new InvalidQueryException($"Method sum() expects first argument to be field token, got {arguments[0]}", QueryText, parameters);

                var sumExpression = arguments[1] as QueryExpression;

                if (sumExpression == null)
                    throw new InvalidQueryException($"Method sum() expects expression after sum(), got {arguments[1]}", QueryText, parameters);

                sumExpression.Field = fieldToken;
                Visit(sumExpression, parameters);
                }

            private string ExtractFieldNameFromFirstArgument(List<object> arguments, string methodName)
                {
                var fieldArgument = arguments[0] as FieldToken;

                if (fieldArgument == null)
                    throw new InvalidQueryException($"Method {methodName}() expects a field name as its first argument");

                return QueryExpression.Extract(_metadata.Query.QueryText, fieldArgument);
            }
        }
    }
}
