using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadata
    {
        private const string CountFieldName = "Count";

        private readonly Dictionary<string, string> _aliasToName = new Dictionary<string, string>();

        public readonly Dictionary<StringSegment, (string PropertyPath, bool Array)> RootAliasPaths = new Dictionary<StringSegment, (string PropertyPath, bool Array)>();

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

        public OrderByField[] OrderBy;

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
            if (Query.From.Alias != null)
            {
                var fromAlias = QueryExpression.Extract(QueryText, Query.From.Alias);
                RootAliasPaths[fromAlias] = (null, false);
            }

            if (Query.GroupBy != null)
            {
                GroupBy = new string[Query.GroupBy.Count];

                for (var i = 0; i < Query.GroupBy.Count; i++)
                    GroupBy[i] = QueryExpression.Extract(QueryText, Query.GroupBy[i]);
            }

            if (Query.Load != null)
                HandleLoadClause(parameters);

            if (Query.SelectFunctionBody != null)
                HandleSelectFunctionBody(parameters);
            else if (Query.Select != null)
                FillSelectFields(parameters);
            if (Query.Where != null)
                new FillWhereFieldsAndParametersVisitor(this, QueryText).Visit(Query.Where, parameters);

            if (Query.OrderBy != null)
            {
                OrderBy = new OrderByField[Query.OrderBy.Count];

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
                            OrderBy[i] = new OrderByField(indexFieldName, order.FieldType, order.Ascending);
                            break;
                        default:
                            ThrowInvalidOperatorTypeInOrderBy(order.Expression.Type, QueryText, parameters);
                            break;
                    }
                }
            }
        }
        
        
        private void HandleSelectFunctionBody(BlittableJsonReaderObject parameters)
        {
            if (Query.Select != null && Query.Select.Count > 0)
                ThrowInvalidFunctionSelectWithMoreFields(parameters);

            if (RootAliasPaths.Count == 0)
                ThrowMissingAliasOnSelectFunctionBody(parameters);

            var name = "__selectOutput";
            if (Query.DeclaredFunctions != null &&
                Query.DeclaredFunctions.ContainsKey(name))
                ThrowUseOfReserveFunctionBodyMethodName(parameters);

            var sb = new StringBuilder();

            sb.Append("function ").Append(name).Append("(rvnQueryArgs");
            int index = 0;
            var args = new SelectField[RootAliasPaths.Count];

            foreach (var alias in RootAliasPaths)
            {
                sb.Append(", ");
                sb.Append(alias.Key);
                args[index++] = SelectField.Create(string.Empty, null, alias.Value.PropertyPath,
                    alias.Value.Array, true);
            }
            sb.AppendLine(") { ");
            foreach (var parameter in parameters.GetPropertyNames())
            {
                sb.Append("var $").Append(parameter).Append(" = rvnQueryArgs.").Append(parameter).AppendLine(";");
            }
            sb.Append("    return ");

            sb.Append(QueryExpression.Extract(Query.QueryText, Query.SelectFunctionBody));

            sb.AppendLine(";").AppendLine("}");

            if (Query.TryAddFunction(name, sb.ToString()) == false)
                ThrowUseOfReserveFunctionBodyMethodName(parameters);


            SelectFields = new[] {SelectField.CreateMethodCall(name, null, args)};
        }

        private void ThrowUseOfReserveFunctionBodyMethodName(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("When using select function body, the '__selectOutput' function is reserved",
                QueryText,parameters);
        }
        
        
        private void ThrowInvalidFunctionSelectWithMoreFields(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("A query can contain a single select function body without extra fields",QueryText, parameters);
        }

        private void ThrowMissingAliasOnSelectFunctionBody(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Use of select function body requires that aliases will be defined, but none had",
                QueryText,
                parameters);
        }

        private void HandleLoadClause(BlittableJsonReaderObject parameters)
        {
            string ParseIncludePath(QueryExpression expr,string path)
            {
                var indexOf = path.IndexOf('.');
                if (indexOf == -1)
                    return path;
                if (Query.From.Alias == null)
                    ThrowInvalidWith(expr, "LOAD clause is trying to use an alias but the from clause hasn't specified one: ", parameters);
                Debug.Assert(Query.From.Alias != null);
                if (Query.From.Alias.TokenLength != indexOf)
                    ThrowInvalidWith(expr, "LOAD clause is trying to use an alias that isn't specified in the from clause: ", parameters);
                var compare = string.Compare(
                    QueryText, Query.From.Alias.TokenStart,
                    path, 0, indexOf, StringComparison.OrdinalIgnoreCase);
                if (compare != 0)
                    ThrowInvalidWith(expr, "LOAD clause is trying to use an alias that isn't specified in the from clause: ", parameters);
                return path.Substring(indexOf + 1);
            }

            foreach (var load in Query.Load)
            {
                if (load.Alias == null)
                    ThrowInvalidWith(load.Expression, "LOAD clause requires an alias but got: ", parameters);

                var alias = QueryExpression.Extract(QueryText, load.Alias);

                string path;
                switch (load.Expression.Type)
                {
                        case OperatorType.Field:
                            path = QueryExpression.Extract(QueryText, load.Expression.Field);
                        break;
                        case OperatorType.Value:
                            path = QueryExpression.Extract(QueryText, load.Expression.Value);
                        break;
                    default:
                        ThrowInvalidWith(load.Expression, "LOAD clause require a field or value refereces", parameters);
                        path = null; // enver hit
                        break;
                }
                var array = false;
                if (alias.EndsWith("[]"))
                {
                    array = true;
                    alias = alias.Substring(0, alias.Length - 2);
                }
                path = ParseIncludePath(load.Expression, path);
                if (RootAliasPaths.TryAdd(alias, (path, array)) == false)
                {
                    ThrowInvalidWith(load.Expression, "LOAD clause duplicate alias detected: ", parameters);
                }
            }
        }

        private void ThrowInvalidWith(QueryExpression expr, string msg, BlittableJsonReaderObject parameters)
        {
            var writer = new StringWriter();
            writer.Write(msg);
            expr.ToString(QueryText, writer);
            throw new InvalidQueryException(writer.GetStringBuilder().ToString(), QueryText, parameters);
        }

        private OrderByField ExtractOrderByFromMethod((QueryExpression Expression, OrderByFieldType FieldType, bool Ascending) order, string method, BlittableJsonReaderObject parameters)
        {
            if (string.Equals("random", method, StringComparison.OrdinalIgnoreCase))
            {
                if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                    return new OrderByField(null, OrderByFieldType.Random, order.Ascending);

                if (order.Expression.Arguments.Count > 1)
                    throw new InvalidQueryException("Invalid ORDER BY 'random()' call, expected zero to one arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);

                var token = order.Expression.Arguments[0] as ValueToken;
                if (token == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'random()' call, expected value token , got " + order.Expression.Arguments[0], QueryText, parameters);

                var value = QueryExpression.Extract(QueryText, token);

                return new OrderByField(
                    null,
                    OrderByFieldType.Random,
                    order.Ascending,
                    null,
                    new[]
                    {
                        new OrderByField.Argument(value, ValueTokenType.String)
                    });
            }

            if (string.Equals("score", method, StringComparison.OrdinalIgnoreCase))
            {
                if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                    return new OrderByField(null, OrderByFieldType.Score, order.Ascending);

                throw new InvalidQueryException("Invalid ORDER BY 'score()' call, expected zero arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);
            }

            if (string.Equals("distance", method, StringComparison.OrdinalIgnoreCase))
            {
                if (order.Expression.Arguments.Count != 2)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected two arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);

                var fieldToken = order.Expression.Arguments[0] as FieldToken;
                if (fieldToken == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected field token, got " + order.Expression.Arguments[0], QueryText, parameters);

                var expression = order.Expression.Arguments[1] as QueryExpression;
                if (expression == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected expression, got " + order.Expression.Arguments[1], QueryText, parameters);

                var fieldName = QueryExpression.Extract(Query.QueryText, fieldToken);
                var methodName = QueryExpression.Extract(Query.QueryText, expression.Field);
                var methodType = QueryMethod.GetMethodType(methodName);

                switch (methodType)
                {
                    case MethodType.Circle:
                        QueryValidator.ValidateCircle(expression.Arguments, QueryText, parameters);
                        break;
                    case MethodType.Wkt:
                        QueryValidator.ValidateWkt(expression.Arguments, QueryText, parameters);
                        break;
                    case MethodType.Point:
                        QueryValidator.ValidatePoint(expression.Arguments, QueryText, parameters);
                        break;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }

                var arguments = new OrderByField.Argument[expression.Arguments.Count];
                for (var i = 0; i < expression.Arguments.Count; i++)
                {
                    var argument = (ValueToken)expression.Arguments[i];
                    var argumentValue = QueryExpression.Extract(Query.QueryText, argument);
                    arguments[i] = new OrderByField.Argument(argumentValue, argument.Type);
                }

                return new OrderByField(
                    fieldName,
                    OrderByFieldType.Distance,
                    order.Ascending,
                    methodType,
                    arguments);
            }

            if (IsGroupBy)
            {
                if (string.Equals("count", method, StringComparison.OrdinalIgnoreCase))
                {
                    if (order.Expression.Arguments == null || order.Expression.Arguments.Count == 0)
                        return new OrderByField(CountFieldName, OrderByFieldType.Long, order.Ascending);

                    throw new InvalidQueryException("Invalid ORDER BY 'count()' call, expected zero arguments, got " + order.Expression.Arguments.Count, QueryText, parameters);
                }

                if (string.Equals("sum", method, StringComparison.OrdinalIgnoreCase))
                {
                    if (order.Expression.Arguments == null)
                        throw new InvalidQueryException("Invalid ORDER BY 'sum()' call, expected one argument but didn't get any", QueryText, parameters);

                    if (order.Expression.Arguments.Count != 1)
                        throw new InvalidQueryException("Invalid ORDER BY 'sum()' call, expected one argument, got " + order.Expression.Arguments.Count, QueryText, parameters);

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

                    return new OrderByField(fieldName, orderingType, order.Ascending);
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

                var selectField = GetSelectField(parameters, expression, alias);
                fields.Add(selectField);
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

        private SelectField GetSelectField(BlittableJsonReaderObject parameters, QueryExpression expression, string alias)
        {
            switch (expression.Type)
            {
                case OperatorType.Value:
                    return GetSelectValue(alias, expression.Value);
                case OperatorType.Field:
                    return GetSelectValue(alias, expression.Field);
                case OperatorType.Method:
                    var methodName = QueryExpression.Extract(QueryText, expression.Field);
                    if (Enum.TryParse(methodName, ignoreCase: true, result: out AggregationOperation aggregation) == false)
                    {
                        if (Query.DeclaredFunctions != null &&
                            Query.DeclaredFunctions.TryGetValue(methodName, out var funcToken))
                        {
                            var args = new SelectField[expression.Arguments.Count];
                            for (int i = 0; i < expression.Arguments.Count; i++)
                            {
                                if (expression.Arguments[i] is QueryExpression argExpr)
                                    args[i] = GetSelectField(parameters, argExpr, null);
                                else if (expression.Arguments[i] is ValueToken vt)
                                    args[i] = GetSelectValue(null, vt);
                                else if (expression.Arguments[i] is FieldToken ft)
                                    args[i] = GetSelectValue(null, ft);
                                else
                                    ThrowInvalidMethodArgument(parameters);
                            }

                            return SelectField.CreateMethodCall(methodName, alias, args);
                        }

                        if (IsGroupBy == false)
                            ThrowUnknownMethodInSelect(methodName, QueryText, parameters);

                        switch (methodName)
                        {
                            case "key":
                                return SelectField.CreateGroupByKeyField(alias, GroupBy);
                            default:
                                ThrowUnknownAggregationMethodInSelectOfGroupByQuery(methodName, QueryText, parameters);
                                return null; // never hit
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

                        return SelectField.CreateGroupByAggregation(fieldName, alias, aggregation);
                    }

                default:
                    ThrowUnhandledExpressionTypeInSelect(expression.Type, QueryText, parameters);
                    return null;// never hit
            }
        }
        
        private void ThrowInvalidMethodArgument(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Invalid method parameter, don't know how to handle it", QueryText, parameters);
        }
        
        private SelectField GetSelectValue(string alias, FieldToken expressionField)
        {
            var name = QueryExpression.Extract(QueryText, expressionField);
            var indexOf = name.IndexOf('.');
            (string Path, bool Array) sourceAlias;

            bool hasSourceAlias = false;
            bool array = false;
            if (indexOf != -1)
            {
                var key = new StringSegment(name, 0, indexOf);
                if (key.Length > 2 && key[key.Length - 1] == ']' && key[key.Length - 2] == '[')
                {
                    key = key.Subsegment(0, key.Length - 2);
                    array = true;
                }
                if (RootAliasPaths.TryGetValue(key, out sourceAlias))
                {
                    name = name.Substring(indexOf + 1);
                    hasSourceAlias = true;
                    array = sourceAlias.Array;
                }
                else if(RootAliasPaths.Count != 0)
                {
                    throw new InvalidOperationException($"Unknown alias {key}, but there are aliases specified in the query ({string.Join(", ", RootAliasPaths.Keys)})");
                }
            }
            else if (RootAliasPaths.TryGetValue(name, out sourceAlias))
            {
                hasSourceAlias = true;
                if (string.IsNullOrEmpty(alias))
                    alias = name;
                array = sourceAlias.Array;
                name = string.Empty;
            }
            return SelectField.Create(name, alias, sourceAlias.Path, array, hasSourceAlias);
        }

        private SelectField GetSelectValue(string alias, ValueToken expressionValue)
        {
            var val = QueryExpression.Extract(QueryText, expressionValue);
            return SelectField.CreateValue(val, alias, expressionValue.Type);
        }

        public string GetIndexFieldName(string fieldNameOrAlias)
        {
            if (_aliasToName.TryGetValue(fieldNameOrAlias, out var indexFieldName))
                return indexFieldName;

            var indexOf = fieldNameOrAlias.IndexOf('.');
            if (indexOf == -1)
                return fieldNameOrAlias;

            var key = new StringSegment(fieldNameOrAlias, 0, indexOf);
          
            if (RootAliasPaths.TryGetValue(key, out _))
            {
                return fieldNameOrAlias.Substring(indexOf + 1);
            }

            if (RootAliasPaths.Count != 0)
            {
                throw new InvalidOperationException($"Unknown alias {key}, but there are aliases specified in the query ({string.Join(", ", RootAliasPaths.Keys)})");
            }

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

        private static void ThrowUnknownMethodInSelect(string methodName, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unknown method call in SELECT clause: '{methodName}' method", queryText, parameters);
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
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);

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
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);
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
                    case MethodType.Within:
                    case MethodType.Contains:
                    case MethodType.Disjoint:
                    case MethodType.Intersects:
                        HandleSpatial(methodName, arguments, parameters);
                        return;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }
            }

            private void HandleSpatial(string methodName, List<object> arguments, BlittableJsonReaderObject parameters)
            {
                var fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);

                if (arguments.Count < 2 || arguments.Count > 3)
                    throw new InvalidQueryException($"Method {methodName}() expects 2-3 arguments to be provided", QueryText, parameters);

                var shapeExpression = arguments[1] as QueryExpression;

                if (shapeExpression == null)
                    throw new InvalidQueryException($"Method {methodName}() expects expression as second argument, got {arguments[1]} type", QueryText, parameters);

                if (arguments.Count == 3)
                {
                    var valueToken = arguments[2] as ValueToken;

                    if (valueToken == null)
                        throw new InvalidQueryException($"Method {methodName}() expects value token as third argument, got {arguments[1]} type", QueryText, parameters);
                }

                methodName = QueryExpression.Extract(_metadata.Query.QueryText, shapeExpression.Field);

                var methodType = QueryMethod.GetMethodType(methodName);
                switch (methodType)
                {
                    case MethodType.Circle:
                        QueryValidator.ValidateCircle(shapeExpression.Arguments, QueryText, parameters);
                        break;
                    case MethodType.Wkt:
                        QueryValidator.ValidateWkt(shapeExpression.Arguments, QueryText, parameters);
                        break;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }

                _metadata.AddWhereField(fieldName, ValueTokenType.Null);
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

            private string ExtractFieldNameFromFirstArgument(List<object> arguments, string methodName, BlittableJsonReaderObject parameters)
            {
                var fieldArgument = arguments[0] as FieldToken;

                if (fieldArgument == null)
                    throw new InvalidQueryException($"Method {methodName}() expects a field name as its first argument",QueryText, parameters);

                return QueryExpression.Extract(_metadata.Query.QueryText, fieldArgument);
            }
        }
    }
}
