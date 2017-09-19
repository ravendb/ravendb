using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadata
    {
        private const string CountFieldName = "Count";

        private readonly Dictionary<string, string> _aliasToName = new Dictionary<string, string>();

        public readonly Dictionary<StringSegment, (string PropertyPath, bool Array)> RootAliasPaths = new Dictionary<StringSegment, (string PropertyPath, bool Array)>();

        public QueryMetadata(string query, BlittableJsonReaderObject parameters, ulong cacheKey, QueryType queryType = QueryType.Select)
        {
            CacheKey = cacheKey;

            var qp = new QueryParser();
            qp.Init(query);
            Query = qp.Parse(queryType);

            QueryText = Query.QueryText;

            IsDynamic = Query.From.Index == false;
            IsDistinct = Query.IsDistinct;
            IsGroupBy = Query.GroupBy != null;

            var fromToken = Query.From.From;

            if (IsDynamic)
                CollectionName = fromToken.Field;
            else
                IndexName = fromToken.Field;

            if (IsDynamic == false || IsGroupBy)
                IsCollectionQuery = false;

            Build(parameters);

            CanCache = cacheKey != 0;
        }

        public readonly bool IsDistinct;

        public readonly bool IsDynamic;

        public readonly bool IsGroupBy;

        public bool IsIntersect { get; private set; }

        public bool IsCollectionQuery { get; private set; } = true;

        public readonly string CollectionName;

        public readonly string IndexName;

        public string DynamicIndexName;

        public readonly Query Query;

        public readonly string QueryText;

        public readonly HashSet<string> IndexFieldNames = new HashSet<string>(StringComparer.Ordinal);

        public readonly Dictionary<string, WhereField> WhereFields = new Dictionary<string, WhereField>(StringComparer.Ordinal);

        public string[] GroupBy;

        public OrderByField[] OrderBy;

        public SelectField[] SelectFields;

        public readonly ulong CacheKey;

        public readonly bool CanCache;

        public string[] Includes;

        private void AddExistField(string fieldName)
        {
            IndexFieldNames.Add(GetIndexFieldName(fieldName));
            IsCollectionQuery = false;
        }

        private void AddWhereField(string fieldName, bool search = false, bool exact = false)
        {
            var indexFieldName = GetIndexFieldName(fieldName);

            if (IsCollectionQuery && indexFieldName != Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                IsCollectionQuery = false;

            IndexFieldNames.Add(indexFieldName);
            WhereFields[indexFieldName] = new WhereField(isFullTextSearch: search, isExactSearch: exact);
        }

        private void Build(BlittableJsonReaderObject parameters)
        {
            string fromAlias = null;
            if (Query.From.Alias != null)
            {
                fromAlias = Query.From.Alias;
                RootAliasPaths[fromAlias] = (null, false);
            }

            if (Query.GroupBy != null)
            {
                GroupBy = new string[Query.GroupBy.Count];

                for (var i = 0; i < Query.GroupBy.Count; i++)
                {
                    var name = Query.GroupBy[i].Field;

                    EnsureValidGroupByField(name, parameters);

                    GroupBy[i] = name;
                }
            }

            if (Query.Load != null)
                HandleLoadClause(parameters);

            if (Query.SelectFunctionBody != null)
                HandleSelectFunctionBody(parameters);
            else if (Query.Select != null)
                FillSelectFields(parameters);
            if (Query.Where != null)
                new FillWhereFieldsAndParametersVisitor(this, fromAlias, QueryText).Visit(Query.Where, parameters);

            if (Query.OrderBy != null)
            {
                OrderBy = new OrderByField[Query.OrderBy.Count];

                for (var i = 0; i < Query.OrderBy.Count; i++)
                {
                    var order = Query.OrderBy[i];
                    if (order.Expression is MethodExpression me)
                    {
                        OrderBy[i] = ExtractOrderByFromMethod(me, order.FieldType, order.Ascending, parameters);
                    }
                    else if (order.Expression is FieldExpression fe)
                    {
                        OrderBy[i] = new OrderByField(GetIndexFieldName(fe.Field), order.FieldType, order.Ascending);
                    }
                    else
                    {
                        ThrowInvalidOperatorTypeInOrderBy(order.Expression.Type.ToString(), QueryText, parameters);
                    }

                    if (IsCollectionQuery && (OrderBy.Length > 1 || OrderBy[0].OrderingType != OrderByFieldType.Random))
                        IsCollectionQuery = false;
                }
            }

            if (Query.Include != null)
                HandleQueryInclude(parameters);
        }

        private void HandleQueryInclude(BlittableJsonReaderObject parameters)
        {
            var includes = new List<string>();
            foreach (var include in Query.Include)
            {
                string path;

                if (include is FieldExpression fe)
                {
                    path = fe.Field;
                }
                else if (include is ValueExpression ve)
                {
                    path = ve.Token;
                }
                else
                {
                    throw new InvalidOperationException("Unable to figure out how to deal with include of type " + include.Type);
                }

                var expressionPath = ParseExpressionPath(include, path, parameters);
                includes.Add(expressionPath);
            }
            Includes = includes.ToArray();
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

            sb.Append("function ").Append(name).Append("(");
            int index = 0;
            var args = new SelectField[RootAliasPaths.Count];

            foreach (var alias in RootAliasPaths)
            {
                if (index != 0)
                    sb.Append(", ");
                sb.Append(alias.Key);
                args[index++] = SelectField.Create(string.Empty, null, alias.Value.PropertyPath,
                    alias.Value.Array, true);
            }
            if (index != 0)
                sb.Append(", ");
            sb.AppendLine("rvnQueryArgs) { ");
            if (parameters != null)
            {
                foreach (var parameter in parameters.GetPropertyNames())
                {
                    sb.Append("var $").Append(parameter).Append(" = rvnQueryArgs.").Append(parameter).AppendLine(";");
                }
            }
            sb.Append("    return ");

            sb.Append(Query.SelectFunctionBody);

            sb.AppendLine(";").AppendLine("}");

            if (Query.TryAddFunction(name, sb.ToString()) == false)
                ThrowUseOfReserveFunctionBodyMethodName(parameters);


            SelectFields = new[] {SelectField.CreateMethodCall(name, null, args)};
        }

        private void ThrowUseOfReserveFunctionBodyMethodName(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("When using select function body, the '__selectOutput' function is reserved",
                QueryText, parameters);
        }

        private void ThrowInvalidFunctionSelectWithMoreFields(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("A query can contain a single select function body without extra fields", QueryText, parameters);
        }

        private void ThrowMissingAliasOnSelectFunctionBody(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Use of select function body requires that aliases will be defined, but none had",
                QueryText,
                parameters);
        }

        private string ParseExpressionPath(QueryExpression expr, string path, BlittableJsonReaderObject parameters)
        {
            var indexOf = path.IndexOf('.');
            if (indexOf == -1)
                return path;
            if (Query.From.Alias == null)
                ThrowInvalidWith(expr, "LOAD clause is trying to use an alias but the from clause hasn't specified one: ", parameters);
            Debug.Assert(Query.From.Alias != null);
            if (Query.From.Alias.Value.Length != indexOf)
                ThrowInvalidWith(expr, "LOAD clause is trying to use an alias that isn't specified in the from clause: ", parameters);
            var compare = string.Compare(
                Query.From.Alias.Value.Buffer,
                Query.From.Alias.Value.Offset,
                path, 0, indexOf, StringComparison.OrdinalIgnoreCase);
            if (compare != 0)
                ThrowInvalidWith(expr, "LOAD clause is trying to use an alias that isn't specified in the from clause: ", parameters);
            return path.Substring(indexOf + 1);
        }

        private void HandleLoadClause(BlittableJsonReaderObject parameters)
        {
            foreach (var load in Query.Load)
            {
                if (load.Alias == null)
                {
                    ThrowInvalidWith(load.Expression, "LOAD clause requires an alias but got: ", parameters);
                    return; // never hit
                }

                var alias = load.Alias.Value;

                string path;
                if (load.Expression is FieldExpression fe)
                {
                    path = fe.Field;
                }
                else if (load.Expression is ValueExpression ve)
                {
                    path = ve.Token;
                }
                else
                {
                    ThrowInvalidWith(load.Expression, "LOAD clause require a field or value refereces", parameters);
                    return; // never hit
                }

                var array = false;
                if (alias.EndsWith("[]"))
                {
                    array = true;
                    alias = alias.Subsegment(0, alias.Length - 2);
                }
                path = ParseExpressionPath(load.Expression, path, parameters);
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
            var sb = writer.GetStringBuilder();
            new StringQueryVisitor(sb).VisitExpression(expr);
            throw new InvalidQueryException(sb.ToString(), QueryText, parameters);
        }

        private OrderByField ExtractOrderByFromMethod(MethodExpression me, OrderByFieldType orderingType, bool asc, BlittableJsonReaderObject parameters)
        {
            if (me.Name.Equals("random", StringComparison.OrdinalIgnoreCase))
            {
                if (me.Arguments == null || me.Arguments.Count == 0)
                    return new OrderByField(null, OrderByFieldType.Random, asc);

                if (me.Arguments.Count > 1)
                    throw new InvalidQueryException("Invalid ORDER BY 'random()' call, expected zero to one arguments, got " + me.Arguments.Count,
                        QueryText, parameters);

                var token = me.Arguments[0] as ValueExpression;
                if (token == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'random()' call, expected value token , got " + me.Arguments[0], QueryText,
                        parameters);

                return new OrderByField(
                    null,
                    OrderByFieldType.Random,
                    asc,
                    null,
                    new[]
                    {
                        new OrderByField.Argument(token.Token, ValueTokenType.String)
                    });
            }

            if (me.Name.Equals("score", StringComparison.OrdinalIgnoreCase))
            {
                if (me.Arguments == null || me.Arguments.Count == 0)
                    return new OrderByField(null, OrderByFieldType.Score, asc);

                throw new InvalidQueryException("Invalid ORDER BY 'score()' call, expected zero arguments, got " + me.Arguments.Count, QueryText,
                    parameters);
            }

            if (me.Name.Equals("distance", StringComparison.OrdinalIgnoreCase))
            {
                if (me.Arguments.Count != 2)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected two arguments, got " + me.Arguments.Count, QueryText,
                        parameters);

                var fieldToken = me.Arguments[0] as FieldExpression;
                if (fieldToken == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected field token, got " + me.Arguments[0], QueryText,
                        parameters);

                var expression = me.Arguments[1] as MethodExpression;
                if (expression == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'distance()' call, expected expression, got " + me.Arguments[1], QueryText,
                        parameters);

                var methodName = expression.Name;
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
                    var argument = (ValueExpression)expression.Arguments[i];
                    arguments[i] = new OrderByField.Argument(argument.Token, argument.Value);
                }

                return new OrderByField(
                    fieldToken.Field,
                    OrderByFieldType.Distance,
                    asc,
                    methodType,
                    arguments);
            }

            if (IsGroupBy)
            {
                if (me.Name.Equals("count", StringComparison.OrdinalIgnoreCase))
                {
                    if (me.Arguments == null || me.Arguments.Count == 0)
                        return new OrderByField(CountFieldName, OrderByFieldType.Long, asc);

                    throw new InvalidQueryException("Invalid ORDER BY 'count()' call, expected zero arguments, got " + me.Arguments.Count, QueryText,
                        parameters);
                }

                if (me.Name.Equals("sum", StringComparison.OrdinalIgnoreCase))
                {
                    if (me.Arguments == null)
                        throw new InvalidQueryException("Invalid ORDER BY 'sum()' call, expected one argument but didn't get any", QueryText, parameters);

                    if (me.Arguments.Count != 1)
                        throw new InvalidQueryException("Invalid ORDER BY 'sum()' call, expected one argument, got " + me.Arguments.Count, QueryText,
                            parameters);

                    var sumFieldToken = me.Arguments[0] as FieldExpression;
                    if (sumFieldToken == null)
                        throw new InvalidQueryException("Invalid ORDER BY sum call, expected field value, go " + me.Arguments[0], QueryText, parameters);


                    if (orderingType == OrderByFieldType.Implicit)
                    {
                        orderingType = OrderByFieldType.Double;
                    }

                    return new OrderByField(sumFieldToken.Field, orderingType, asc);
                }
            }

            throw new InvalidQueryException("Invalid ORDER BY method call " + me.Name, QueryText, parameters);
        }

        private void FillSelectFields(BlittableJsonReaderObject parameters)
        {
            var fields = new List<SelectField>(Query.Select.Count);

            foreach (var fieldInfo in Query.Select)
            {
                string alias = null;

                if (fieldInfo.Alias != null)
                    alias = fieldInfo.Alias;

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
            if (expression is ValueExpression ve)
            {
                return SelectField.CreateValue(ve.Token, alias, ve.Value);
            }
            if (expression is FieldExpression fe)
            {
                if (fe.IsQuoted)
                {
                    return SelectField.CreateValue(fe.Field, alias, ValueTokenType.String);
                }
                return GetSelectValue(alias, fe);
            }
            if (expression is MethodExpression me)
            {
                var methodName = me.Name.Value;
                if (Enum.TryParse(methodName, ignoreCase: true, result: out AggregationOperation aggregation) == false)
                {
                    if (Query.DeclaredFunctions != null && Query.DeclaredFunctions.TryGetValue(methodName, out _))
                    {
                        var args = new SelectField[me.Arguments.Count];
                        for (int i = 0; i < me.Arguments.Count; i++)
                        {
                            if (me.Arguments[i] is ValueExpression vt)
                                args[i] = SelectField.CreateValue(vt.Token, alias, vt.Value);
                            else if (me.Arguments[i] is FieldExpression ft)
                                args[i] = GetSelectValue(null, ft);
                            else
                                args[i] = GetSelectField(parameters, me.Arguments[i], null);
                        }

                        return SelectField.CreateMethodCall(methodName, alias, args);
                    }

                    if (string.Equals("id", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        return SelectField.Create(Constants.Documents.Indexing.Fields.DocumentIdFieldName, alias);
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
                string fieldName = null;

                switch (aggregation)
                {
                    case AggregationOperation.Count:
                        fieldName = CountFieldName;
                        break;
                    case AggregationOperation.Sum:
                        if (me.Arguments == null)
                        {
                            ThrowMissingFieldNameArgumentOfSumMethod(QueryText, parameters);
                            return null; // never hit
                        }
                        if (me.Arguments.Count != 1)
                            ThrowIncorrectNumberOfArgumentsOfSumMethod(me.Arguments.Count, QueryText, parameters);

                        var sumFieldToken = me.Arguments[0] as FieldExpression;
                        if (sumFieldToken == null)
                        {
                            ThrowMissingFieldNameArgumentOfSumMethod(QueryText, parameters);
                            return null; //never hit
                        }

                        fieldName = sumFieldToken.Field.Value;
                        break;
                }

                Debug.Assert(fieldName != null);

                return SelectField.CreateGroupByAggregation(fieldName, alias, aggregation);
            }
            ThrowUnhandledExpressionTypeInSelect(expression.Type.ToString(), QueryText, parameters);
            return null; // never hit
        }

        private SelectField GetSelectValue(string alias, FieldExpression expressionField)
        {
            var name = expressionField.Field.Value;
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
                else if (RootAliasPaths.Count != 0)
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

        private static void ThrowBetweenMustHaveFieldSource(string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Between must have a source that is a field expression.", queryText, parameters);
        }

        private static void ThrowIncompatibleTypesOfVariables(string fieldName, string queryText, BlittableJsonReaderObject parameters,
            params QueryExpression[] valueTokens)
        {
            throw new InvalidQueryException($"Incompatible types of variables in WHERE clause on '{fieldName}' field. It got values of the following types: " +
                                            $"{string.Join(",", valueTokens.Select(x => x.Type.ToString()))}", queryText, parameters);
        }

        private static void ThrowIncompatibleTypesOfParameters(string fieldName, string queryText, BlittableJsonReaderObject parameters,
            params QueryExpression[] valueTokens)
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

        private static void ThrowUnhandledExpressionTypeInSelect(string expressionType, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unhandled expression of type {expressionType} in SELECT clause", queryText, parameters);
        }

        private static void ThrowInvalidOperatorTypeInOrderBy(string type, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Invalid type of operator in ORDER BY clause. Operator: {type}", queryText, parameters);
        }

        private static void ThrowGroupByCollectionIsNotSupported(string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Grouping by collections in auto map reduce indexes is not supported", queryText, parameters);
        }

        private class FillWhereFieldsAndParametersVisitor : WhereExpressionVisitor
        {
            private readonly QueryMetadata _metadata;
            private readonly string _fromAlias;
            private int _insideExact;

            public FillWhereFieldsAndParametersVisitor(QueryMetadata metadata, string fromAlias, string queryText) : base(queryText)
            {
                _metadata = metadata;
                _fromAlias = fromAlias;
            }

            private IDisposable Exact()
            {
                _insideExact++;

                return new DisposableAction(() => _insideExact--);
            }

            public override void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters)
            {
                if (fieldName is FieldExpression fe)
                    _metadata.AddWhereField(fe.Field, exact: _insideExact > 0);
            }

            public override void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters)
            {
                var fieldExpression = fieldName as FieldExpression;
                if (fieldExpression == null)
                {
                    ThrowBetweenMustHaveFieldSource(QueryText, parameters);
                    return; // never hit
                }

                var fv = firstValue as ValueExpression;
                var sv = secondValue as ValueExpression;

                if (fv == null || sv == null || fv.Value != sv.Value)
                    ThrowIncompatibleTypesOfVariables(fieldExpression.Field, QueryText, parameters, firstValue, secondValue);

                var valueType1 = GetValueTokenType(parameters, fv, unwrapArrays: false);
                var valueType2 = GetValueTokenType(parameters, sv, unwrapArrays: false);

                if (QueryBuilder.AreValueTokenTypesValid(valueType1, valueType2) == false)
                    ThrowIncompatibleTypesOfParameters(fieldExpression.Field, QueryText, parameters, firstValue, secondValue);

                _metadata.AddWhereField(fieldExpression.Field, exact: _insideExact > 0);
            }

            public override void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters)
            {
                if (values.Count == 0)
                    return;

                var fieldExpression = fieldName as FieldExpression;
                if (fieldExpression == null)
                {
                    ThrowInvalidInSource(parameters);
                    return; // never hit
                }

                var previousType = ValueTokenType.Null;
                for (var i = 0; i < values.Count; i++)
                {
                    var value = values[i] as ValueExpression;
                    if (value == null)
                    {
                        ThrowInvalidInValue(parameters);
                        return; // never hit
                    }

                    if (i > 0)
                    {
                        var previousValue = (ValueExpression)values[i - 1];

                        if (QueryBuilder.AreValueTokenTypesValid(previousValue.Value, value.Value) == false)
                            ThrowIncompatibleTypesOfVariables(fieldExpression.Field, QueryText, parameters, values.ToArray());
                    }

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: true);
                    if (i > 0 && QueryBuilder.AreValueTokenTypesValid(previousType, valueType) == false)
                        ThrowIncompatibleTypesOfParameters(fieldExpression.Field, QueryText, parameters, values.ToArray());

                    if (valueType != ValueTokenType.Null)
                        previousType = valueType;
                }

                _metadata.AddWhereField(fieldExpression.Field, exact: _insideExact > 0);
            }

            private void ThrowInvalidInSource(BlittableJsonReaderObject parameters)
            {
                throw new InvalidQueryException("In source must be a field", QueryText, parameters);
            }

            private void ThrowInvalidInValue(BlittableJsonReaderObject parameters)
            {
                throw new InvalidQueryException("In expression arguments must all be values", QueryText, parameters);
            }

            public override void VisitMethodTokens(StringSegment methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count == 0)
                    return;

                string fieldName;

                var methodType = QueryMethod.GetMethodType(methodName);

                switch (methodType)
                {
                    case MethodType.Id:
                        if (arguments.Count < 1 || arguments.Count > 2)
                            throw new InvalidQueryException($"Method {methodName}() expects one argument to be provided", QueryText, parameters);

                        var idExpression = arguments[arguments.Count - 1];
                        if (idExpression == null)
                            throw new InvalidQueryException($"Method {methodName}() expects expression , got {arguments[arguments.Count - 1]}", QueryText, parameters);

                        if (arguments.Count == 2)
                        {
                            if (_fromAlias == null)
                                throw new InvalidQueryException("Alias was passed to method 'id()' but query does not specify document alias.", QueryText, parameters);

                            var idAliasToken = arguments[0] as FieldExpression;
                            if (idAliasToken == null)
                                throw new InvalidQueryException($"Method 'id()' expects field token as a first argument, got {arguments[0]} type", QueryText, parameters);

                            if (idAliasToken.Field.Equals(_fromAlias) == false)
                                throw new InvalidQueryException(
                                    $"Alias passed to method 'id({idAliasToken.Field})' does not match specified document alias ('{_fromAlias}').", QueryText,
                                    parameters);
                        }

                        _metadata.AddWhereField(Constants.Documents.Indexing.Fields.DocumentIdFieldName);
                        break;
                    case MethodType.StartsWith:
                    case MethodType.EndsWith:
                    case MethodType.Search:
                    case MethodType.Lucene:
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);

                        if (arguments.Count == 1)
                            throw new InvalidQueryException($"Method {methodName}() expects second argument to be provided", QueryText, parameters);

                        var valueToken = arguments[1] as ValueExpression;

                        if (valueToken == null)
                            throw new InvalidQueryException($"Method {methodName}() expects value token as second argument, got {arguments[1]} type", QueryText,
                                parameters);

                        if (methodType == MethodType.Search)
                            _metadata.AddWhereField(fieldName, search: true);
                        else
                            _metadata.AddWhereField(fieldName, exact: _insideExact > 0);
                        break;
                    case MethodType.Exists:
                        fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);
                        _metadata.AddExistField(fieldName);
                        break;
                    case MethodType.Boost:
                        var firstArg = arguments[0];

                        if (firstArg == null)
                            throw new InvalidQueryException($"Method {methodName}() expects expression , got {arguments[0]}", QueryText, parameters);

                        Visit(firstArg, parameters);
                        break;
                    case MethodType.Intersect:
                        _metadata.IsIntersect = true;

                        for (var i = 0; i < arguments.Count; i++)
                        {
                            var expressionArgument = arguments[i];
                            Visit(expressionArgument, parameters);
                        }
                        return;
                    case MethodType.Exact:
                        if (arguments.Count != 1)
                            throw new InvalidQueryException($"Method {methodName}() expects one argument, got " + arguments.Count, QueryText, parameters);

                        using (Exact())
                        {
                            var expressionArgument = arguments[0];
                            Visit(expressionArgument, parameters);
                        }
                        return;
                    case MethodType.Count:
                        HandleCount(methodName, arguments, parameters);
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

            private void HandleSpatial(string methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                var fieldName = ExtractFieldNameFromFirstArgument(arguments, methodName, parameters);

                if (arguments.Count < 2 || arguments.Count > 3)
                    throw new InvalidQueryException($"Method {methodName}() expects 2-3 arguments to be provided", QueryText, parameters);

                var shapeExpression = arguments[1] as MethodExpression;

                if (shapeExpression == null)
                    throw new InvalidQueryException($"Method {methodName}() expects expression as second argument, got {arguments[1]} type", QueryText, parameters);

                if (arguments.Count == 3)
                {
                    var valueToken = arguments[2] as ValueExpression;

                    if (valueToken == null)
                        throw new InvalidQueryException($"Method {methodName}() expects value token as third argument, got {arguments[1]} type", QueryText, parameters);
                }

                methodName = shapeExpression.Name;

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

                _metadata.AddWhereField(fieldName, exact: _insideExact > 0);
            }

            private void HandleCount(string providedCountMethodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 1)
                    throw new InvalidQueryException("Method count() expects no argument", QueryText, parameters);

                if (arguments[0] is FieldExpression countExpression)
                {
                    _metadata._aliasToName[providedCountMethodName] = CountFieldName;
//                    countExpression.Field = methodNameToken;

                    Visit(countExpression, parameters);
                }
                else
                    throw new InvalidQueryException($"Method count() expects expression after its invocation, got {arguments[0]}", QueryText, parameters);
            }

            private void HandleSum(List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 2)
                    throw new InvalidQueryException("Method sum() expects one argument and operator after its invocation", QueryText, parameters);

                var fieldToken = arguments[0] as FieldExpression;

                if (fieldToken == null)
                    throw new InvalidQueryException($"Method sum() expects first argument to be field token, got {arguments[0]}", QueryText, parameters);

                var sumExpression = arguments[1];

                if (sumExpression == null)
                    throw new InvalidQueryException($"Method sum() expects expression after sum(), got {arguments[1]}", QueryText, parameters);

//                sumExpression.Field = fieldToken;
                Visit(sumExpression, parameters);
            }

            private string ExtractFieldNameFromFirstArgument(List<QueryExpression> arguments, string methodName, BlittableJsonReaderObject parameters)
            {
                var fieldArgument = arguments[0] as FieldExpression;

                if (fieldArgument == null)
                    throw new InvalidQueryException($"Method {methodName}() expects a field name as its first argument", QueryText, parameters);

                return fieldArgument.Field;
            }
        }

        public string GetUpdateBody()
        {
            if (Query.UpdateBody == null)
                throw new InvalidOperationException("UPDATE cluase was not specified");

            var updateBody = Query.UpdateBody;

            if (Query.From.Alias == null) // will have to use this 
            {
                if (Query.Load != null)
                    throw new InvalidOperationException("When using LOAD, a from alias is required");
                return updateBody;
            }

            var fromAlias = Query.From.Alias.Value;
            // patch is sending this, but we can also specify the alias.
            // this is so we can more easily share the code between query patch
            // and per document patch
            var sb = new StringBuilder("var ").Append(fromAlias).AppendLine(" = this;");

            if (Query.Load != null)
            {
                foreach (var load in Query.Load)
                {
                    var fieldExpression = load.Expression as FieldExpression;
                    if(fieldExpression == null)
                        throw new InvalidOperationException("Load clause can only load paths with fields, but got " + load.Expression);
                    var fullFieldPath = fieldExpression.Field.Value;   
                    if (fullFieldPath.StartsWith(fromAlias) == false)
                        throw new InvalidOperationException("Load clause can only load paths starting from the from alias: " + fromAlias);
                    var indexOfDot = fullFieldPath.IndexOf('.', fromAlias.Length);
                    fullFieldPath = fullFieldPath.Substring(indexOfDot + 1);

                    sb.Append("var ").Append(load.Alias)
                        .Append(" = loadPath(")
                        .Append(fromAlias)
                        .Append(", '")
                        .Append(fullFieldPath.Trim())
                        .AppendLine("');");
                }
            }
            sb.Append(updateBody);

            return sb.ToString();
        }

        private void EnsureValidGroupByField(string groupByFieldName, BlittableJsonReaderObject parameters)
        {
            var indexOfSeparatorStart = groupByFieldName.IndexOf(BlittableJsonTraverser.CollectionSeparator[0]);

            if (indexOfSeparatorStart != -1)
            {
                if (groupByFieldName.Length > indexOfSeparatorStart + BlittableJsonTraverser.CollectionSeparator.Length)
                {
                    if (groupByFieldName[indexOfSeparatorStart + 1] == BlittableJsonTraverser.CollectionSeparator[1] &&
                        groupByFieldName[indexOfSeparatorStart + 2] == BlittableJsonTraverser.CollectionSeparator[2])
                    {
                        ThrowGroupByCollectionIsNotSupported(QueryText, parameters);
                    }
                }
            }
        }
    }
}
