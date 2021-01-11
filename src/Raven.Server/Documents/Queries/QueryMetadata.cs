using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Esprima;
using Esprima.Ast;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Counters;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Highlightings;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.TimeSeries;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;
using BinaryExpression = Raven.Server.Documents.Queries.AST.BinaryExpression;

namespace Raven.Server.Documents.Queries
{
    public class QueryMetadata
    {
        internal static string SelectOutput = "__selectOutput";

        private readonly Dictionary<string, QueryFieldName> _aliasToName = new Dictionary<string, QueryFieldName>();

        public readonly Dictionary<StringSegment, (string PropertyPath, bool Array, bool Parameter, bool Quoted, string LoadFromAlias)> RootAliasPaths = new Dictionary<StringSegment, (string, bool, bool, bool, string)>();

        public QueryMetadata(string query, BlittableJsonReaderObject parameters, ulong cacheKey, QueryType queryType = QueryType.Select, DocumentDatabase database = null)
            : this(ParseQuery(query, queryType, database), parameters, cacheKey)
        {
        }

        private static Query ParseQuery(string q, QueryType queryType, DocumentDatabase database = null)
        {
            var qp = new QueryParser();
            qp.Init(q, database?.DocumentsStorage);
            return qp.Parse(queryType);
        }

        public QueryMetadata(Query query, BlittableJsonReaderObject parameters, ulong cacheKey)
        {
            CacheKey = cacheKey;

            Query = query;

            QueryText = Query.QueryText;

            IsGraph = Query.GraphQuery != null;
            IsDistinct = Query.IsDistinct;
            IsGroupBy = Query.GroupBy != null;

            if (IsGraph == false)
            {
                IsDynamic = Query.From.Index == false;
                var fromToken = Query.From.From;

                if (IsDynamic)
                    CollectionName = fromToken.FieldValue;
                else
                    IndexName = fromToken.FieldValue;

                if (IsDynamic == false || IsGroupBy)
                    IsCollectionQuery = false;
            }
            else
            {
                IsCollectionQuery = false;
            }

            if (IsGroupBy && IsDynamic == false)
                throw new ArgumentException("Can't use 'group by' when querying on an Index. 'group by' can be used only when querying on collections.");

            DeclaredFunctions = Query.DeclaredFunctions;

            Build(parameters);

            CanCache = cacheKey != 0;

            CreatedAt = DateTime.UtcNow;
            LastQueriedAt = CreatedAt;
        }

        public readonly bool IsDistinct;

        public readonly bool IsDynamic;

        public readonly bool IsGroupBy;

        public readonly bool IsGraph;
        public bool HasFacet { get; private set; }

        public bool HasSuggest { get; private set; }

        public bool HasMoreLikeThis { get; private set; }

        public bool HasBoost { get; private set; }

        public bool HasIntersect { get; private set; }

        public bool HasCmpXchg { get; private set; }

        public bool HasCmpXchgIncludes { get; internal set; }

        public bool HasHighlightings { get; private set; }

        public bool HasExplanations { get; private set; }

        public bool HasTimings { get; private set; }

        public bool HasCounterSelect { get; internal set; }

        public bool HasCmpXchgSelect { get; internal set; }

        public bool HasTimeSeriesSelect { get; internal set; }

        public bool IsCollectionQuery { get; private set; } = true;

        public Dictionary<string, DeclaredFunction> DeclaredFunctions { get; }

        public readonly string CollectionName;

        public readonly string IndexName;

        public string AutoIndexName;

        public readonly Query Query;

        public readonly string QueryText;

        public readonly HashSet<QueryFieldName> IndexFieldNames = new HashSet<QueryFieldName>();

        public readonly Dictionary<QueryFieldName, WhereField> WhereFields = new Dictionary<QueryFieldName, WhereField>();

        public GroupByField[] GroupBy;

        public OrderByField[] OrderBy;

        public SelectField[] SelectFields;

        public HighlightingField[] Highlightings;

        public ExplanationField Explanation;

        public CounterIncludesField CounterIncludes;

        public TimeSeriesIncludesField TimeSeriesIncludes;

        public readonly ulong CacheKey;

        public readonly bool CanCache;

        public string[] Includes;

        public string[] CompareExchangeValueIncludes;

        public bool HasIncludeOrLoad;

        public bool HasOrderByCustom;

        public bool HasOrderByRandom;

        public DateTime CreatedAt;

        public DateTime LastQueriedAt;

        public List<SpatialProperty> SpatialProperties;

        public List<SpatialShapeBase> SpatialShapes;

        private void AddExistField(QueryFieldName fieldName, BlittableJsonReaderObject parameters)
        {
            IndexFieldNames.Add(GetIndexFieldName(fieldName, parameters));
            IsCollectionQuery = false;
        }

        public void AddWhereField(QueryFieldName fieldName,
            BlittableJsonReaderObject parameters,
            bool search = false,
            bool exact = false,
            AutoSpatialOptions spatial = null,
            OperatorType? operatorType = null,
            bool isNegated = false,
            string methodName = null
            )
        {
            QueryFieldName indexFieldName;

            if (spatial != null && IsDynamic)
            {
                indexFieldName = new QueryFieldName(fieldName.Value, fieldName.IsQuoted);
            }
            else
            {
                var split = fieldName.Value.Split(".");
                if (split.Length > 1 && NotInRootAliasPaths(split[0]))
                {
                    ThrowUnknownAlias(split[0], parameters);
                }
                indexFieldName = GetIndexFieldName(fieldName, parameters);
            }

            if (operatorType == null &&
                // to support startsWith(id(), ...)
                string.Equals(methodName, "startsWith", StringComparison.OrdinalIgnoreCase))
                operatorType = OperatorType.Equal;

            if (search || exact || spatial != null || isNegated ||
                operatorType != OperatorType.Equal)
            {
                IsCollectionQuery = false;
            }
            else if (indexFieldName.Equals(QueryFieldName.DocumentId) == false)
            {
                IsCollectionQuery = false;
            }

            if (IndexFieldNames.Add(indexFieldName) == false)
            {
                // RavenDB-14176
                // Field might already exist in this list, but might be incomplete

                if (WhereFields.TryGetValue(indexFieldName, out var existingField))
                {
                    search |= existingField.IsFullTextSearch;
                    exact |= existingField.IsExactSearch;
                    spatial = spatial ?? existingField.Spatial;
                }
            }

            WhereFields[indexFieldName] = new WhereField(isFullTextSearch: search, isExactSearch: exact, spatial: spatial);
        }

        private static readonly Dictionary<StringSegment, WithEdgesExpression> EmptyEdges = new Dictionary<StringSegment, WithEdgesExpression>();

        private void Build(BlittableJsonReaderObject parameters)
        {
            string fromAlias = null;
            if (Query.From.Alias != null)
            {
                fromAlias = Query.From.Alias.Value.Value;
                RootAliasPaths[fromAlias] = (null, false, false, false, null);
            }

            if (Query.GroupBy != null)
            {
                GroupBy = new GroupByField[Query.GroupBy.Count];

                for (var i = 0; i < Query.GroupBy.Count; i++)
                {
                    GroupBy[i] = GetGroupByField(Query.GroupBy[i].Expression, Query.GroupBy[i].Alias?.Value, parameters);
                }
            }

            if (Query.Load != null)
                HandleLoadClause(parameters);

            if (IsGraph)
            {
                var recursivePatterns = Query.GraphQuery.RecursiveMatches;
                var edgePredicateKeys = Query.GraphQuery.WithEdgePredicates ?? EmptyEdges;
                var documentQueryKeys = Query.GraphQuery.WithDocumentQueries;

                if (recursivePatterns != null)
                {
                    foreach (var alias in recursivePatterns)
                    {
                        RootAliasPaths.TryAdd(alias, (null, false, false, false, null));
                    }
                }
                foreach (var edge in edgePredicateKeys)
                {
                    RootAliasPaths.TryAdd(edge.Key, (null, false, false, false, null));
                }
                foreach (var doc in documentQueryKeys)
                {
                    RootAliasPaths.TryAdd(doc.Key, (null, false, false, false, null));
                }

                if (documentQueryKeys == null)
                {
                    ThrowMissingVertexMatchClauses();
                }

                if (Query.Select != null)
                {
                    foreach (var projection in Query.Select)
                    {
                        if (!(projection.Expression is FieldExpression field))
                            continue;

                        var alias = field.Compound[0];
                        if (!edgePredicateKeys.ContainsKey(alias) && !documentQueryKeys.ContainsKey(alias) &&
                            recursivePatterns?.Contains(alias) != true)
                        {
                            ThrowOnUndefinedAlias(alias);
                        }
                    }
                }
            }

            if (Query.SelectFunctionBody.FunctionText != null)
                HandleSelectFunctionBody(parameters);
            else if (Query.Select != null)
                FillSelectFields(parameters);

            if (Query.Where != null)
            {
                Query.Where.ThrowIfInvalidMethodInvocationInWhere(parameters, QueryText);
                new FillWhereFieldsAndParametersVisitor(this, fromAlias, QueryText).Visit(Query.Where, parameters);
            }

            if (Query.OrderBy != null)
            {
                OrderBy = new OrderByField[Query.OrderBy.Count];

                for (var i = 0; i < Query.OrderBy.Count; i++)
                {
                    var order = Query.OrderBy[i];
                    if (order.Expression is MethodExpression me)
                    {
                        OrderBy[i] = ExtractOrderByFromMethod(me, fromAlias, order.FieldType, order.Ascending, parameters);
                    }
                    else if (order.Expression is FieldExpression fe)
                    {
                        OrderBy[i] = new OrderByField(GetIndexFieldName(fe, parameters), order.FieldType, order.Ascending);
                    }
                    else
                    {
                        ThrowInvalidOperatorTypeInOrderBy(order.Expression.Type.ToString(), QueryText, parameters);
                    }

                    if (IsCollectionQuery && OrderBy.Length > 0)
                        IsCollectionQuery = false;
                }
            }

            if (Query.Include != null)
                HandleQueryInclude(parameters);

            if (Query.DeclaredFunctions != null)
                HandleDeclaredFunctions();
        }

        private static void ThrowOnUndefinedAlias(StringSegment alias)
        {
            throw new InvalidQueryException($"Could not find alias '{alias}' defined the query. SELECT clause must include an alias that refers to either a vertex or an edge");
        }

        private static void ThrowMissingVertexMatchClauses()
        {
            throw new InvalidOperationException("Graph queries should have at least one vertex match clause");
        }

        private void HandleDeclaredFunctions()
        {
            foreach (var function in Query.DeclaredFunctions)
            {
                if (function.Value.Type == DeclaredFunction.FunctionType.TimeSeries)
                {
                    HasTimeSeriesSelect = true;
                    continue;
                }

                if (function.Value.Type == DeclaredFunction.FunctionType.JavaScript)
                {
                    var body = function.Value.JavaScript.Body;
                    HandleDeclaredFunctionBody(body);
                    if (HasIncludeOrLoad)
                        return;
                }
            }
        }

        private void HandleDeclaredFunctionBody(IEnumerable<Node> body)
        {
            foreach (var statement in body)
            {
                if (statement is ReturnStatement returnStatement)
                {
                    if (returnStatement.Argument is ObjectExpression objectExpression)
                    {
                        foreach (var property in objectExpression.Properties)
                        {
                            if (property is StaticMemberExpression staticMemberExpression)
                            {
                                HandleDeclaredFunctionStaticMemberExpression(staticMemberExpression);
                                if (HasIncludeOrLoad)
                                    return;
                            }
                        }
                    }
                    else if (returnStatement.Argument is StaticMemberExpression staticMemberExpression)
                    {
                        HandleDeclaredFunctionStaticMemberExpression(staticMemberExpression);
                        if (HasIncludeOrLoad)
                            return;
                    }
                }
                else if (statement is FunctionDeclaration functionDeclaration)
                {
                    HandleDeclaredFunctionBody(functionDeclaration.Body.ChildNodes);
                }
            }
        }

        private void HandleDeclaredFunctionStaticMemberExpression(StaticMemberExpression staticMemberExpression)
        {
            if (staticMemberExpression.Object is CallExpression callExpression)
            {
                if (callExpression.Callee is Identifier identifier)
                {
                    if (identifier.Name == "load" || identifier.Name == "include" || identifier.Name == "loadPath")
                    {
                        HasIncludeOrLoad = true;
                    }
                }
            }
        }

        private void ThrowInvalidMethod(BlittableJsonReaderObject parameters, MethodExpression me)
        {
            throw new InvalidQueryException("Where clause cannot contain just an '" + me.Name + "' method", Query.QueryText, parameters);
        }

        private void HandleQueryInclude(BlittableJsonReaderObject parameters)
        {
            List<string> includes = null;
            List<string> compareExchangeValueIncludes = null;
            List<HighlightingField> highlightings = null;

            void AddInclude(QueryExpression include, string path, ref List<string> listToAdd)
            {
                var expressionPath = ParseExpressionPath(include, path, Query.From.Alias);

                if (expressionPath.LoadFromAlias != null &&
                    NotInRootAliasPaths(expressionPath.LoadFromAlias))
                    ThrowUnknownAlias(expressionPath.LoadFromAlias, parameters);

                if (listToAdd == null)
                    listToAdd = new List<string>();

                listToAdd.Add(expressionPath.Path);
            }

            foreach (var include in Query.Include)
            {
                switch (include)
                {
                    case FieldExpression fe:
                        HasIncludeOrLoad = true;

                        AddInclude(include, fe.FieldValue, ref includes);
                        break;
                    case ValueExpression ve:
                        HasIncludeOrLoad = true;

                        AddInclude(include, ve.Token.Value, ref includes);
                        break;
                    case MethodExpression me:
                        var methodType = QueryMethod.GetMethodType(me.Name.Value);
                        switch (methodType)
                        {
                            case MethodType.Highlight:
                                if (IsGroupBy)
                                    throw new InvalidQueryException("Dynamic group by queries cannot have highlighting.", QueryText, parameters);

                                HasHighlightings = true;

                                QueryValidator.ValidateHighlight(me.Arguments, QueryText, parameters);

                                if (highlightings == null)
                                    highlightings = new List<HighlightingField>();

                                highlightings.Add(CreateHighlightingField(me, parameters));
                                break;
                            case MethodType.Explanation:
                                if (IsCollectionQuery)
                                    throw new InvalidQueryException("Collection queries cannot return explanations.", QueryText, parameters);

                                if (HasExplanations)
                                    throw new InvalidQueryException("Query cannot include duplicate explanations.", QueryText, parameters);

                                QueryValidator.ValidateExplanations(me.Arguments, QueryText, parameters);

                                Explanation = CreateExplanationField(me);
                                HasExplanations = true;
                                break;
                            case MethodType.Timings:
                                QueryValidator.ValidateTimings(me.Arguments, QueryText, parameters);
                                HasTimings = true;
                                break;
                            case MethodType.Counters:
                                QueryValidator.ValidateIncludeCounter(me.Arguments, QueryText, parameters);

                                if (CounterIncludes == null)
                                {
                                    CounterIncludes = new CounterIncludesField();
                                }

                                AddToCounterIncludes(CounterIncludes, me, parameters);
                                break;
                            case MethodType.TimeSeries:
                                QueryValidator.ValidateIncludeTimeseries(me.Arguments, QueryText, parameters);

                                if (TimeSeriesIncludes == null)
                                {
                                    TimeSeriesIncludes = new TimeSeriesIncludesField();
                                }

                                AddToTimeSeriesIncludes(TimeSeriesIncludes, me, parameters);
                                break;
                            case MethodType.CompareExchange:
                                QueryValidator.ValidateIncludeCompareExchangeValue(me.Arguments, QueryText, parameters);

                                HasCmpXchgIncludes = true;

                                var fieldName = ExtractFieldNameFromArgument(me.Arguments[0], withoutAlias: true, me.Name.Value, parameters, QueryText);

                                AddInclude(include, fieldName.Value, ref compareExchangeValueIncludes);
                                break;
                            default:
                                throw new InvalidQueryException($"Unable to figure out how to deal with include method '{methodType}'", QueryText, parameters);
                        }
                        break;
                    default:
                        throw new InvalidQueryException("Unable to figure out how to deal with include of type " + include.Type, QueryText, parameters);
                }
            }

            if (HasIncludeOrLoad)
                Includes = includes?.ToArray();

            if (HasHighlightings)
                Highlightings = highlightings?.ToArray();

            if (HasCmpXchgIncludes)
                CompareExchangeValueIncludes = compareExchangeValueIncludes?.ToArray();
        }

        private static ExplanationField CreateExplanationField(MethodExpression expression)
        {
            var result = new ExplanationField();

            if (expression.Arguments.Count == 1)
            {
                var ve = (ValueExpression)expression.Arguments[0];
                result.AddOptions(ve.Token.Value, ve.Value);
            }

            return result;
        }

        private HighlightingField CreateHighlightingField(MethodExpression expression, BlittableJsonReaderObject parameters)
        {
            var fieldName = ExtractFieldNameFromFirstArgument(expression.Arguments, expression.Name.Value, parameters);

            var result = new HighlightingField(fieldName);

            for (var i = 1; i < expression.Arguments.Count; i++)
            {
                var ve = (ValueExpression)expression.Arguments[i];

                switch (i)
                {
                    case 1:
                        result.AddFragmentLength(ve.Token.Value, ve.Value);
                        break;
                    case 2:
                        result.AddFragmentCount(ve.Token.Value, ve.Value);
                        break;
                    case 3:
                        result.AddOptions(ve.Token.Value, ve.Value);
                        break;
                }
            }

            return result;
        }

        private void HandleSelectFunctionBody(BlittableJsonReaderObject parameters)
        {
            if (Query.Select != null && Query.Select.Count > 0)
                ThrowInvalidFunctionSelectWithMoreFields(parameters);

            if (RootAliasPaths.Count == 0 && IsGraph == false)
                ThrowMissingAliasOnSelectFunctionBody(parameters);

            // validate that this is valid JS code
            try
            {
                Query.SelectFunctionBody.Program = ValidateScript(parameters);
                CheckIfProjectionHasSpecialMethod(Query.SelectFunctionBody.Program);
            }
            catch (Exception e)
            {
                var msg = QueryParser.AddLineAndColumnNumberToErrorMessage(e, "Select clause contains invalid script");
                throw new InvalidQueryException(msg, QueryText, parameters, e);
            }

            if (Query.DeclaredFunctions != null &&
                Query.DeclaredFunctions.ContainsKey(SelectOutput))
                ThrowUseOfReserveFunctionBodyMethodName(parameters);

            var sb = new StringBuilder();

            sb.Append("function ").Append(SelectOutput).Append("(");
            int index = 0;
            var args = new SelectField[IsGraph ?
                Query.GraphQuery.WithDocumentQueries.Count + Query.GraphQuery.WithEdgePredicates.Count + Query.GraphQuery.RecursiveMatches.Count :
                RootAliasPaths.Count];

            foreach (var alias in RootAliasPaths)
            {
                if (index != 0)
                    sb.Append(", ");
                sb.Append(alias.Key.Value);
                args[index++] = SelectField.Create(QueryFieldName.Empty, alias.Key.Value, alias.Value.PropertyPath,
                    alias.Value.Array, true, alias.Value.Parameter, alias.Value.Quoted, alias.Value.LoadFromAlias);
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

            sb.Append(Query.SelectFunctionBody.FunctionText);

            sb.AppendLine(";").AppendLine("}");

            if (Query.TryAddFunction(new DeclaredFunction
            {
                Name = SelectOutput,
                Type = DeclaredFunction.FunctionType.JavaScript,
                FunctionText = sb.ToString(),
                JavaScript = Query.SelectFunctionBody.Program
            }) == false)
            {
                ThrowUseOfReserveFunctionBodyMethodName(parameters);
            }

            SelectFields = new[] { SelectField.CreateMethodCall(SelectOutput, null, args) };
        }

        private void AddToCounterIncludes(CounterIncludesField counterIncludes, MethodExpression expression, BlittableJsonReaderObject parameters)
        {
            string sourcePath = null;
            var start = 0;
            if (expression.Arguments.Count > 0 &&
                expression.Arguments[0] is FieldExpression fe)
            {
                start = 1;

                if (Query.From.Alias?.Value != fe.FieldValue)
                {
                    if (RootAliasPaths.TryGetValue(fe.FieldValue, out var value))
                    {
                        sourcePath = value.PropertyPath;
                    }
                    else if (fe.FieldValue != null)
                    {
                        if (Query.From.Alias?.Value == null)
                        {
                            sourcePath = fe.FieldValue;
                        }
                        else
                        {
                            var split = fe.FieldValue.Split('.');
                            if (split.Length >= 2 &&
                                split[0] == Query.From.Alias.Value)
                            {
                                sourcePath = fe.FieldValue.Substring(split[0].Length + 1);
                            }
                        }
                    }
                }
            }

            if (start == expression.Arguments.Count)
            {
                counterIncludes.Counters[sourcePath ?? string.Empty] = new HashSet<string>();
                return;
            }

            for (var index = start; index < expression.Arguments.Count; index++)
            {
                if (!(expression.Arguments[index] is ValueExpression vt))
                    continue;

                if (vt.Value == ValueTokenType.Parameter)
                {
                    foreach (var v in QueryBuilder.GetValues(Query, this, parameters, vt))
                    {
                        AddCounterToInclude(counterIncludes, parameters, v, sourcePath);
                    }

                    continue;
                }

                var value = QueryBuilder.GetValue(Query, this, parameters, vt);

                AddCounterToInclude(counterIncludes, parameters, value, sourcePath);
            }
        }

        private void AddCounterToInclude(CounterIncludesField counterIncludes, BlittableJsonReaderObject parameters,
            (object Value, ValueTokenType Type) parameterValue, string sourcePath)
        {
            if (parameterValue.Type != ValueTokenType.String)
                throw new InvalidQueryException("Parameters of method `counters` must be of type `string` or `string[]`, " +
                                                $"but got `{parameterValue.Value}` of type `{parameterValue.Type}`", QueryText, parameters);

            counterIncludes.AddCounter(parameterValue.Value.ToString(), sourcePath);
        }

        private void AddToTimeSeriesIncludes(TimeSeriesIncludesField timeSeriesIncludes, MethodExpression expression, BlittableJsonReaderObject parameters)
        {
            string alias = null;
            var start = 0;
            if (expression.Arguments.Count > 0 &&
                expression.Arguments[0] is FieldExpression fe)
            {
                start = 1;

                if (Query.From.Alias?.Value != fe.FieldValue)
                {
                    if (RootAliasPaths.TryGetValue(fe.FieldValue, out var value))
                    {
                        alias = value.PropertyPath;
                    }

                    else if (fe.FieldValue != null)
                    {
                        if (Query.From.Alias?.Value == null)
                        {
                            alias = fe.FieldValue;
                        }
                        else
                        {
                            var split = fe.FieldValue.Split('.');
                            if (split.Length >= 2 &&
                                split[0] == Query.From.Alias.Value)
                            {
                                alias = fe.FieldValue.Substring(split[0].Length + 1);
                            }
                        }
                    }
                }
            }

            if (start == expression.Arguments.Count)
            {
                timeSeriesIncludes.TimeSeries[alias ?? string.Empty] = new HashSet<AbstractTimeSeriesRange>(AbstractTimeSeriesRangeComparer.Instance);
                return;
            }

            switch (expression.Arguments.Count - start)
            {
                case 3:
                {
                    var args = new string[3];

                    for (var index = start; index < expression.Arguments.Count; index++)
                    {
                        if (!(expression.Arguments[index] is ValueExpression vt))
                            continue;
                        var argIndex = index - start;
                        var arg = QueryBuilder.GetValue(Query, this, parameters, vt);

                        // name arg
                        if (argIndex == 0)
                        {
                            if (arg.Type != ValueTokenType.String)
                                throw new InvalidQueryException("Name parameters of method `timeseries` must be of type `string``, " +
                                                                $"but got `{arg.Value}` of type `{arg.Type}`", QueryText, parameters);
                            args[argIndex] = arg.Value.ToString();
                            continue;
                        }

                        // from/to args
                        switch (arg.Type)
                        {
                            case ValueTokenType.String:
                                args[argIndex] = arg.Value.ToString();
                                break;
                            case ValueTokenType.Null:
                                args[argIndex] = null;
                                break;
                            default:
                                throw new InvalidQueryException("From/To parameters of method of `timeseries` must be of type `string` or `null`, " +
                                                                $"but got `{arg.Value}` of type `{arg.Type}`", QueryText, parameters);
                        }
                    }
                    timeSeriesIncludes.AddTimeSeries(args[0], args[1], args[2], alias);
                    break;
                }
                case 2:
                    {
                        string name = TimeseriesIncludesHelper.ExtractValueFromExpression(expression.Arguments[0]);

                        if (!(expression.Arguments[1] is MethodExpression methodExpression))
                            throw new InvalidQueryException($"Expected to get include {nameof(MethodType.TimeSeries)} clause expression, but got: {expression}.", Query.QueryText, parameters);

                        switch (methodExpression.Arguments.Count)
                        {
                            case 1:
                                {
                                    // last count query
                                    var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, Query.QueryText);
                                    timeSeriesIncludes.AddTimeSeries(name, type, count, alias);
                                    break;
                                }
                            case 2:
                                {
                                    // last time query
                                    var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, Query.QueryText);
                                    timeSeriesIncludes.AddTimeSeries(name, type, time, alias);
                                    break;
                                }
                            default:
                                throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", Query.QueryText, parameters);
                        }

                        break;
                    }
                case 1:
                    {
                        if (!(expression.Arguments[0] is MethodExpression methodExpression))
                            throw new InvalidQueryException($"Expected to get include '{nameof(MethodType.TimeSeries)}' clause expression, but got: '{expression}'.", Query.QueryText, parameters);

                        switch (methodExpression.Arguments.Count)
                        {
                            case 1:
                                {
                                    // include timeseries(last(11))
                                    var (type, count) = TimeseriesIncludesHelper.ParseCount(methodExpression, Query.QueryText);
                                    timeSeriesIncludes.AddTimeSeries(Constants.TimeSeries.All, type, count, alias);
                                    break;
                                }
                            case 2:
                                {
                                    // include timeseries(last(600, 'seconds'))
                                    var (type, time) = TimeseriesIncludesHelper.ParseTime(methodExpression, Query.QueryText);
                                    timeSeriesIncludes.AddTimeSeries(Constants.TimeSeries.All, type, time);

                                    break;
                                }
                            default:
                                throw new InvalidQueryException($"Got invalid arguments count '{methodExpression.Arguments.Count}' in '{methodExpression.Name}' method.", Query.QueryText, parameters);
                        }
                        break;
                    }
                default:
                    throw new InvalidQueryException($"Got invalid arguments count '{expression.Arguments.Count}' in '{expression.Name}' method.", Query.QueryText, parameters);
            }
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

        public static (string Path, string LoadFromAlias) ParseExpressionPath(QueryExpression expr, string path, StringSegment? alias)
        {
            var indexOf = path.IndexOf('.');
            if (indexOf == -1 || alias == null)
                return (path, null);

            Debug.Assert(alias != null);

            var compare = string.Compare(
                alias.Value.Buffer,
                alias.Value.Offset,
                path, 0, indexOf, StringComparison.OrdinalIgnoreCase);

            return compare != 0 ? (path.Substring(indexOf + 1), path.Substring(0, indexOf)) : (path.Substring(indexOf + 1), null);
        }

        private void HandleLoadClause(BlittableJsonReaderObject parameters)
        {
            HasIncludeOrLoad = true;
            foreach (var load in Query.Load)
            {
                if (load.Alias == null)
                {
                    ThrowInvalidWith(load.Expression, "LOAD clause requires an alias but got: ", parameters);
                    return; // never hit
                }

                var alias = load.Alias.Value;

                var parameter = false;
                var quoted = false;
                string path;
                if (load.Expression is FieldExpression fe)
                {
                    path = fe.FieldValue;
                    quoted = fe.IsQuoted;
                }
                else if (load.Expression is ValueExpression ve)
                {
                    path = ve.Token.Value;
                    parameter = ve.Value == ValueTokenType.Parameter;
                }
                else
                {
                    ThrowInvalidWith(load.Expression, "LOAD clause require a field or value references", parameters);
                    return; // never hit
                }

                var array = false;
                if (alias.EndsWith("[]", StringComparison.Ordinal))
                {
                    array = true;
                    alias = alias.Subsegment(0, alias.Length - 2);
                }

                string loadFromAlias;
                (path, loadFromAlias) = ParseExpressionPath(load.Expression, path, Query.From.Alias);

                if (loadFromAlias != null &&
                    NotInRootAliasPaths(loadFromAlias))
                {
                    ThrowUnknownAlias(loadFromAlias, parameters);
                }

                if (RootAliasPaths.TryAdd(alias, (path, array, parameter, quoted, loadFromAlias)) == false)
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

        private OrderByField ExtractOrderByFromMethod(MethodExpression me, string fromAlias, OrderByFieldType orderingType, bool asc, BlittableJsonReaderObject parameters)
        {
            if (me.Name.Equals("id", StringComparison.OrdinalIgnoreCase))
            {
                if (orderingType == OrderByFieldType.Implicit)
                    orderingType = OrderByFieldType.String;

                switch (orderingType)
                {
                    case OrderByFieldType.AlphaNumeric:
                    case OrderByFieldType.String:
                        return new OrderByField(new QueryFieldName(Constants.Documents.Indexing.Fields.DocumentIdFieldName, false), orderingType, asc, MethodType.Id);
                    default:
                        throw new InvalidQueryException("Invalid ORDER BY 'id()' call, this field can only be sorted as a string or alphanumeric value, but got " + orderingType, QueryText, parameters);
                }
            }

            if (me.Name.Equals("custom", StringComparison.OrdinalIgnoreCase))
            {
                HasOrderByCustom = true;

                if (me.Arguments.Count != 2)
                    throw new InvalidQueryException("Invalid ORDER BY 'custom()' call, expected two arguments, got " + me.Arguments.Count, QueryText, parameters);

                var fieldName = ExtractFieldNameFromFirstArgument(me.Arguments, "custom", parameters);

                var token = me.Arguments[1] as ValueExpression;
                if (token == null)
                    throw new InvalidQueryException("Invalid ORDER BY 'custom()' call, expected value token , got " + me.Arguments[1], QueryText, parameters);

                return new OrderByField(
                    fieldName,
                    OrderByFieldType.Custom,
                    asc,
                    null,
                    new[]
                    {
                        new OrderByField.Argument(token.Token.Value, ValueTokenType.String)
                    });
            }

            if (me.Name.Equals("random", StringComparison.OrdinalIgnoreCase))
            {
                HasOrderByRandom = true;

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
                        new OrderByField.Argument(token.Token.Value, ValueTokenType.String)
                    });
            }

            if (me.Name.Equals("score", StringComparison.OrdinalIgnoreCase))
            {
                if (me.Arguments == null || me.Arguments.Count == 0)
                    return new OrderByField(null, OrderByFieldType.Score, asc);

                throw new InvalidQueryException("Invalid ORDER BY 'score()' call, expected zero arguments, got " + me.Arguments.Count, QueryText,
                    parameters);
            }

            if (me.Name.Equals("spatial.distance", StringComparison.OrdinalIgnoreCase))
            {
                if (me.Arguments.Count < 2 && me.Arguments.Count > 3)
                    throw new InvalidQueryException("Invalid ORDER BY 'spatial.distance(from, to, roundFactor)' call, expected 2-3 arguments, got " + me.Arguments.Count, QueryText,
                        parameters);

                QueryFieldName fieldName;

                if (me.Arguments[0] is MethodExpression firstArgME)
                {
                    var visitor = new FillWhereFieldsAndParametersVisitor(this, fromAlias, QueryText);
                    visitor.HandleSpatial("spatial.distance", me.Arguments, withoutAlias: true, parameters);
                    fieldName = new QueryFieldName(firstArgME.GetText(null), true);
                }
                else
                {
                    fieldName = ExtractFieldNameFromFirstArgument(me.Arguments, "spatial.distance", parameters);
                }

                var lastArgument = me.Arguments[1];

                if (!(lastArgument is MethodExpression expression))
                    throw new InvalidQueryException("Invalid ORDER BY 'spatial.distance()' call, expected expression, got " + lastArgument, QueryText,
                        parameters);

                var methodName = expression.Name;
                var methodType = QueryMethod.GetMethodType(methodName.Value);

                switch (methodType)
                {
                    case MethodType.Spatial_Circle:
                        QueryValidator.ValidateCircle(expression.Arguments, QueryText, parameters);
                        break;
                    case MethodType.Spatial_Wkt:
                        QueryValidator.ValidateWkt(expression.Arguments, QueryText, parameters);
                        break;
                    case MethodType.Spatial_Point:
                        QueryValidator.ValidatePoint(expression.Arguments, QueryText, parameters);
                        break;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }

                var additional = me.Arguments.Count == 3 ? 1 : 0;
                var arguments = new OrderByField.Argument[expression.Arguments.Count + additional];
                for (var i = 0; i < expression.Arguments.Count; i++)
                {
                    var argument = (ValueExpression)expression.Arguments[i];
                    arguments[i] = new OrderByField.Argument(argument.Token.Value, argument.Value);
                }

                if (additional != 0)
                {
                    // copy the roundFactor for spatial.distance
                    var argument = (ValueExpression)me.Arguments[2];
                    arguments[expression.Arguments.Count] = new OrderByField.Argument(argument.Token.Value, argument.Value);
                }

                return new OrderByField(
                    fieldName,
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
                        return new OrderByField(QueryFieldName.Count, OrderByFieldType.Long, asc)
                        {
                            AggregationOperation = AggregationOperation.Count
                        };

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

                    if (!(me.Arguments[0] is FieldExpression sumFieldToken))
                        throw new InvalidQueryException("Invalid ORDER BY sum call, expected field value, go " + me.Arguments[0], QueryText, parameters);

                    if (orderingType == OrderByFieldType.Implicit)
                    {
                        orderingType = OrderByFieldType.Double;
                    }

                    return new OrderByField(new QueryFieldName(sumFieldToken.FieldValue, sumFieldToken.IsQuoted), orderingType, asc)
                    {
                        AggregationOperation = AggregationOperation.Sum
                    };
                }
            }

            throw new InvalidQueryException("Invalid ORDER BY method call " + me.Name, QueryText, parameters);
        }

        [ThreadStatic] private static HashSet<string> _duplicateAliasHelper;

        static QueryMetadata()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _duplicateAliasHelper = null;
        }

        private void FillSelectFields(BlittableJsonReaderObject parameters)
        {
            if (_duplicateAliasHelper == null)
                _duplicateAliasHelper = new HashSet<string>();
            try
            {
                SelectFields = new SelectField[Query.Select.Count];

                for (var index = 0; index < Query.Select.Count; index++)
                {
                    var fieldInfo = Query.Select[index];
                    string alias = null;

                    if (fieldInfo.Alias != null)
                        alias = fieldInfo.Alias.Value.Value;

                    var expression = fieldInfo.Expression;

                    var selectField = GetSelectField(parameters, expression, alias);

                    SelectFields[index] = selectField;

                    var finalAlias = selectField.Alias ?? selectField.Name?.Value;

                    if (finalAlias != null && _duplicateAliasHelper.Add(finalAlias) == false)
                    {
                        if (finalAlias == selectField.Function)
                        {
                            ThrowDuplicateFunctionAliasInSelectClause(parameters, finalAlias);
                        }
                        else
                        {
                            ThrowInvalidDuplicateAliasInSelectClause(parameters, finalAlias);
                        }
                    }

                    if (selectField.Alias != null)
                    {
                        if (selectField.IsGroupByKey == false)
                        {
                            if (selectField.Name != null)
                                _aliasToName[selectField.Alias] = selectField.Name;
                        }
                        else
                        {
                            if (selectField.GroupByKeys.Length == 1)
                                _aliasToName[selectField.Alias] = selectField.GroupByKeys[0].Name;
                        }
                    }
                }
            }
            finally
            {
                _duplicateAliasHelper.Clear();
            }
        }

        private void ThrowDuplicateFunctionAliasInSelectClause(BlittableJsonReaderObject parameters, string finalAlias)
        {
            throw new InvalidQueryException($"Duplicate function call '{finalAlias}' in 'select' clause is detected. Function calls in 'select' without aliases implicitly consider function name as alias. In order to fix it, use explicit aliases - 'select id(employee) as EmployeeId, id(manager) as ManagerId'", QueryText, parameters);
        }

        private void ThrowInvalidDuplicateAliasInSelectClause(BlittableJsonReaderObject parameters, string finalAlias)
        {
            throw new InvalidQueryException($"Duplicate alias '{finalAlias}' detected. Note: if an alias is not specified, the implicit alias is equal to field name. So a select clause 'select employee.Name, manager.Name' is invalid because it has two fields with the same implicit alias - 'Name'. In order to fix it, use explicit aliases - 'select employee.Name as EmployeeName, manager.Name as ManagerName'", QueryText, parameters);
        }

        private SelectField GetSelectField(BlittableJsonReaderObject parameters, QueryExpression expression, string alias)
        {
            if (expression is ValueExpression ve)
            {
                if (HasFacet)
                    ThrowFacetQueryMustContainsOnlyFacetInSelect(ve, parameters);

                if (HasSuggest)
                    ThrowSuggestionQueryMustContainsOnlySuggestInSelect(ve, parameters);

                return SelectField.CreateValue(ve.Token.Value, alias, ve.Value);
            }

            if (expression is FieldExpression fe)
            {
                if (HasFacet)
                    ThrowFacetQueryMustContainsOnlyFacetInSelect(fe, parameters);

                if (HasSuggest)
                    ThrowSuggestionQueryMustContainsOnlySuggestInSelect(fe, parameters);

                if (fe.IsQuoted && fe.Compound.Count == 1)
                    return SelectField.CreateValue(fe.Compound[0].Value, alias, ValueTokenType.String);
                return GetSelectValue(alias, fe, parameters);
            }

            if (expression is MethodExpression me)
            {
                var methodName = me.Name.Value;
                if (Enum.TryParse(methodName, ignoreCase: true, result: out AggregationOperation aggregation) == false)
                {
                    if (Query.DeclaredFunctions != null && Query.DeclaredFunctions.TryGetValue(methodName, out var tuple))
                    {

                        CheckIfProjectionHasSpecialMethod(tuple.JavaScript);

                        if (HasFacet)
                            ThrowFacetQueryMustContainsOnlyFacetInSelect(me, parameters);

                        if (HasSuggest)
                            ThrowSuggestionQueryMustContainsOnlySuggestInSelect(me, parameters);

                        var args = new SelectField[me.Arguments.Count];
                        for (int i = 0; i < me.Arguments.Count; i++)
                        {
                            if (me.Arguments[i] is ValueExpression vt)
                                args[i] = SelectField.CreateValue(vt.Token.Value, alias, vt.Value);
                            else if (me.Arguments[i] is FieldExpression ft)
                                args[i] = GetSelectValue(null, ft, parameters);
                            else
                                args[i] = GetSelectField(parameters, me.Arguments[i], null);
                        }

                        return SelectField.CreateMethodCall(methodName, alias, args);
                    }

                    if (string.Equals("id", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (HasFacet)
                            ThrowFacetQueryMustContainsOnlyFacetInSelect(me, parameters);

                        if (HasSuggest)
                            ThrowSuggestionQueryMustContainsOnlySuggestInSelect(me, parameters);

                        if (IsGroupBy)
                            ThrowInvalidIdInGroupByQuery(parameters);

                        if (me.Arguments.Count > 0)
                        {
                            if (!(me.Arguments[0] is FieldExpression argumentExpression) || me.Arguments.Count != 1)
                            {
                                ThrowInvalidArgumentToId(parameters);
                            }
                            else
                            {
                                if (!RootAliasPaths.ContainsKey(argumentExpression.FieldValue))
                                    ThrowUnknownAlias(argumentExpression.FieldValue, parameters);
                                return SelectField.CreateMethodCall("id", alias, new[] { SelectField.Create(QueryFieldName.DocumentId, argumentExpression.FieldValue) });
                            }
                        }

                        return SelectField.Create(QueryFieldName.DocumentId, alias);
                    }

                    if (string.Equals("facet", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (IsGroupBy)
                            ThrowFacetQueryCannotBeGroupBy(parameters);

                        if (IsDistinct)
                            ThrowFacetQueryCannotBeDistinct(parameters);

                        if (HasSuggest)
                            ThrowFacetQueryCannotBeSuggest(parameters);

                        HasFacet = true;

                        return CreateFacet(me, alias, parameters);
                    }

                    if (string.Equals("spatial.distance", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        return SelectField.CreateMethodCall("spatial.distance", alias, ConvertSelectArguments(parameters, alias, me, methodName));
                    }

                    if (string.Equals("suggest", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (IsGroupBy)
                            ThrowSuggestionQueryCannotBeGroupBy(parameters);

                        if (IsDistinct)
                            ThrowSuggestionQueryCannotBeDistinct(parameters);

                        if (HasFacet)
                            ThrowSuggestionQueryCannotBeFacet(parameters);

                        HasSuggest = true;
                        IsCollectionQuery = false;

                        return CreateSuggest(me, alias, parameters);
                    }

                    if (string.Equals("counter", methodName, StringComparison.OrdinalIgnoreCase)
                        || string.Equals("counterRaw", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (HasFacet)
                            ThrowFacetQueryMustContainsOnlyFacetInSelect(me, parameters);

                        if (HasSuggest)
                            ThrowSuggestionQueryMustContainsOnlySuggestInSelect(me, parameters);

                        if (me.Arguments.Count == 0 || me.Arguments.Count > 2)
                            ThrowInvalidNumberOfArgumentsForCounter(methodName, parameters, me.Arguments.Count);

                        var counterField = SelectField.CreateCounterField(alias, ConvertSelectArguments(parameters, alias, me, methodName));
                        if (string.Equals("counterRaw", methodName, StringComparison.OrdinalIgnoreCase))
                        {
                            counterField.FunctionArgs = new SelectField[0];
                        }

                        HasCounterSelect = true;

                        return counterField;
                    }

                    if (string.Equals("cmpxchg", methodName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (HasFacet)
                            ThrowFacetQueryMustContainsOnlyFacetInSelect(me, parameters);

                        if (HasSuggest)
                            ThrowSuggestionQueryMustContainsOnlySuggestInSelect(me, parameters);

                        if (me.Arguments.Count == 0 || me.Arguments.Count > 1)
                            ThrowInvalidNumberOfArgumentsForCompareExchange(methodName, parameters, me.Arguments.Count);

                        var methodField = SelectField.CreateMethodCall(methodName, alias, ConvertSelectArguments(parameters, alias, me, methodName));

                        HasCmpXchgSelect = true;

                        return methodField;
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

                if (HasFacet)
                    ThrowFacetQueryMustContainsOnlyFacetInSelect(expression, parameters);

                if (HasSuggest)
                    ThrowSuggestionQueryMustContainsOnlySuggestInSelect(expression, parameters);

                QueryFieldName fieldName = null;

                switch (aggregation)
                {
                    case AggregationOperation.Count:
                        if (IsGroupBy == false)
                            ThrowInvalidAggregationMethod(parameters, methodName);
                        fieldName = QueryFieldName.Count;
                        break;
                    case AggregationOperation.Sum:
                        if (IsGroupBy == false)
                            ThrowInvalidAggregationMethod(parameters, methodName);
                        if (me.Arguments == null)
                        {
                            ThrowMissingFieldNameArgumentOfSumMethod(QueryText, parameters);
                            return null; // never hit
                        }
                        if (me.Arguments.Count != 1)
                            ThrowIncorrectNumberOfArgumentsOfSumMethod(me.Arguments.Count, QueryText, parameters);

                        fieldName = GetIndexFieldName(ExtractFieldNameFromFirstArgument(me.Arguments, "sum", parameters), parameters);
                        break;
                }

                Debug.Assert(fieldName != null);

                return SelectField.CreateGroupByAggregation(fieldName, alias, aggregation);
            }

            ThrowUnhandledExpressionTypeInSelect(expression.Type.ToString(), QueryText, parameters);
            return null; // never hit
        }

        private SelectField[] ConvertSelectArguments(BlittableJsonReaderObject parameters, string alias, MethodExpression me, string methodName)
        {
            var args = new SelectField[me.Arguments.Count];
            for (int i = 0; i < me.Arguments.Count; i++)
            {
                if (me.Arguments[i] is ValueExpression vt)
                    args[i] = SelectField.CreateValue(vt.Token.Value, alias, vt.Value);
                else if (me.Arguments[i] is FieldExpression ft)
                    args[i] = GetSelectValue(null, ft, parameters);
                else
                    ThrowCounterInvalidArgument(methodName, me.Arguments[i], parameters);
            }

            return args;
        }

        private void CheckIfProjectionHasSpecialMethod(Esprima.Ast.Program ast)
        {
            if (ast == null || (HasIncludeOrLoad && HasCounterSelect && HasCmpXchg && HasTimeSeriesSelect && HasCmpXchgIncludes))
                return;

            var visitor = new HasSpecialMethodVisitor(this);
            visitor.Visit(ast);
        }

        private SuggestionField CreateSuggest(MethodExpression expression, string alias, BlittableJsonReaderObject parameters)
        {
            if (expression.Arguments.Count < 2 || expression.Arguments.Count > 3)
                ThrowSuggestMethodMustHaveTwoOrThreeArguments(expression.Arguments.Count, parameters);

            var result = new SuggestionField();

            var name = ExtractFieldNameFromArgument(expression.Arguments[0], withoutAlias: false, "suggest", parameters, QueryText);

            if (expression.Arguments[1] is ValueExpression termExpression)
                result.AddTerm(termExpression.Token.Value, termExpression.Value);
            else
                ThrowSuggestMethodArgumentMustBeValue(2, expression.Arguments[1], parameters);

            if (expression.Arguments.Count == 3)
            {
                if (expression.Arguments[2] is ValueExpression optionsExpression)
                    result.AddOptions(optionsExpression.Token.Value, optionsExpression.Value);
                else
                    ThrowSuggestMethodArgumentMustBeValue(3, expression.Arguments[1], parameters);
            }

            result.Name = name;
            result.Alias = alias;

            return result;
        }

        private FacetField CreateFacet(MethodExpression expression, string alias, BlittableJsonReaderObject parameters)
        {
            QueryFieldName name = null;
            var result = new FacetField();

            for (var i = 0; i < expression.Arguments.Count; i++)
            {
                var argument = expression.Arguments[i];

                if (name == null && i == 0 && (argument is FieldExpression || argument is ValueExpression))
                {
                    name = ExtractFieldNameFromArgument(argument, withoutAlias: false, "facet", parameters, QueryText);
                    continue;
                }

                if (argument is ValueExpression ve)
                {
                    result.AddOptions(ve.Token.Value, ve.Value);
                    continue;
                }

                if (argument is MethodExpression me)
                {
                    var methodType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.Id:
                            if (expression.Arguments.Count != 1)
                                ThrowInvalidFacetUsingSetupDocument(parameters);

                            if (me.Arguments.Count != 1)
                                ThrowInvalidArgumentToIdInFacet(parameters);

                            result.FacetSetupDocumentId = ExtractFieldNameFromArgument(me.Arguments[0], withoutAlias: false, me.Name.Value, parameters, QueryText);
                            break;
                        case MethodType.Average:
                            AddFacetAggregation(me, result, FacetAggregation.Average, parameters);
                            break;
                        case MethodType.Sum:
                            AddFacetAggregation(me, result, FacetAggregation.Sum, parameters);
                            break;
                        case MethodType.Min:
                            AddFacetAggregation(me, result, FacetAggregation.Min, parameters);
                            break;
                        case MethodType.Max:
                            AddFacetAggregation(me, result, FacetAggregation.Max, parameters);
                            break;
                        default:
                            ThrowInvalidAggregationMethod(parameters, me.Name.Value);
                            break;
                    }

                    continue;
                }

                if (argument is BetweenExpression bee)
                {
                    if (name == null)
                        name = new QueryFieldName(((FieldExpression)bee.Source).GetText(null), false);

                    result.Ranges.Add(bee);
                    continue;
                }

                if (argument is BinaryExpression be)
                {
                    if (name == null)
                        name = new QueryFieldName(ExtractFieldNameFromBinaryExpression(be), false);

                    result.Ranges.Add(be);
                    continue;

                    string ExtractFieldNameFromBinaryExpression(BinaryExpression binaryExpression)
                    {
                        if (binaryExpression.Left is FieldExpression lfe)
                            return lfe.GetText(null);

                        if (binaryExpression.Left is ValueExpression lve)
                            return lve.Token.Value;

                        if (binaryExpression.Left is BinaryExpression lbe)
                            return ExtractFieldNameFromBinaryExpression(lbe);

                        if (binaryExpression.Right is BinaryExpression rbe)
                            return ExtractFieldNameFromBinaryExpression(rbe);

                        ThrowInvalidArgumentExpressionInFacetQuery(argument, parameters);
                        return null;
                    }
                }

                ThrowInvalidArgumentExpressionInFacetQuery(argument, parameters);
            }

            result.Name = name;
            result.Alias = alias;

            return result;
        }

        private void AddFacetAggregation(MethodExpression me, FacetField field, FacetAggregation aggregation, BlittableJsonReaderObject parameters)
        {
            if (me.Arguments.Count != 1)
                ThrowInvalidNumberOfArgumentsOfFacetAggregation(aggregation, 1, me.Arguments.Count, parameters);

            var methodFieldName = ExtractFieldNameFromArgument(me.Arguments[0], withoutAlias: false, me.Name.Value, parameters, QueryText);

            try
            {
                field.AddAggregation(aggregation, methodFieldName, me.Alias);
            }
            catch (Exception e)
            {
                throw new InvalidQueryException("Parsing facet aggregation operation failed.", QueryText, parameters, e);
            }
        }

        private SelectField GetSelectValue(string alias, FieldExpression expressionField, BlittableJsonReaderObject parameters)
        {
            (string Path, bool Array, bool Parameter, bool Quoted, string LoadFromAlias) sourceAlias;
            //TODO : handle here the case where expressionField has "[]" in the middle
            var name = new QueryFieldName(expressionField.FieldValue, expressionField.IsQuoted);
            bool hasSourceAlias = false;
            bool array = false;
            bool parameter = false;
            bool quoted = false;
            string loadFromAlias = null;

            if (expressionField.Compound.Count > 1)
            {
                if (expressionField.Compound.Last() == "[]")
                {
                    array = true;
                }

                if (RootAliasPaths.TryGetValue(expressionField.Compound[0], out sourceAlias))
                {
                    name = new QueryFieldName(expressionField.FieldValueWithoutAlias, expressionField.IsQuoted);
                    hasSourceAlias = true;
                    array = sourceAlias.Array;
                }
                else if (RootAliasPaths.Count != 0)
                {
                    ThrowUnknownAlias(expressionField.Compound[0].Value, parameters);
                }
            }
            else if (RootAliasPaths.TryGetValue(expressionField.Compound[0], out sourceAlias))
            {
                hasSourceAlias = true;
                if (string.IsNullOrEmpty(alias))
                    alias = expressionField.Compound[0].Value;
                array = sourceAlias.Array;
                name = QueryFieldName.Empty;
                parameter = sourceAlias.Parameter;
                quoted = sourceAlias.Quoted;
                loadFromAlias = sourceAlias.LoadFromAlias;
            }
            var fld = SelectField.Create(name, alias, sourceAlias.Path, array, hasSourceAlias, parameter, quoted, loadFromAlias);
            fld.ExpressionField = expressionField;
            return fld;
        }

        public QueryFieldName GetIndexFieldName(FieldExpression fe, BlittableJsonReaderObject parameters)
        {
            if (IsGraph)
            {
                return new QueryFieldName(fe.FieldValue, fe.IsQuoted);
            }

            if (_aliasToName.TryGetValue(fe.Compound[0].Value, out var indexFieldName) &&
                fe.Compound[0] != Query.From.Alias)
            {
                if (fe.Compound.Count != 1)
                    throw new InvalidQueryException("Field alias " + fe.Compound[0] + " cannot be used in a compound field, but got: " + fe, QueryText, parameters);

                return indexFieldName;
            }
            if (fe.Compound.Count == 1)
                return new QueryFieldName(fe.Compound[0].Value, fe.IsQuoted);

            if (RootAliasPaths.TryGetValue(fe.Compound[0], out _))
            {
                if (fe.Compound.Count == 2)
                {
                    return new QueryFieldName(fe.Compound[1].Value, fe.IsQuoted);
                }

                return new QueryFieldName(fe.FieldValueWithoutAlias, fe.IsQuoted);
            }

            if (RootAliasPaths.Count != 0)
            {
                ThrowUnknownAlias(fe.Compound[0].Value, parameters);
            }

            return new QueryFieldName(fe.FieldValue, fe.IsQuoted);
        }

        public QueryFieldName GetIndexFieldName(QueryFieldName fieldNameOrAlias, BlittableJsonReaderObject parameters)
        {
            if (_aliasToName.TryGetValue(fieldNameOrAlias.Value, out var indexFieldName))
                return indexFieldName;

            var indexOf = fieldNameOrAlias.Value.IndexOf('.');
            if (indexOf == -1)
                return fieldNameOrAlias;

            var key = new StringSegment(fieldNameOrAlias.Value, 0, indexOf);

            if (RootAliasPaths.TryGetValue(key, out _))
            {
                return new QueryFieldName(fieldNameOrAlias.Value.Substring(indexOf + 1), fieldNameOrAlias.IsQuoted);
            }

            if (RootAliasPaths.Count != 0)
            {
                ThrowUnknownAlias(key.Value, parameters);
            }

            return fieldNameOrAlias;
        }

        private GroupByField GetGroupByField(QueryExpression expression, string alias, BlittableJsonReaderObject parameters)
        {
            var byArrayBehavior = GroupByArrayBehavior.NotApplicable;
            QueryFieldName name;

            if (expression is FieldExpression field)
            {
                name = GetIndexFieldName(field, parameters);

                if (field.Compound.Count > 1)
                {
                    foreach (var part in field.Compound)
                    {
                        if (part == "[]")
                        {
                            byArrayBehavior = GroupByArrayBehavior.ByIndividualValues;
                            break;
                        }
                    }
                }
            }
            else if (expression is MethodExpression method)
            {
                var methodType = QueryMethod.GetMethodType(method.Name.Value);

                switch (methodType)
                {
                    case MethodType.Array:
                        name = GetIndexFieldName(method.Arguments[0] as FieldExpression, parameters);
                        byArrayBehavior = GroupByArrayBehavior.ByContent;
                        break;

                    default:
                        throw new InvalidQueryException($"Unsupported method '{method.Name}' in GROUP BY", QueryText, parameters);
                }
            }
            else if (expression is ValueExpression val)
            {
                name = new QueryFieldName(val.GetText(null), false);
            }
            else
                throw new InvalidQueryException($"Unsupported expression type '{expression.Type}' in GROUP BY", QueryText, parameters);

            return new GroupByField(name, byArrayBehavior, alias);
        }

        private static void ThrowBetweenMustHaveFieldSource(string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Between must have a source that is a field expression.", queryText, parameters);
        }

        private static void ThrowIncompatibleTypesOfVariables(object fieldName, string queryText, BlittableJsonReaderObject parameters,
            params QueryExpression[] valueTokens)
        {
            throw new InvalidQueryException($"Incompatible types of variables in WHERE clause on '{fieldName}' field. It got values of the following types: " +
                                            $"{string.Join(",", valueTokens.Select(x => x.Type.ToString()))}", queryText, parameters);
        }

        private static void ThrowIncompatibleTypesOfParameters(object fieldName, string queryText, BlittableJsonReaderObject parameters,
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

        private void ThrowUnknownAlias(string alias, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unknown alias {alias}, but there are aliases specified in the query ({string.Join(", ", RootAliasPaths.Keys)})",
                QueryText, parameters);
        }

        private void ThrowInvalidArgumentToId(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("id() in simple select clause must only be used without arguments", QueryText, parameters);
        }

        private void ThrowInvalidAggregationMethod(BlittableJsonReaderObject parameters, string methodName)
        {
            throw new InvalidQueryException(methodName + " may only be used in group by queries", QueryText, parameters);
        }

        private void ThrowInvalidIdInGroupByQuery(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use id() method in a group by query", QueryText, parameters);
        }

        private void ThrowFacetQueryCannotBeGroupBy(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use GROUP BY in a facet query", QueryText, parameters);
        }

        private void ThrowFacetQueryCannotBeDistinct(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use SELECT DISTINCT in a facet query", QueryText, parameters);
        }

        private void ThrowInvalidNumberOfArgumentsOfFacetAggregation(FacetAggregation method, int expected, int got, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Invalid number of arguments of {method} method in a facet query. Expected {expected}, got {got}", QueryText, parameters);
        }

        private void ThrowInvalidArgumentToIdInFacet(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("id() in facet query must have one argument which is identifier of a facet setup document", QueryText, parameters);
        }

        private void ThrowInvalidFacetUsingSetupDocument(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("facet() specifying a facet setup document using id() call must not have any additional arguments", QueryText, parameters);
        }

        private void ThrowInvalidArgumentExpressionInFacetQuery(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unsupported expression of type {expression.GetType().Name} specified as an argument of facet(). Text: {expression.GetText(null)}.", QueryText, parameters);
        }

        private void ThrowFacetQueryMustContainsOnlyFacetInSelect(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unsupported expression of type {expression.GetType().Name} specified as an argument of facet(). Text: {expression.GetText(null)}.", QueryText, parameters);
        }

        private void ThrowSuggestionQueryMustContainsOnlySuggestInSelect(QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Unsupported expression of type {expression.GetType().Name} specified as an argument of suggest(). Text: {expression.GetText(null)}.", QueryText, parameters);
        }

        private void ThrowSuggestionQueryCannotBeFacet(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use SELECT suggest() in a facet query", QueryText, parameters);
        }

        private void ThrowFacetQueryCannotBeSuggest(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use SELECT facet() in a suggestion query", QueryText, parameters);
        }

        private void ThrowSuggestionQueryCannotBeDistinct(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use SELECT DISTINCT in a suggestion query", QueryText, parameters);
        }

        private void ThrowSuggestionQueryCannotBeGroupBy(BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Cannot use GROUP BY in a suggestion query", QueryText, parameters);
        }

        private void ThrowSuggestMethodArgumentMustBeValue(int index, QueryExpression argument, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Argument at index '{index}' in suggest() must be a value but was '{argument.GetType()}'", QueryText, parameters);
        }

        private void ThrowSuggestMethodMustHaveTwoOrThreeArguments(int count, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Method suggest() must contain two or three arguments but '{count}' were specified", QueryText, parameters);
        }

        private void ThrowInvalidNumberOfArgumentsForCounter(string methodName, BlittableJsonReaderObject parameters, int argsCount)
        {
            throw new InvalidQueryException($"There is no overload of method '{methodName}' that takes {argsCount} arguments. " +
                                            $"Supported overloads are : {methodName}(name), {methodName}(doc, name).", QueryText, parameters);
        }

        private void ThrowInvalidNumberOfArgumentsForCompareExchange(string methodName, BlittableJsonReaderObject parameters, int argsCount)
        {
            throw new InvalidQueryException($"There is no overload of method '{methodName}' that takes {argsCount} arguments. " +
                                            $"Supported overloads are : {methodName}(name).", QueryText, parameters);
        }

        private void ThrowCounterInvalidArgument(string methodName, QueryExpression expression, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Invalid argument of type {expression.GetType().Name} specified as an argument of {methodName}(). Text: {expression.GetText(null)}.", QueryText, parameters);
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

            protected override void VisitBinaryExpression(BinaryExpression be)
            {
                if (be.Operator != OperatorType.Equal)
                    _metadata.IsCollectionQuery = false;
            }

            public override void VisitBooleanMethod(QueryExpression leftSide, QueryExpression rightSide, OperatorType operatorType, BlittableJsonReaderObject parameters)
            {
                if (leftSide is FieldExpression fe)
                {
                    _metadata.AddWhereField(new QueryFieldName(fe.FieldValue, fe.IsQuoted), parameters, exact: _insideExact > 0, operatorType: operatorType);
                }
                if (leftSide is MethodExpression lme)
                {
                    var methodType = QueryMethod.GetMethodType(lme.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.Id:// WHERE id() = [<Func> | <Value>]
                            if (rightSide is MethodExpression)
                            {
                                _metadata.AddWhereField(QueryFieldName.DocumentId, parameters, exact: _insideExact > 0, operatorType: operatorType);
                            }
                            else
                            {
                                if (rightSide is FieldExpression rfe)
                                    _metadata.AddWhereField(new QueryFieldName(rfe.FieldValue, rfe.IsQuoted), parameters, exact: _insideExact > 0, operatorType: operatorType);
                                _metadata.AddWhereField(QueryFieldName.DocumentId, parameters, exact: _insideExact > 0, operatorType: operatorType);
                            }
                            break;
                        case MethodType.Sum:
                        case MethodType.Count:
                            VisitFieldToken(leftSide, rightSide, parameters, null);
                            break;
                        default:
                            throw new ArgumentException($"The method {methodType} on the left side inside the WHERE clause is not supported.");
                    }
                }

                if (rightSide is MethodExpression rme)
                {
                    var methodType = QueryMethod.GetMethodType(rme.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.CompareExchange:
                            if (rme.Arguments.Count != 1)
                                throw new InvalidQueryException("Method cmpxchg() expects only one argument to be provided", QueryText, parameters);

                            if (!(rme.Arguments[0] is ValueExpression))
                                throw new InvalidQueryException($"Method cmpxchg() expects value token as second argument, got {rme.Arguments[0]} type", QueryText, parameters);

                            _metadata.HasCmpXchg = true;
                            break;
                    }
                }
            }

            public override void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters, OperatorType? operatorType)
            {
                if (fieldName is FieldExpression fe)
                    _metadata.AddWhereField(new QueryFieldName(fe.FieldValue, fe.IsQuoted), parameters, exact: _insideExact > 0);
                if (fieldName is MethodExpression me)
                {
                    var methodType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.Id:
                            _metadata.AddWhereField(QueryFieldName.DocumentId, parameters, exact: _insideExact > 0, operatorType: operatorType);
                            break;
                        case MethodType.Count:
                            _metadata.AddWhereField(QueryFieldName.Count, parameters, exact: _insideExact > 0);
                            break;
                        case MethodType.Sum:
                            if (me.Arguments != null && me.Arguments[0] is FieldExpression f)
                                VisitFieldToken(f, value, parameters, operatorType);
                            break;
                    }
                }
            }

            public override void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters)
            {
                if (fieldName is FieldExpression fe)
                {
                    _metadata.AddWhereField(new QueryFieldName(fe.FieldValue, fe.IsQuoted), parameters, exact: _insideExact > 0);
                }
                else if (fieldName is MethodExpression me)
                {
                    VisitMethodTokens(me.Name, me.Arguments, parameters);
                }
                else
                {
                    ThrowBetweenMustHaveFieldSource(QueryText, parameters);
                    return; // never hit
                }

                var fv = firstValue as ValueExpression;
                var sv = secondValue as ValueExpression;

                if (fv == null || sv == null || fv.Value != sv.Value)
                    ThrowIncompatibleTypesOfVariables(fieldName, QueryText, parameters, firstValue, secondValue);

                var valueType1 = GetValueTokenType(parameters, fv, unwrapArrays: false);
                var valueType2 = GetValueTokenType(parameters, sv, unwrapArrays: false);

                if (QueryBuilder.AreValueTokenTypesValid(valueType1, valueType2) == false)
                    ThrowIncompatibleTypesOfParameters(fieldName, QueryText, parameters, firstValue, secondValue);
            }

            public override void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters)
            {
                if (values.Count == 0)
                    return;

                var previousType = ValueTokenType.Null;
                for (var i = 0; i < values.Count; i++)
                {
                    if (!(values[i] is ValueExpression value))
                    {
                        ThrowInvalidInValue(parameters);
                        return; // never hit
                    }

                    if (i > 0)
                    {
                        var previousValue = (ValueExpression)values[i - 1];

                        if (QueryBuilder.AreValueTokenTypesValid(previousValue.Value, value.Value) == false)
                            ThrowIncompatibleTypesOfVariables(fieldName, QueryText, parameters, values.ToArray());
                    }

                    var valueType = GetValueTokenType(parameters, value, unwrapArrays: true);
                    if (i > 0 && QueryBuilder.AreValueTokenTypesValid(previousType, valueType) == false)
                        ThrowIncompatibleTypesOfParameters(fieldName, QueryText, parameters, values.ToArray());

                    if (valueType != ValueTokenType.Null)
                        previousType = valueType;
                }
                if (fieldName is FieldExpression fieldExpression)
                    _metadata.AddWhereField(new QueryFieldName(fieldExpression.FieldValue, fieldExpression.IsQuoted), parameters, exact: _insideExact > 0, operatorType: OperatorType.Equal);
            }

            private void ThrowInvalidInValue(BlittableJsonReaderObject parameters)
            {
                throw new InvalidQueryException("In expression arguments must all be values", QueryText, parameters);
            }

            public override void VisitMethodTokens(StringSegment methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                QueryFieldName fieldName;

                var methodType = QueryMethod.GetMethodType(methodName.Value);

                switch (methodType)
                {
                    case MethodType.Id:

                        if (arguments.Count == 0)
                        {
                            if (_fromAlias == null)
                            {
                                _metadata.AddWhereField(QueryFieldName.DocumentId, parameters);
                                break;
                            }

                            throw new InvalidQueryException($"Method {methodName}() was called, but as the query is using an alias ({_fromAlias}), must also be provided to this method.", QueryText, parameters);
                        }

                        if (arguments.Count > 2)
                            throw new InvalidQueryException($"Method {methodName}() expects not more than two arguments to be provided", QueryText, parameters);

                        var idExpression = arguments[arguments.Count - 1];
                        if (idExpression == null)
                            throw new InvalidQueryException($"Method {methodName}() expects expression , got {arguments[arguments.Count - 1]}", QueryText, parameters);

                        if (arguments.Count == 2)
                        {
                            if (_fromAlias == null)
                                throw new InvalidQueryException("Alias was passed to method 'id()' but query does not specify document alias.", QueryText, parameters);

                            if (!(arguments[0] is FieldExpression idAliasToken))
                                throw new InvalidQueryException($"Method 'id()' expects field token as a first argument, got {arguments[0]} type", QueryText, parameters);

                            if (idAliasToken.Compound.Count != 1 || idAliasToken.Compound[0].Equals(_fromAlias) == false)
                                throw new InvalidQueryException(
                                    $"Alias passed to method 'id({idAliasToken.Compound[0]})' does not match specified document alias ('{_fromAlias}').", QueryText,
                                    parameters);
                        }

                        _metadata.AddWhereField(QueryFieldName.DocumentId, parameters);
                        break;
                    case MethodType.StartsWith:
                    case MethodType.EndsWith:
                    case MethodType.Search:
                    case MethodType.Regex:
                    case MethodType.Lucene:
                        fieldName = _metadata.ExtractFieldNameFromFirstArgument(arguments, methodName.Value, parameters);

                        if (arguments.Count == 1)
                            throw new InvalidQueryException($"Method {methodName}() expects second argument to be provided", QueryText, parameters);

                        if (!(arguments[1] is ValueExpression))
                            throw new InvalidQueryException($"Method {methodName}() expects value token as second argument, got {arguments[1]} type", QueryText,
                                parameters);

                        if (methodType == MethodType.Search || methodType == MethodType.Lucene)
                            _metadata.AddWhereField(fieldName, parameters, search: true, methodName: methodName.Value);
                        else
                            _metadata.AddWhereField(fieldName, parameters, exact: _insideExact > 0, methodName: methodName.Value);
                        break;
                    case MethodType.Exists:
                        fieldName = _metadata.ExtractFieldNameFromFirstArgument(arguments, methodName.Value, parameters);
                        _metadata.AddExistField(fieldName, parameters);
                        break;
                    case MethodType.Boost:
                        _metadata.HasBoost = true;
                        var firstArg = arguments.Count == 0 ? null : arguments[0];

                        if (firstArg == null)
                            throw new InvalidQueryException($"Method {methodName}() expects expression , got {arguments[0]}", QueryText, parameters);

                        Visit(firstArg, parameters);
                        break;
                    case MethodType.Intersect:
                        _metadata.HasIntersect = true;

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
                        // nothing needs to be done here
                        return;
                    case MethodType.Sum:
                        HandleSum(arguments, parameters);
                        return;
                    case MethodType.Spatial_Within:
                    case MethodType.Spatial_Contains:
                    case MethodType.Spatial_Disjoint:
                    case MethodType.Spatial_Intersects:
                        HandleSpatial(methodName.Value, arguments, withoutAlias: false, parameters);
                        return;
                    case MethodType.MoreLikeThis:
                        HandleMoreLikeThis(methodName.Value, arguments, parameters);
                        return;
                    case MethodType.Fuzzy:
                        HandleFuzzy(methodName.Value, arguments, parameters);
                        return;
                    case MethodType.Proximity:
                        HandleProximity(methodName.Value, arguments, parameters);
                        return;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }
            }

            private void HandleProximity(string methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 2)
                    throw new InvalidQueryException($"Method {methodName}() expects to have two arguments", QueryText, parameters);

                var firstArgument = arguments[0] as MethodExpression;
                if (firstArgument == null)
                    throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a method expression", QueryText, parameters);

                var method = QueryMethod.GetMethodType(firstArgument.Name.Value);
                if (method != MethodType.Search)
                    throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a search method", QueryText, parameters);

                var secondArgument = arguments[1];
                if (secondArgument is ValueExpression == false)
                    throw new InvalidQueryException($"Method {methodName}() expects that second argument will be a value", QueryText, parameters);

                var value = QueryBuilder.GetValue(_metadata.Query, _metadata, parameters, secondArgument);
                if (value.Type != ValueTokenType.Long)
                    throw new InvalidQueryException($"Method {methodName}() expects that second argument will be a number", QueryText, parameters);

                Visit(firstArgument, parameters);
            }

            private void HandleFuzzy(string methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 2)
                    throw new InvalidQueryException($"Method {methodName}() expects to have two arguments", QueryText, parameters);

                var firstArgument = arguments[0] as BinaryExpression;
                if (firstArgument == null)
                    throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a binary expression", QueryText, parameters);

                if (firstArgument.Operator != OperatorType.Equal)
                    throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a binary expression with equals operator", QueryText, parameters);

                var secondArgument = arguments[1];
                if (secondArgument is ValueExpression == false)
                    throw new InvalidQueryException($"Method {methodName}() expects that second argument will be a value", QueryText, parameters);

                var value = QueryBuilder.GetValue(_metadata.Query, _metadata, parameters, secondArgument);
                if (value.Type != ValueTokenType.Long && value.Type != ValueTokenType.Double)
                    throw new InvalidQueryException($"Method {methodName}() expects that second argument will be a number", QueryText, parameters);

                Visit(firstArgument, parameters);
            }

            private void HandleMoreLikeThis(string methodName, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (_metadata.IsDynamic)
                    throw new InvalidQueryException($"Method {methodName}() cannot be used in dynamic queries", QueryText, parameters);

                if (arguments.Count == 0 || arguments.Count > 2)
                    throw new InvalidQueryException($"Method {methodName}() expects to have one or two arguments", QueryText, parameters);

                _metadata.HasMoreLikeThis = true;

                var firstArgument = arguments[0];
                if (firstArgument is BinaryExpression == false && firstArgument is FieldExpression == false && firstArgument is ValueExpression == false)
                    throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a binary expression or value", QueryText, parameters);

                if (arguments.Count != 2)
                    return;

                var secondArgument = arguments[1];
                if (secondArgument is ValueExpression)
                    return;

                throw new InvalidQueryException($"Method {methodName}() expects that second argument will be a parameter name or value", QueryText, parameters);
            }

            public void HandleSpatial(string methodName, List<QueryExpression> arguments, bool withoutAlias, BlittableJsonReaderObject parameters)
            {
                AutoSpatialOptions fieldOptions = null;
                QueryFieldName fieldName;
                if (_metadata.IsDynamic == false)
                {
                    if (arguments.Count == 0)
                        throw new InvalidQueryException($"Method {methodName}() expects at least one argument to be passed", QueryText, parameters);

                    var argument = arguments[0];
                    if (argument is FieldExpression == false && argument is ValueExpression == false)
                        throw new InvalidQueryException($"Method {methodName}() expects that first argument will be a field name when static index is queried", QueryText, parameters);

                    fieldName = _metadata.ExtractFieldNameFromArgument(argument, withoutAlias, methodName, parameters, QueryText);
                }
                else
                {
                    if (!(arguments[0] is MethodExpression spatialExpression))
                        throw new InvalidQueryException($"Method {methodName}() expects first argument to be a method expression", QueryText, parameters);

                    var spatialType = QueryMethod.GetMethodType(spatialExpression.Name.Value);
                    switch (spatialType)
                    {
                        case MethodType.Spatial_Wkt:
                            if (spatialExpression.Arguments.Count != 1)
                                throw new InvalidQueryException($"Method {methodName}() expects first argument to be a wkt() method with 1 argument", QueryText, parameters);

                            var wkt = _metadata.ExtractFieldNameFromArgument(spatialExpression.Arguments[0], withoutAlias, "wkt", parameters, QueryText).Value;

                            fieldOptions = new AutoSpatialOptions(AutoSpatialOptions.AutoSpatialMethodType.Wkt, new List<string>
                            {
                                wkt
                            });
                            break;
                        case MethodType.Spatial_Point:
                            if (spatialExpression.Arguments.Count != 2)
                                throw new InvalidQueryException($"Method {methodName}() expects first argument to be a point() method with 2 arguments", QueryText, parameters);

                            var latitudePropertyPath = _metadata.ExtractFieldNameFromArgument(spatialExpression.Arguments[0], withoutAlias, "point", parameters, QueryText).Value;
                            var longitudePropertyPath = _metadata.ExtractFieldNameFromArgument(spatialExpression.Arguments[1], withoutAlias, "point", parameters, QueryText).Value;

                            fieldOptions = new AutoSpatialOptions(AutoSpatialOptions.AutoSpatialMethodType.Point, new List<string>
                            {
                                latitudePropertyPath,
                                longitudePropertyPath
                            });

                            if (_metadata.SelectFields != null)
                            {
                                var latitudePropertySelectedFieldPath = GetSelectedFieldPath(latitudePropertyPath);
                                var longitudePropertySelectedFieldPath = GetSelectedFieldPath(longitudePropertyPath);
                                if (latitudePropertySelectedFieldPath != null && longitudePropertySelectedFieldPath != null)
                                {
                                    AddSpatialPropertiesToMetadata(latitudePropertySelectedFieldPath, longitudePropertySelectedFieldPath);
                                }
                            }
                            else
                            {
                                AddSpatialPropertiesToMetadata(latitudePropertyPath, longitudePropertyPath);
                            }
                            break;
                        default:
                            throw new InvalidQueryException($"Method {methodName}() expects first argument to be a point() or wkt() method", QueryText, parameters);
                    }

                    fieldName = new QueryFieldName(spatialExpression.GetText(null), false);
                }

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

                methodName = shapeExpression.Name.Value;

                var args = shapeExpression.Arguments;
                var methodType = QueryMethod.GetMethodType(methodName);
                
                switch (methodType)
                {
                    case MethodType.Spatial_Circle:
                        QueryValidator.ValidateCircle(args, QueryText, parameters);

                        if (_metadata.IsDynamic)
                        {
                            var unitsStr = args.Count == 4 ? (args[3] as ValueExpression).Token.ToString() : null;
                            var circleShape = new Circle(
                                (args[0] as ValueExpression).Token.ToString(),
                                (args[1] as ValueExpression).Token.ToString(),
                                (args[2] as ValueExpression).Token.ToString(),
                                unitsStr);

                            AddSpatialShapeToMetadata(circleShape);
                        }
                        break;
                    
                    case MethodType.Spatial_Wkt:
                        QueryValidator.ValidateWkt(args, QueryText, parameters);

                        if (_metadata.IsDynamic)
                        {
                            SpatialShape? shapeType = GetShapeString(args[0]);
                            switch (shapeType)
                            {
                                case SpatialShape.Circle:
                                    var unitsStrWkt = args.Count == 2 ? (args[1] as ValueExpression).Token.ToString() : null;
                                    var circleShapeWkt = new Circle((args[0] as ValueExpression).Token.ToString(), unitsStrWkt);
                                    AddSpatialShapeToMetadata(circleShapeWkt);
                                    break;
                                case SpatialShape.Polygon:
                                    var polygonShape = new Polygon((args[0] as ValueExpression).Token.ToString());
                                    AddSpatialShapeToMetadata(polygonShape);
                                    break;
                            }
                        }
                        break;
                    
                    case MethodType.Spatial_Point:
                        QueryValidator.ValidatePoint(args, QueryText, parameters);
                        break;
                    
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, QueryText, parameters);
                        break;
                }

                _metadata.AddWhereField(fieldName, parameters, exact: _insideExact > 0, spatial: fieldOptions);
            }

            private void HandleSum(List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                if (arguments.Count != 1)
                    throw new InvalidQueryException("Method sum() expects one argument only", QueryText, parameters);

                if (!(arguments[0] is FieldExpression f))
                    throw new InvalidQueryException($"Method sum() expects first argument to be field token, got {arguments[0]}", QueryText, parameters);

                _metadata.AddWhereField(new QueryFieldName(f.FieldValue, f.IsQuoted), parameters);
            }

            private string GetSelectedFieldPath(string propertyPath)
            {
                foreach (var selectField in _metadata.SelectFields)
                {
                    var selectedName = selectField.Name.ToString();
                    var selectedNameLength = selectedName.Length;
                    
                    if (propertyPath.StartsWith(selectedName) && (propertyPath.Length == selectedNameLength || propertyPath[selectedNameLength] == '.'))
                    {
                        if (selectField.Alias != null)
                        {
                            return selectField.Alias + propertyPath.Substring(selectedNameLength);
                        }

                        return propertyPath;
                    }
                }

                return null;
            }

            private void AddSpatialPropertiesToMetadata(string latitudePropertyPath, string longitudePropertyPath)
            {
                _metadata.SpatialProperties ??= new List<SpatialProperty>();
                
                var spatialProperty = new SpatialProperty(latitudePropertyPath, longitudePropertyPath);
                if (_metadata.SpatialProperties.Exists(x => x.LatitudeProperty == spatialProperty.LatitudeProperty && x.LongitudeProperty == spatialProperty.LongitudeProperty) == false)
                {
                    _metadata.SpatialProperties.Add(spatialProperty);
                }
            }

            private SpatialShape? GetShapeString(QueryExpression expression)
            {
                var expresionString = (expression as ValueExpression).Token.ToString();
                var tokens = expresionString.Split('(');
                if (tokens.Length == 0)
                {
                    throw new ArgumentException("Invalid WKT string format");
                }

                var firstToken = tokens[0].ToUpper().Trim();
                if (firstToken == "POLYGON")
                {
                    return SpatialShape.Polygon;
                }
                
                if (firstToken == "CIRCLE")
                {
                    return SpatialShape.Circle;
                }

                return null;
            }
            
            private void AddSpatialShapeToMetadata(SpatialShapeBase spatialShape)
            {
                _metadata.SpatialShapes ??= new List<SpatialShapeBase>();
                _metadata.SpatialShapes.Add(spatialShape);
            }
        }

        private QueryFieldName ExtractFieldNameFromFirstArgument(List<QueryExpression> arguments, string methodName, BlittableJsonReaderObject parameters)
        {
            if (arguments == null || arguments.Count == 0)
                throw new InvalidQueryException($"Method {methodName}() expects a field name as its first argument but no arguments were passed", QueryText, parameters);

            var argument = arguments[0];

            return ExtractFieldNameFromArgument(argument, withoutAlias: false, methodName, parameters, QueryText);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private QueryFieldName ExtractFieldNameFromArgument(QueryExpression argument, bool withoutAlias, string methodName, BlittableJsonReaderObject parameters, string queryText)
        {
            if (argument is FieldExpression field)
            {
                var name = field.FieldValue;
                if (withoutAlias && ShouldStripAlias(field))
                    name = field.FieldValueWithoutAlias;

                return new QueryFieldName(name, field.IsQuoted);
            }

            if (argument is ValueExpression value) // escaped string might go there
                return new QueryFieldName(value.Token.Value, value.Value == ValueTokenType.String);

            if (argument is MethodExpression method && string.Equals(method.Name.Value, Constants.Documents.Indexing.Fields.DocumentIdMethodName, StringComparison.OrdinalIgnoreCase)) //id property might be written as id() or id(<alias>)
                return new QueryFieldName(Constants.Documents.Indexing.Fields.DocumentIdFieldName, false);

            throw new InvalidQueryException($"Method {methodName}() expects a field name as its argument", queryText, parameters);
        }

        public string GetUpdateBody(BlittableJsonReaderObject parameters)
        {
            if (Query.UpdateBody == null)
                throw new InvalidQueryException("UPDATE clause was not specified", QueryText, parameters);

            var updateBody = Query.UpdateBody;

            if (Query.From.Alias == null) // will have to use this
            {
                if (Query.Load != null)
                    throw new InvalidQueryException("When using LOAD, a from alias is required", QueryText, parameters);
                return updateBody;
            }

            var fromAlias = Query.From.Alias.Value;
            // patch is sending this, but we can also specify the alias.
            // this is so we can more easily share the code between query patch
            // and per document patch
            var sb = new StringBuilder("var ").Append(fromAlias.Value).AppendLine(" = this;");

            if (Query.Load != null)
            {
                foreach (var load in Query.Load)
                {
                    if (!(load.Expression is FieldExpression fieldExpression))
                        throw new InvalidQueryException("Load clause can only load paths with fields, but got " + load.Expression, QueryText, parameters);
                    if (fieldExpression.Compound[0] != fromAlias)
                        throw new InvalidQueryException("Load clause can only load paths starting from the from alias: " + fromAlias, QueryText, parameters);

                    sb.Append("var ").Append(load.Alias)
                        .Append(" = loadPath(")
                        .Append(fromAlias.Value)
                        .Append(", '")
                        .Append(string.Join(".", fieldExpression.Compound.Skip(1)).Trim())
                        .AppendLine("');");
                }
            }
            sb.Append(updateBody);

            return sb.ToString();
        }

        private static readonly HashSet<string> JsBaseObjects = new HashSet<string>
        {
            "Math", "Number", "Object", "Date", "Array",
            // out stuff
            "console", "spatial"
        };

        private Esprima.Ast.Program ValidateScript(BlittableJsonReaderObject parameters)
        {
            HashSet<string> maybeUnknowns = null;
            Identifier currentProp = null;

            void VerifyKnownAliases(Node node)
            {
                switch (node)
                {
                    case Identifier identifier when currentProp == null:
                        currentProp = identifier;
                        break;
                    case ArrowFunctionExpression arrowFunction:
                        RemoveFromUnknowns(arrowFunction.Params);
                        break;
                    case FunctionExpression functionExpression:
                        RemoveFromUnknowns(functionExpression.Params);
                        break;
                    case Property prop when prop.Key == currentProp:
                        if (maybeUnknowns?.Count > 0)
                            ThrowUnknownAlias(maybeUnknowns.First(), parameters);
                        currentProp = null;
                        break;
                    case StaticMemberExpression sme when sme.Object is Identifier id &&
                                                         UnknownIdentifier(id.Name):
                        maybeUnknowns = maybeUnknowns ?? new HashSet<string>();
                        maybeUnknowns.Add(id.Name);
                        break;
                }
            }

            bool UnknownIdentifier(string identifier)
            {
                return RootAliasPaths.TryGetValue(identifier, out _) == false &&
                       JsBaseObjects.Contains(identifier) == false &&
                       (parameters == null ||
                        identifier.StartsWith("$") == false ||
                        parameters.TryGet(identifier.Substring(1), out object _) == false);
            }

            void RemoveFromUnknowns(NodeList<Expression> functionParameters)
            {
                if (maybeUnknowns == null || maybeUnknowns.Count == 0)
                    return;

                foreach (var p in functionParameters)
                {
                    if (!(p is Identifier i))
                        continue;

                    maybeUnknowns.Remove(i.Name);
                }
            }

            return new JavaScriptParser("return " + Query.SelectFunctionBody.FunctionText, new ParserOptions(), VerifyKnownAliases).ParseScript();
        }

        private bool NotInRootAliasPaths(string key)
        {
            return RootAliasPaths.Count > 0 &&
                   RootAliasPaths.TryGetValue(key, out _) == false;
        }

        private bool ShouldStripAlias(FieldExpression expr)
        {
            if (RootAliasPaths.Count == 0)
                return false;

            return RootAliasPaths.TryGetValue(expr.Compound[0], out _);
        }
    }
}
