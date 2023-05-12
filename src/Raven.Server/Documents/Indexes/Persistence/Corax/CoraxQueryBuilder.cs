using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.SortingMatches;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Spatial4n.Shapes;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using CoraxConstants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Corax;


namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static class CoraxQueryBuilder
{
    internal const int TakeAll = -1;

    internal class Parameters
    {
        public readonly IndexSearcher IndexSearcher;
        public readonly TransactionOperationContext ServerContext;
        public readonly DocumentsOperationContext DocumentsContext;
        public readonly IndexQueryServerSide Query;
        public readonly Index Index;
        public readonly BlittableJsonReaderObject QueryParameters;
        public readonly QueryBuilderFactories Factories;
        public readonly IndexFieldsMapping IndexFieldsMapping;
        public readonly FieldsToFetch FieldsToFetch;
        public readonly Dictionary<string, CoraxHighlightingTermIndex> HighlightingTerms;
        public readonly int Take;
        public readonly List<string> BuildSteps;
        public readonly MemoizationMatchProvider<AllEntriesMatch> AllEntries;
        public readonly QueryMetadata Metadata;
        public readonly bool HasDynamics;
        public readonly Lazy<List<string>> DynamicFields;
        public readonly ByteStringContext Allocator;
        public readonly bool HasBoost;

        internal Parameters(IndexSearcher searcher, ByteStringContext allocator, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext,
            IndexQueryServerSide query, Index index, BlittableJsonReaderObject queryParameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping,
            FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, List<string> buildSteps = null)
        {
            IndexSearcher = searcher;
            ServerContext = serverContext;
            Query = query;
            Index = index;
            QueryParameters = queryParameters;
            Factories = factories;
            IndexFieldsMapping = indexFieldsMapping;
            FieldsToFetch = fieldsToFetch;
            DocumentsContext = documentsContext;
            HighlightingTerms = highlightingTerms;
            Take = take;
            BuildSteps = buildSteps;
            AllEntries = IndexSearcher.Memoize(IndexSearcher.AllEntries());
            Metadata = query.Metadata;
            HasDynamics = index.Definition.HasDynamicFields;
            DynamicFields = HasDynamics
                ? new Lazy<List<string>>(() => IndexSearcher.GetFields())
                : null;
            HasBoost = index.HasBoostedFields | query.Metadata.HasBoost;
            Allocator = allocator;
        }
    }

    private static IQueryMatch MaterializeWhenNeeded(IQueryMatch source)
    {
        if (source is CoraxBooleanQueryBase cbq)
        {
            source = cbq.Materialize();
        }
        else if (source is CoraxBooleanItem cbi)
            source = cbi.Materialize();

        return source;
    }

    internal static IQueryMatch BuildQuery(Parameters builderParameters, out OrderMetadata[] sortMetadata)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;
            var metadata = builderParameters.Query.Metadata;
            var indexSearcher = builderParameters.IndexSearcher;
            var allEntries = indexSearcher.Memoize(indexSearcher.AllEntries());

            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery(builderParameters, metadata.Query.Where);
                coraxQuery = MaterializeWhenNeeded(coraxQuery);
            }
            else
            {
                coraxQuery = allEntries.Replay();
            }

            sortMetadata = GetSortMetadata(builderParameters);

            if (sortMetadata is not null)
                coraxQuery = OrderBy(builderParameters, coraxQuery, sortMetadata);
            else
                sortMetadata = null;

            // The parser already throws parse exception if there is a syntax error.
            // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
            return coraxQuery;
        }
    }

    private static IQueryMatch ToCoraxQuery(Parameters builderParameters, QueryExpression expression, bool exact = false, int? proximity = null)
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var serverContext = builderParameters.ServerContext;
        var documentsContext = builderParameters.DocumentsContext;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

        if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            QueryBuilderHelper.ThrowQueryTooComplexException(metadata, queryParameters);

        if (expression is null)
            return builderParameters.AllEntries.Replay();

        if (expression is BinaryExpression where)
        {
            builderParameters.BuildSteps?.Add($"Where: {expression.Type} - {expression} (operator: {where.Operator})");
            switch (where.Operator)
            {
                case OperatorType.And:
                {
                    IQueryMatch left = null;
                    IQueryMatch right = null;

                    // translate ((Foo >= $p1) and (Foo <= $p2)) to a more efficient between query
                    if (@where.Left is BinaryExpression lbe && lbe.IsRangeOperation &&
                        @where.Right is BinaryExpression rbe && rbe.IsRangeOperation && lbe.Left.Equals(rbe.Left) &&
                        lbe.Right is ValueExpression leftVal && rbe.Right is ValueExpression rightVal)
                    {
                        BetweenExpression bq = null;
                        if (lbe.IsGreaterThan && rbe.IsLessThan)
                        {
                            bq = new BetweenExpression(lbe.Left, leftVal, rightVal)
                            {
                                MinInclusive = lbe.Operator == OperatorType.GreaterThanEqual, MaxInclusive = rbe.Operator == OperatorType.LessThanEqual,
                            };
                        }

                        if (lbe.IsLessThan && rbe.IsGreaterThan)
                        {
                            bq = new BetweenExpression(lbe.Left, rightVal, leftVal)
                            {
                                MinInclusive = rbe.Operator == OperatorType.GreaterThanEqual, MaxInclusive = lbe.Operator == OperatorType.LessThanEqual
                            };
                        }

                        if (bq != null)
                            return TranslateBetweenQuery(builderParameters, bq, exact);
                    }

                    switch (@where.Left, @where.Right)
                    {
                        case (NegatedExpression ne1, NegatedExpression ne2):
                            left = ToCoraxQuery(builderParameters, ne1.Expression, exact);
                            right = ToCoraxQuery(builderParameters, ne2.Expression, exact);

                            TryMergeTwoNodesForAnd(indexSearcher, builderParameters.AllEntries, ref left, ref right, out var merged, true);

                            return indexSearcher.AndNot(builderParameters.AllEntries.Replay(), indexSearcher.Or(left, right));

                        case (NegatedExpression ne1, _):
                            left = ToCoraxQuery(builderParameters, @where.Right, exact);
                            right = ToCoraxQuery(builderParameters, ne1.Expression, exact);

                            TryMergeTwoNodesForAnd(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged, true);

                            return indexSearcher.AndNot(right, left);

                        case (_, NegatedExpression ne1):
                            left = ToCoraxQuery(builderParameters, @where.Left, exact);
                            right = ToCoraxQuery(builderParameters, ne1.Expression, exact);

                            TryMergeTwoNodesForAnd(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged, true);
                            return indexSearcher.AndNot(left, right);

                        default:
                            left = ToCoraxQuery(builderParameters, @where.Left, exact);
                            right = ToCoraxQuery(builderParameters, @where.Right, exact);


                            if (TryMergeTwoNodesForAnd(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged))
                                return merged;

                            return indexSearcher.And(left, right);
                    }
                }
                case OperatorType.Or:
                {
                    var left = ToCoraxQuery(builderParameters, @where.Left, exact);
                    var right = ToCoraxQuery(builderParameters, @where.Right, exact);

                    builderParameters.BuildSteps?.Add(
                        $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

                    var match = new CoraxOrQueries(indexSearcher);
                    if (match.TryAddItem(left) && match.TryAddItem(right))
                        return match;

                    TryMergeTwoNodesForAnd(indexSearcher, builderParameters.AllEntries, ref left, ref right, out var _, true);

                    return indexSearcher.Or(left, right);
                }
                default:
                {
                    var operation = QueryBuilderHelper.TranslateUnaryMatchOperation(where.Operator);

                    QueryExpression right = where.Right;

                    if (where.Right is MethodExpression rme)
                    {
                        right = QueryBuilderHelper.EvaluateMethod(metadata.Query, metadata, serverContext, documentsContext.DocumentDatabase.CompareExchangeStorage, rme, queryParameters);
                    }


                    var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, where.Left, metadata);

                    exact = QueryBuilderHelper.IsExact(builderParameters.Index, exact, fieldName);

                    var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, right, true);

                    var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
                        builderParameters.DynamicFields, exact: exact, hasBoost: builderParameters.HasBoost);

                    CoraxHighlightingTermIndex highlightingTerm = null;
                    bool? isHighlighting = builderParameters.HighlightingTerms?.TryGetValue(fieldName, out highlightingTerm);
                    if (isHighlighting.HasValue && isHighlighting.Value == false)
                    {
                        highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName};
                        builderParameters.HighlightingTerms.TryAdd(fieldName, highlightingTerm);
                    }

                    var match = valueType switch
                    {
                        ValueTokenType.Double => CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, value, operation),
                        ValueTokenType.Long => CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, value, operation),
                        ValueTokenType.True or
                            ValueTokenType.False or
                            ValueTokenType.Null or
                            ValueTokenType.String or
                            ValueTokenType.Parameter => HandleStringUnaryMatch(builderParameters),
                        _ => throw new NotSupportedException($"Unhandled token type: {valueType}")
                    };

                    if (highlightingTerm != null && valueType is ValueTokenType.Double or ValueTokenType.Long)
                    {
                        highlightingTerm.Values = value.ToString();
                    }

                    return match;


                    IQueryMatch HandleStringUnaryMatch(Parameters queryEnvironment)
                    {
                        if (exact && queryEnvironment.Metadata.IsDynamic)
                        {
                            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                            fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
                                builderParameters.DynamicFields, exact: exact, hasBoost: builderParameters.HasBoost);
                        }

                        if (value == null)
                        {
                            if (operation is UnaryMatchOperation.Equals)
                                return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, null, UnaryMatchOperation.Equals);
                            else if (operation is UnaryMatchOperation.NotEquals)
                                //Please consider if we ever need to support this.
                                return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, null, UnaryMatchOperation.NotEquals);
                            else
                                throw new NotSupportedException($"Unhandled operation: {operation}");
                        }

                        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                        if (highlightingTerm != null)
                            highlightingTerm.Values = valueAsString;

                        return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, valueAsString, operation);
                    }
                }
            }
        }

        if (expression is NegatedExpression ne)
        {
            builderParameters.BuildSteps?.Add($"Negated: {expression.Type} - {ne}");

            // 'not foo and bar' should be parsed as:
            // (not foo) and bar, instead of not (foo and bar)
            if (ne.Expression is BinaryExpression nbe &&
                nbe.Parenthesis == false &&
                (nbe.Operator == OperatorType.And || nbe.Operator == OperatorType.Or)
               )
            {
                var newExpr = new BinaryExpression(new NegatedExpression(nbe.Left),
                    nbe.Right, nbe.Operator);
                return ToCoraxQuery(builderParameters, newExpr, exact);
            }

            return ToCoraxQuery(builderParameters, ne.Expression, exact);
        }

        if (expression is BetweenExpression be)
        {
            builderParameters.BuildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(builderParameters, be, exact);
        }

        if (expression is InExpression ie)
        {
            builderParameters.BuildSteps?.Add($"In: {expression.Type} - {ie}");

            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, ie.Source, metadata);

            CoraxHighlightingTermIndex highlightingTerm = null;
            if (builderParameters.HighlightingTerms != null)
            {
                highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName};
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }

            exact = QueryBuilderHelper.IsExact(builderParameters.Index, exact, fieldName);
            if (exact && builderParameters.Metadata.IsDynamic)
            {
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                if (builderParameters.HighlightingTerms != null)
                {
                    highlightingTerm.DynamicFieldName = fieldName;
                    builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
                }
            }

            var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
                builderParameters.DynamicFields, exact: exact);

            if (ie.All)
            {
                var uniqueMatches = new HashSet<string>();
                foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
                {
                    if (exact && builderParameters.Metadata.IsDynamic)
                        fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                    uniqueMatches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
                }

                return indexSearcher.AllInQuery(fieldMetadata, uniqueMatches);
            }

            var matches = new List<string>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
            {
                matches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
            }

            if (highlightingTerm != null)
                highlightingTerm.Values = matches;
            
            return indexSearcher.InQuery(fieldMetadata, matches);
        }

        if (expression is TrueExpression)
        {
            builderParameters.BuildSteps?.Add($"True: {expression.Type} - {expression}");

            return builderParameters.AllEntries.Replay();
        }

        if (expression is MethodExpression me)
        {
            var methodName = me.Name.Value;
            var methodType = QueryMethod.GetMethodType(methodName);

            builderParameters.BuildSteps?.Add($"Method: {expression.Type} - {me} - method: {methodType}, {methodName}");

            switch (methodType)
            {
                case MethodType.Search:
                    return HandleSearch(builderParameters, me, proximity);
                case MethodType.Boost:
                    return HandleBoost(builderParameters, me, exact);
                case MethodType.StartsWith:
                    return HandleStartsWith(builderParameters, me, exact);
                case MethodType.EndsWith:
                    return HandleEndsWith(builderParameters, me, exact);
                case MethodType.Exists:
                    return HandleExists(builderParameters, me);
                case MethodType.Exact:
                    return HandleExact(builderParameters, me, proximity);
                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    return HandleSpatial(builderParameters, me, methodType);
                case MethodType.Regex:
                    return HandleRegex(builderParameters, me);
                case MethodType.MoreLikeThis:
                    return builderParameters.AllEntries.Replay();
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, queryParameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", metadata.QueryText, queryParameters);
    }

    public static MoreLikeThisQuery.MoreLikeThisQuery BuildMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            var filterQuery = BuildQuery(builderParameters, out _);
            filterQuery = MaterializeWhenNeeded(filterQuery);

            var moreLikeThisQuery = ToMoreLikeThisQuery(builderParameters, whereExpression, out var baseDocument, out var options);
            moreLikeThisQuery = MaterializeWhenNeeded(moreLikeThisQuery);

            return new MoreLikeThisQuery.MoreLikeThisQuery
            {
                BaseDocument = baseDocument, BaseDocumentQuery = moreLikeThisQuery, FilterQuery = filterQuery, Options = options
            };
        }
    }

    private static IQueryMatch ToMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression, out string baseDocument, out BlittableJsonReaderObject options)
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var serverContext = builderParameters.ServerContext;
        var context = builderParameters.DocumentsContext;
        baseDocument = null;
        options = null;

        var moreLikeThisExpression = QueryBuilderHelper.FindMoreLikeThisExpression(whereExpression);
        if (moreLikeThisExpression == null)
            throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

        if (moreLikeThisExpression.Arguments.Count == 2)
        {
            var value = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
            if (value.Type == ValueTokenType.String)
                options = IndexOperationBase.ParseJsonStringIntoBlittable(QueryBuilderHelper.GetValueAsString(value.Value), context);
            else
                options = value.Value as BlittableJsonReaderObject;
        }

        var firstArgument = moreLikeThisExpression.Arguments[0];
        if (firstArgument is BinaryExpression binaryExpression)
            return ToCoraxQuery(builderParameters, binaryExpression);

        var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, firstArgument).Value);
        if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
        {
            if (firstArgumentBool)
                return indexSearcher.AllEntries();

            return indexSearcher.EmptyMatch(); // empty boolean query yields 0 documents
        }

        baseDocument = firstArgumentValue;
        return null;
    }

    private static bool TryMergeTwoNodesForAnd(IndexSearcher indexSearcher, MemoizationMatchProvider<AllEntriesMatch> allEntries, ref IQueryMatch lhs, ref IQueryMatch rhs, out CoraxBooleanQueryBase merged, bool requiredMaterialization = false)
    {
        merged = null;
        switch (lhs, rhs, requiredMaterialization)
        {
            case (CoraxAndQueries lhsBq, CoraxAndQueries rhsBq, false):
                if (lhsBq.TryMerge(rhsBq))
                {
                    merged = lhsBq;
                    return true;
                }

                lhs = lhsBq.Materialize();
                rhs = rhsBq.Materialize();
                return false;

            case (CoraxAndQueries lhsBq, CoraxBooleanItem rhsBq, false):
                if (lhsBq.TryAnd(rhsBq))
                {
                    merged = lhsBq;
                    return true;
                }

                lhs = lhsBq.Materialize();
                return false;
            case (CoraxBooleanItem lhsBq, CoraxAndQueries rhsBq, false):
                if (rhsBq.TryAnd(lhsBq))
                {
                    merged = rhsBq;
                    return true;
                }

                rhs = rhsBq.Materialize();
                return false;

            case (CoraxBooleanItem lhsBq, CoraxBooleanItem rhsBq, false):
                if (CoraxBooleanItem.CanBeMergedForAnd(lhsBq, rhsBq))
                {
                    merged = new CoraxAndQueries(indexSearcher, allEntries, lhsBq, rhsBq);
                    return true;
                }

                return false;
            default:
                if (lhs is CoraxBooleanItem cbi)
                    lhs = cbi.Materialize();
                else if (lhs is CoraxBooleanQueryBase cbq)
                    lhs = cbq.Materialize();
                if (rhs is CoraxBooleanItem cbi1)
                    rhs = cbi1.Materialize();
                else if (rhs is CoraxBooleanQueryBase cbq1)
                    rhs = cbq1.Materialize();
                return false;
        }
    }

    private static IQueryMatch HandleExact(Parameters builderParameters, MethodExpression expression, int? proximity = null)
    {
        return ToCoraxQuery(builderParameters, expression.Arguments[0], true, proximity);
    }

    private static IQueryMatch TranslateBetweenQuery(Parameters builderParameters, BetweenExpression be, bool exact)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, be.Source, metadata);
        var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, be.Min);
        var (valueSecond, valueSecondType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, be.Max);
        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, exact: exact, hasBoost: builderParameters.HasBoost);
        var leftSideOperation = be.MinInclusive ? UnaryMatchOperation.GreaterThanOrEqual : UnaryMatchOperation.GreaterThan;
        var rightSideOperation = be.MaxInclusive ? UnaryMatchOperation.LessThanOrEqual : UnaryMatchOperation.LessThan;

        if ((valueFirstType, valueSecondType) is (ValueTokenType.Double, ValueTokenType.Double) or (ValueTokenType.Long, ValueTokenType.Long))
        {
            if (builderParameters.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName, Values = (valueFirst, valueSecond)};
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }
        }

        return (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(),
            _ => CoraxBooleanItem.Build(builderParameters.IndexSearcher, index, fieldMetadata, valueFirst, valueSecond, UnaryMatchOperation.Between, leftSideOperation,
                rightSideOperation)
        };

        IQueryMatch HandleStringBetween()
        {
            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            var valueFirstAsString = QueryBuilderHelper.CoraxGetValueAsString(valueFirst);
            var valueSecondAsString = QueryBuilderHelper.CoraxGetValueAsString(valueSecond);

            if (builderParameters.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName, Values = (valueFirst, valueSecond)};
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }

            return CoraxBooleanItem.Build(builderParameters.IndexSearcher, index, fieldMetadata, valueFirstAsString, valueSecondAsString, UnaryMatchOperation.Between,
                leftSideOperation, rightSideOperation);
        }
    }

    private static IQueryMatch HandleExists(Parameters builderParameters, MethodExpression expression)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var fieldMetadata = FieldMetadata.Build(builderParameters.Allocator, fieldName, default, default, default);
        return builderParameters.IndexSearcher.ExistsQuery(fieldMetadata);
    }

    private static IQueryMatch HandleStartsWith(Parameters builderParameters, MethodExpression expression, bool exact)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, queryParameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        CoraxHighlightingTermIndex highlightingTerm = null;
        if (builderParameters.HighlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName, Values = valueAsString};
            builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
        if (exact && builderParameters.Metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (builderParameters.HighlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }
        }

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, exact: exact, hasBoost: builderParameters.HasBoost);
        return builderParameters.IndexSearcher.StartWithQuery(fieldMetadata, valueAsString);
    }

    private static IQueryMatch HandleEndsWith(Parameters builderParameters, MethodExpression expression, bool exact)
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var highlightingTerms = builderParameters.HighlightingTerms;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, queryParameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);

        CoraxHighlightingTermIndex highlightingTerm = null;
        if (highlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex {FieldName = fieldName, Values = valueAsString};
            highlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(builderParameters.Index, exact, fieldName);
        if (exact && metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (highlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                highlightingTerms[fieldName] = highlightingTerm;
            }
        }

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, exact: exact, hasBoost: builderParameters.HasBoost);
        
        return indexSearcher.EndsWithQuery(fieldMetadata, valueAsString);
    }

    private static IQueryMatch HandleBoost(Parameters builderParameters, MethodExpression expression, bool exact)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var indexSearcher = builderParameters.IndexSearcher;

        if (expression.Arguments.Count != 2)
        {
            throw new InvalidQueryException($"Boost(expression, boostVal) requires two arguments, but was called with {expression.Arguments.Count}",
                metadata.QueryText, queryParameters);
        }


        float boost;
        var (val, type) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, expression.Arguments[1]);
        switch (val)
        {
            case float f:
                boost = f;
                break;
            case double d:
                boost = (float)d;
                break;
            case int i:
                boost = i;
                break;
            case long l:
                boost = l;
                break;
            case string s:
                if (float.TryParse(s, out boost) == false)
                {
                    throw new InvalidQueryException($"The boost value must be a valid float, but was called with {s}",
                        metadata.QueryText, queryParameters);
                }
                break;
            default:
                throw new InvalidQueryException($"Unable to find boost value: {val} ({type})",
                    metadata.QueryText, queryParameters);
        }


        
        var query = ToCoraxQuery(builderParameters, expression.Arguments[0], exact);
        
        switch (query)
        {
            case CoraxBooleanQueryBase cbqb:
                cbqb.Boosting = boost;
                return cbqb;
            case CoraxBooleanItem cbi:
                cbi.Boosting = boost;
                return cbi;
            default:
                return indexSearcher.Boost(query, boost);
    }
    }

    private static IQueryMatch HandleSearch(Parameters builderParameters, MethodExpression expression, int? proximity)
    {
        var metadata = builderParameters.Metadata;
        var highlightingTerms = builderParameters.HighlightingTerms;
        var queryParameters = builderParameters.QueryParameters;
        var indexSearcher = builderParameters.IndexSearcher;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

        QueryFieldName fieldName;
        var isDocumentId = false;
        switch (expression.Arguments[0])
        {
            case FieldExpression ft:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, ft, metadata);
                break;
            case ValueExpression vt:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, vt, metadata);
                break;
            case MethodExpression me when QueryMethod.GetMethodType(me.Name.Value) == MethodType.Id:
                fieldName = QueryFieldName.DocumentId;
                isDocumentId = true;
                break;
            default:
                throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);
        }


        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, builderParameters.Metadata.QueryText,
                builderParameters.QueryParameters);

        Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        if (highlightingTerms != null && highlightingTerms.TryGetValue(fieldName, out var highlightingTerm) == false)
        {
            highlightingTerm = new CoraxHighlightingTermIndex {Values = valueAsString,};

            highlightingTerm.FieldName = fieldName;
            highlightingTerms?.TryAdd(fieldName, highlightingTerm);


            if (metadata.IsDynamic && isDocumentId == false)
            {
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                // We now add the dynamic field too. 
                highlightingTerm.DynamicFieldName = fieldName;
            }
        }
        else if (metadata.IsDynamic && isDocumentId == false)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
        }

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, handleSearch: true, hasBoost: builderParameters.HasBoost);

        if (proximity.HasValue)
        {
            throw new NotSupportedException($"{nameof(Corax)} doesn't support proximity over search() method");
        }

        CoraxConstants.Search.Operator @operator = CoraxConstants.Search.Operator.Or;
        if (expression.Arguments.Count == 3)
        {
            var fieldExpression = (FieldExpression)expression.Arguments[2];
            if (fieldExpression.Compound.Count != 1)
                QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, queryParameters, fieldExpression);

            var op = fieldExpression.Compound[0];
            if (string.Equals("AND", op.Value, StringComparison.OrdinalIgnoreCase))
                @operator = Constants.Search.Operator.And;
            else if (string.Equals("OR", op.Value, StringComparison.OrdinalIgnoreCase))
                @operator = Constants.Search.Operator.Or;
            else
                QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, queryParameters, fieldExpression);
        }
        
        return indexSearcher.SearchQuery(fieldMetadata, GetValues(), @operator);
        
        /*
         * Here we need to deal with value that comes from the user, which means that we
         * have to be careful.
         *
         * The rules are that we'll split the terms on whitespace, except if they are quoted
         * using ", however, you may escape the " using \, and \ using \\.
         */
        IEnumerable<string> GetValues()
        {
            List<int> escapePositions = null;

            var quoted = false;
            var lastWordStart = 0;
            for (var i = 0; i < valueAsString.Length; i++)
            {
                switch (valueAsString[i])
                {
                    case '\\' when IsEscaped(valueAsString, i):
                        AddEscapePosition(i);
                        break;
                    case '"':
                        if (IsEscaped(valueAsString, i))
                        {
                            AddEscapePosition(i);
                            continue;
                        }

                        if (lastWordStart != i)
                        {
                            yield return YieldValue(valueAsString, lastWordStart, i - lastWordStart, escapePositions);
                        }

                        quoted = !quoted;
                        lastWordStart = i + 1;
                        break;
                    case '\t':
                    case ' ':
                        if (quoted)
                            continue;

                        if (lastWordStart != i)
                        {
                            yield return YieldValue(valueAsString, lastWordStart, i - lastWordStart, escapePositions);
                        }

                        lastWordStart = i + 1; // skipping
                        break;
                }
            }

            if (valueAsString.Length - lastWordStart > 0)
                yield return YieldValue(valueAsString, lastWordStart, valueAsString.Length - lastWordStart, escapePositions);


            void AddEscapePosition(int i)
            {
                escapePositions ??= new List<int>(16);
                escapePositions.Add(i - 1);
            }
        }

        string YieldValue(string input, int startIndex, int length, List<int> escapePositions)
        {
            if (escapePositions == null || escapePositions.Count == 0)
                return input.Substring(startIndex, length);

            var sb = new StringBuilder(input, startIndex, length, length);

            for (int i = escapePositions.Count - 1; i >= 0; i--)
            {
                sb.Remove(escapePositions[i] - startIndex, 1);
            }

            escapePositions.Clear();

            return sb.ToString();
        }

        bool IsEscaped(string input, int index)
        {
            var count = 0;
            for (int i = index - 1; i >= 0; i--)
            {
                if (input[i] == '\\')
                {
                    count++;
                }
                else
                {
                    break;
                }
            }

            return (count & 1) == 1;
        }
    }


    private static IQueryMatch HandleSpatial(Parameters builderParameters, MethodExpression expression, MethodType spatialMethod)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var allocator = builderParameters.Allocator;
        
        string fieldName;
        if (metadata.IsDynamic == false)
            fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        else
        {
            var spatialExpression = (MethodExpression)expression.Arguments[0];
            fieldName = metadata.GetSpatialFieldName(spatialExpression, builderParameters.QueryParameters);
        }
        
        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);
        var shapeExpression = (MethodExpression)expression.Arguments[1];

        var distanceErrorPct = RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
        if (expression.Arguments.Count == 3)
        {
            var distanceErrorPctValue = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[2]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

            distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
        }

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);

        var methodName = shapeExpression.Name;
        var methodType = QueryMethod.GetMethodType(methodName.Value);

        IShape shape = null;
        switch (methodType)
        {
            case MethodType.Spatial_Circle:
                shape = QueryBuilderHelper.HandleCircle(metadata.Query, shapeExpression, metadata, queryParameters, fieldName, spatialField, out _);
                break;
            case MethodType.Spatial_Wkt:
                shape = QueryBuilderHelper.HandleWkt(builderParameters, fieldName, shapeExpression, spatialField, out _);
                break;
            default:
                QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, builderParameters.QueryParameters);
                break;
        }

        Debug.Assert(shape != null);

        var operation = spatialMethod switch
        {
            MethodType.Spatial_Within => global::Corax.Utils.Spatial.SpatialRelation.Within,
            MethodType.Spatial_Disjoint => global::Corax.Utils.Spatial.SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => global::Corax.Utils.Spatial.SpatialRelation.Intersects,
            MethodType.Spatial_Contains => global::Corax.Utils.Spatial.SpatialRelation.Contains,
            _ => (global::Corax.Utils.Spatial.SpatialRelation)QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, builderParameters.QueryParameters)
        };


        //var args = new SpatialArgs(operation, shape) {DistErrPct = distanceErrorPct};

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), operation);
    }

    private static IQueryMatch HandleRegex(Parameters builderParameters, MethodExpression expression)
    {
        if (expression.Arguments.Count != 2)
            throw new ArgumentException(
                $"Regex method was invoked with {expression.Arguments.Count} arguments ({expression})" +
                " while it should be invoked with 2 arguments e.g. Regex(foo.Name,\"^[a-z]+?\")");

        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String && !(valueType == ValueTokenType.Parameter && IsStringFamily(value)))
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("regex", ValueTokenType.String, valueType, builderParameters.Metadata.QueryText,
                builderParameters.QueryParameters);
        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        return builderParameters.IndexSearcher.RegexQuery(FieldMetadata.Build(builderParameters.Allocator, fieldName, default, default, default), builderParameters.Factories.GetRegexFactory(valueAsString));

        bool IsStringFamily(object value)
        {
            return value is string || value is StringSegment || value is LazyStringValue;
        }
    }

    public static OrderMetadata[] GetSortMetadata(Parameters builderParameters)
    {
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var getSpatialField = builderParameters.Factories?.GetSpatialFieldFactory;
        var indexMapping = builderParameters.IndexFieldsMapping;
        var queryMapping = builderParameters.FieldsToFetch;
        var allocator = builderParameters.Allocator;
        if (query.PageSize == 0) // no need to sort when counting only
            return null;

        var orderByFields = query.Metadata.OrderBy;

        if (orderByFields == null)
        {
            if (builderParameters.HasBoost && index.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved)
                return new[] {new OrderMetadata(true, MatchCompareFieldType.Score)};
                return null;
        }

        int sortIndex = 0;
        var sortArray = new OrderMetadata[8];

        foreach (var field in orderByFields)
        {
            if (field.OrderingType == OrderByFieldType.Random)
            {
                throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support OrderByRandom.");
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                if (field.Ascending)
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, true);
                else
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score);

                continue;
            }

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                var spatialField = getSpatialField(field.Name);
                var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                    builderParameters.DynamicFields, indexMapping, queryMapping, false);

                int lastArgument;
                IPoint point;
                switch (field.Method)
                {
                    case MethodType.Spatial_Circle:
                        var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(cLatitude, cLongitude).Center;
                        break;
                    case MethodType.Spatial_Wkt:
                        var wkt = field.Arguments[0].GetString(query.QueryParameters);
                        SpatialUnits? spatialUnits = null;
                        lastArgument = 1;
                        if (field.Arguments.Length > 1)
                        {
                            spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);
                            lastArgument = 2;
                        }

                        point = spatialField.ReadShape(wkt, spatialUnits).Center;
                        break;
                    case MethodType.Spatial_Point:
                        var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                        var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(pLatitude, pLongitude).Center;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                var roundTo = field.Arguments.Length > lastArgument
                    ? field.Arguments[lastArgument].GetDouble(query.QueryParameters)
                    : 0D;

                sortArray[sortIndex++] = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
                    spatialField.Units is SpatialUnits.Kilometers
                        ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers 
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles);
                continue;
            }

            var orderingType = field.OrderingType;
            if (index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics, builderParameters.DynamicFields,
                indexMapping, queryMapping, false);
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence);
        }

        return sortArray[0..sortIndex];
    }

    private static IQueryMatch OrderBy(Parameters builderParameters, IQueryMatch match, ReadOnlySpan<OrderMetadata> orderMetadata)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;
        switch (orderMetadata.Length)
        {
            //Note: we want to use generics up to 3 comparers. This way we gonna avoid virtual calls in most cases.
            case 0:
                return match;
            case 1:
                return indexSearcher.OrderBy(match, orderMetadata[0], take);

            case 2:
            {
                var firstComparerType = QueryBuilderHelper.GetComparerType(orderMetadata[0].Ascending, orderMetadata[0]);
                var secondComparerType = QueryBuilderHelper.GetComparerType(orderMetadata[1].Ascending, orderMetadata[1]);
                return (firstComparerType, secondComparerType) switch
                {
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                        indexSearcher, match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                        indexSearcher, match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                        indexSearcher, match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                        indexSearcher, match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        default(BoostingComparer),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        default(BoostingComparer)),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                    (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                        match,
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                        new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),

                    var (type1, type2) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2})")
                };
            }
            case 3:
            {
                return (QueryBuilderHelper.GetComparerType(orderMetadata[0].Ascending, orderMetadata[0]),
                        QueryBuilderHelper.GetComparerType(orderMetadata[1].Ascending, orderMetadata[1]),
                        QueryBuilderHelper.GetComparerType(orderMetadata[2].Ascending, orderMetadata[2])
                    ) switch
                    {
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new LegacySortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),

                        var (type1, type2, type3) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2}, {type3})")
                    };
            }
        }

        var comparers = new IMatchComparer[orderMetadata.Length];
        for (int i = 0; i < orderMetadata.Length; ++i)
        {
            var order = orderMetadata[i];
            comparers[i] = (order.Ascending, order.FieldType) switch
            {
                (true, MatchCompareFieldType.Alphanumeric) => new LegacySortingMatch.AlphanumericAscendingMatchComparer(indexSearcher,  order),
                (false, MatchCompareFieldType.Alphanumeric) => new LegacySortingMatch.AlphanumericDescendingMatchComparer(indexSearcher,  order),
                (true, MatchCompareFieldType.Spatial) => new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[i]),
                (false, MatchCompareFieldType.Spatial) => new LegacySortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[i]),
                (_, MatchCompareFieldType.Score) => default(BoostingComparer),
                (true, _) => new LegacySortingMatch.AscendingMatchComparer(indexSearcher,  order),
                (false, _) => new LegacySortingMatch.DescendingMatchComparer(indexSearcher,  order),
            };
        }

        return orderMetadata.Length switch
        {
            2 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1]),
            3 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2]),
            4 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2], comparers[3]),
            5 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4]),
            6 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5]),
            7 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5], comparers[6]),
            8 => SortingMultiMatch.Create(indexSearcher, match, comparers[0], comparers[1], comparers[2], comparers[3], comparers[4], comparers[5], comparers[6],
                comparers[7]),
            _ => throw new InvalidQueryException("Maximum amount of comparers in ORDER BY clause is 8.")
        };
    }
}
