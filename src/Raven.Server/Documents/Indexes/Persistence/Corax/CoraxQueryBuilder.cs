using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Spatial4n.Shapes;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using Query = Raven.Server.Documents.Queries.AST.Query;
using CoraxConstants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Corax;


namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static class CoraxQueryBuilder
{
    internal const int TakeAll = -1;
    private const bool HasNoInnerBinary = false;
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
        public readonly MemoizationMatchProviderRef<AllEntriesMatch> AllEntries;
        public readonly QueryMetadata Metadata;
    
        internal Parameters(IndexSearcher searcher, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, IndexQueryServerSide query, Index index, BlittableJsonReaderObject queryParameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping, FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, List<string> buildSteps = null)
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
            AllEntries = new MemoizationMatchProviderRef<AllEntriesMatch>(IndexSearcher.Memoize(IndexSearcher.AllEntries()));
            Metadata = query.Metadata;
        }
    }

    internal static IQueryMatch MateralizeWhenNeeded(IQueryMatch source, ref bool isBinary)
    {
        if (source is CoraxBooleanQuery cbq)
        {
            source = cbq.Materialize();
            isBinary |= cbq.HasInnerBinary;
        }

        if (source is CoraxBooleanItem cbi)
            source = cbi.Materialize();

        return source;
    }

    internal static IQueryMatch BuildQuery(Parameters builderParameters, out bool isBinary)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;
            var metadata = builderParameters.Query.Metadata;
            var indexSearcher = builderParameters.IndexSearcher;
            var allEntries = indexSearcher.Memoize(indexSearcher.AllEntries());
            isBinary = HasNoInnerBinary;

            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery<NullScoreFunction>(builderParameters, metadata.Query.Where, default, out isBinary);
                coraxQuery = MateralizeWhenNeeded(coraxQuery, ref isBinary);

            }
            else
            {
                coraxQuery = allEntries.Replay();
            }

            isBinary |= coraxQuery is BinaryMatch;

            if (metadata.Query.OrderBy is not null)
            {
                var sortMetadata = GetSortMetadata(builderParameters);
                coraxQuery = OrderBy(builderParameters, coraxQuery, sortMetadata);
            }
            // The parser already throws parse exception if there is a syntax error.
            // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
            return coraxQuery;
        }
    }
    
    private static IQueryMatch ToCoraxQuery<TScoreFunction>(Parameters builderParameters, QueryExpression expression, TScoreFunction scoreFunction, out bool hasBinary, bool exact = false, int? proximity = null)
        where TScoreFunction : IQueryScoreFunction
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var serverContext = builderParameters.ServerContext;
        var documentsContext = builderParameters.DocumentsContext;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        
        hasBinary = false;
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
                                    MinInclusive = lbe.Operator == OperatorType.GreaterThanEqual,
                                    MaxInclusive = rbe.Operator == OperatorType.LessThanEqual,
                                };
                            }

                            if (lbe.IsLessThan && rbe.IsGreaterThan)
                            {
                                bq = new BetweenExpression(lbe.Left, rightVal, leftVal)
                                {
                                    MinInclusive = rbe.Operator == OperatorType.GreaterThanEqual,
                                    MaxInclusive = lbe.Operator == OperatorType.LessThanEqual
                                };
                            }

                            if (bq != null)
                                return TranslateBetweenQuery(builderParameters, bq, scoreFunction, exact);
                        }

                        switch (@where.Left, @where.Right)
                        {
                            case (NegatedExpression ne1, NegatedExpression ne2):
                                left = ToCoraxQuery(builderParameters, ne1.Expression, scoreFunction, out var leftInnerBinary, exact);
                                right = ToCoraxQuery(builderParameters, ne2.Expression, scoreFunction, out var rightInnerBinary, exact);

                                TryMergeTwoNodes(indexSearcher, builderParameters.AllEntries, ref left, ref right, out var merged, scoreFunction, true);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.AndNot(builderParameters.AllEntries.Replay(), indexSearcher.Or(left, right));

                            case (NegatedExpression ne1, _):
                                left = ToCoraxQuery(builderParameters, @where.Right, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(builderParameters, ne1.Expression, scoreFunction, out rightInnerBinary, exact);

                                TryMergeTwoNodes(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged, scoreFunction, true);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.AndNot(right, left);

                            case (_, NegatedExpression ne1):
                                left = ToCoraxQuery(builderParameters, @where.Left, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(builderParameters, ne1.Expression, scoreFunction, out rightInnerBinary, exact);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                TryMergeTwoNodes(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged, scoreFunction, true);
                                return indexSearcher.AndNot(left, right);

                            default:
                                left = ToCoraxQuery(builderParameters, @where.Left, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(builderParameters, @where.Right, scoreFunction, out rightInnerBinary, exact);


                                if (TryMergeTwoNodes(indexSearcher, builderParameters.AllEntries, ref left, ref right, out merged, scoreFunction))
                                    return merged;

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.And(left, right);
                        }
                    }
                case OperatorType.Or:
                    {
                        var left = ToCoraxQuery(builderParameters, @where.Left, scoreFunction, out var leftInnerBinary, exact);
                        var right = ToCoraxQuery(builderParameters, @where.Right, scoreFunction, out var rightInnerBinary, exact);

                        builderParameters.BuildSteps?.Add(
                            $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

                        TryMergeTwoNodes(indexSearcher, builderParameters.AllEntries, ref left, ref right, out var _, scoreFunction, true);
                        hasBinary = leftInnerBinary | rightInnerBinary;

                        return indexSearcher.Or(left, right);
                    }
                default:
                    {
                        var operation = QueryBuilderHelper.TranslateUnaryMatchOperation(where.Operator);

                        QueryExpression right = where.Right;

                        if (where.Right is MethodExpression rme)
                        {
                            right = QueryBuilderHelper.EvaluateMethod(metadata.Query, metadata, serverContext, documentsContext, rme, queryParameters);
                        }


                        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, where.Left, metadata);

                        exact = QueryBuilderHelper.IsExact(builderParameters.Index, exact, fieldName);

                        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, right, true);

                        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);

                        CoraxHighlightingTermIndex highlightingTerm = null;
                        bool? isHighlighting = builderParameters.HighlightingTerms?.TryGetValue(fieldName, out highlightingTerm);
                        if (isHighlighting.HasValue && isHighlighting.Value == false)
                        {
                            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
                            builderParameters.HighlightingTerms.TryAdd(fieldName, highlightingTerm);
                        }

                        var match = valueType switch
                        {
                            ValueTokenType.Double => new CoraxBooleanItem(indexSearcher, fieldName.Value, fieldId, value, operation, scoreFunction),
                            ValueTokenType.Long => new CoraxBooleanItem(indexSearcher, fieldName.Value, fieldId, value, operation, scoreFunction),
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


                        CoraxBooleanItem HandleStringUnaryMatch(Parameters queryEnvironment)
                        {
                            if (exact && queryEnvironment.Metadata.IsDynamic)
                            {
                                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                                fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);
                            }

                            if (value == null)
                            {
                                if (operation is UnaryMatchOperation.Equals)
                                    return new CoraxBooleanItem(indexSearcher, fieldName, fieldId, null, UnaryMatchOperation.Equals, scoreFunction);
                                else if (operation is UnaryMatchOperation.NotEquals)
                                    //Please consider if we ever need to support this.
                                    return new CoraxBooleanItem(indexSearcher, fieldName, fieldId, null, UnaryMatchOperation.NotEquals, scoreFunction);
                                else
                                    throw new NotSupportedException($"Unhandled operation: {operation}");
                            }

                            var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                            if (highlightingTerm != null)
                                highlightingTerm.Values = valueAsString;

                            return new CoraxBooleanItem(indexSearcher, fieldName.Value, fieldId, valueAsString, operation, scoreFunction);
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
                return ToCoraxQuery(builderParameters, newExpr, scoreFunction, out hasBinary, exact);
            }

            return ToCoraxQuery(builderParameters, ne.Expression, scoreFunction, out hasBinary, exact);
        }

        if (expression is BetweenExpression be)
        {
            builderParameters.BuildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(builderParameters, be, scoreFunction, exact);
        }

        if (expression is InExpression ie)
        {
            builderParameters.BuildSteps?.Add($"In: {expression.Type} - {ie}");

            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, ie.Source, metadata);

            CoraxHighlightingTermIndex highlightingTerm = null;
            if (builderParameters.HighlightingTerms != null)
            {
                highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
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

            var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);

            if (ie.All)
            {
                var uniqueMatches = new HashSet<string>();
                foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
                {
                    if (exact && builderParameters.Metadata.IsDynamic)
                        fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                    uniqueMatches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
                }

                return indexSearcher.AllInQuery(fieldName, uniqueMatches, fieldId);
            }

            var matches = new List<string>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
            {
                matches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
            }

            if (highlightingTerm != null)
                highlightingTerm.Values = matches;

            return (scoreFunction) switch
            {
                (NullScoreFunction) => indexSearcher.InQuery(fieldName, matches, fieldId),
                (_) => indexSearcher.InQuery(fieldName, matches, fieldId)
            };
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
                    return HandleSearch(builderParameters, me, proximity, scoreFunction);
                case MethodType.Boost:
                    return HandleBoost(builderParameters, me, exact, out hasBinary);
                case MethodType.StartsWith:
                    return HandleStartsWith(builderParameters, me, exact, scoreFunction);
                case MethodType.EndsWith:
                    return HandleEndsWith(builderParameters, me, scoreFunction, exact);
                case MethodType.Exists:
                    return HandleExists(builderParameters, me, scoreFunction);
                case MethodType.Exact:
                    return HandleExact(builderParameters, me, scoreFunction, out hasBinary, proximity);
                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    return HandleSpatial(builderParameters, me, methodType);
                case MethodType.Regex:
                    return HandleRegex(builderParameters, me, scoreFunction);
                case MethodType.MoreLikeThis:
                    return builderParameters.AllEntries.Replay();
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, queryParameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", metadata.QueryText, queryParameters);
    }

    public static MoreLikeThisQuery.MoreLikeThisQuery BuildMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression, out bool isBinary)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            var filterQuery = BuildQuery(builderParameters, out isBinary);
            filterQuery = MateralizeWhenNeeded(filterQuery, ref isBinary);

            var moreLikeThisQuery = ToMoreLikeThisQuery(builderParameters, whereExpression, out isBinary, out var baseDocument, out var options);
            moreLikeThisQuery = MateralizeWhenNeeded(moreLikeThisQuery, ref isBinary);

            return new MoreLikeThisQuery.MoreLikeThisQuery { BaseDocument = baseDocument, BaseDocumentQuery = moreLikeThisQuery, FilterQuery = filterQuery, Options = options };
        }
    }

    private static IQueryMatch ToMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression, out bool isBinary, out string baseDocument, out BlittableJsonReaderObject options)
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var serverContext = builderParameters.ServerContext;
        var context = builderParameters.DocumentsContext;
        isBinary = false;
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
            return ToCoraxQuery(builderParameters, binaryExpression, default(NullScoreFunction), out isBinary);

        isBinary = false;
        var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, firstArgument).Value);
        if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
        {

            if (firstArgumentBool)
                return indexSearcher.AllEntries();

            return indexSearcher.EmptySet(); // empty boolean query yields 0 documents
        }

        baseDocument = firstArgumentValue;
        return null;
    }

    private static bool TryMergeTwoNodes<TScoreFunction>(IndexSearcher indexSearcher, MemoizationMatchProviderRef<AllEntriesMatch> allEntries, ref IQueryMatch lhs,
        ref IQueryMatch rhs, out CoraxBooleanQuery merged, TScoreFunction scoreFunction, bool reruiredMaterialization = false)
        where TScoreFunction : IQueryScoreFunction
    {
        merged = null;
        switch (lhs, rhs, reruiredMaterialization)
        {
            case (CoraxBooleanQuery lhsBq, CoraxBooleanQuery rhsBq, false):
                if (lhsBq.TryMerge(rhsBq))
                {
                    merged = lhsBq;
                    return true;
                }

                lhs = lhsBq.Materialize();
                rhs = rhsBq.Materialize();
                return false;

            case (CoraxBooleanQuery lhsBq, CoraxBooleanItem rhsBq, false):
                if (lhsBq.TryAnd(rhsBq))
                {
                    merged = lhsBq;
                    return true;
                }

                lhs = lhsBq.Materialize();
                return false;
            case (CoraxBooleanItem lhsBq, CoraxBooleanQuery rhsBq, false):
                if (rhsBq.TryAnd(lhsBq))
                {
                    merged = rhsBq;
                    return true;
                }

                rhs = rhsBq.Materialize();
                return false;

            case (CoraxBooleanItem lhsBq, CoraxBooleanItem rhsBq, false):
                if (CoraxBooleanItem.CanBeMerged(lhsBq, rhsBq))
                {
                    merged = new CoraxBooleanQuery(indexSearcher, allEntries, lhsBq, rhsBq, scoreFunction);
                    return true;
                }

                return false;
            default:
                if (lhs is CoraxBooleanItem cbi)
                    lhs = cbi.Materialize();
                else if (lhs is CoraxBooleanQuery cbq)
                    lhs = cbq.Materialize();
                if (rhs is CoraxBooleanItem cbi1)
                    rhs = cbi1.Materialize();
                else if (rhs is CoraxBooleanQuery cbq1)
                    rhs = cbq1.Materialize();
                return false;
        }
    }


    private static IQueryMatch HandleExact<TScoreFunction>(Parameters builderParameters, MethodExpression expression, TScoreFunction scoreFunction, out bool hasBinary, int? proximity = null)
        where TScoreFunction : IQueryScoreFunction
    {
        return ToCoraxQuery(builderParameters, expression.Arguments[0], scoreFunction, out hasBinary, true, proximity);
    }

    private static CoraxBooleanItem TranslateBetweenQuery<TScoreFunction>(Parameters builderParameters, BetweenExpression be, TScoreFunction scoreFunction, bool exact)
        where TScoreFunction : IQueryScoreFunction
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, be.Source, metadata);
        var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, be.Min);
        var (valueSecond, valueSecondType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, be.Max);
        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);
        var leftSideOperation = be.MinInclusive ? UnaryMatchOperation.GreaterThanOrEqual : UnaryMatchOperation.GreaterThan;
        var rightSideOperation = be.MaxInclusive ? UnaryMatchOperation.LessThanOrEqual : UnaryMatchOperation.LessThan;



        if ((valueFirstType, valueSecondType) is (ValueTokenType.Double, ValueTokenType.Double) or (ValueTokenType.Long, ValueTokenType.Long))
        {
            if (builderParameters.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }
        }
        return (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(),
            _ => new CoraxBooleanItem(builderParameters.IndexSearcher, fieldName, fieldId, valueFirst, valueSecond, UnaryMatchOperation.Between, leftSideOperation, rightSideOperation, scoreFunction)
        };

        CoraxBooleanItem HandleStringBetween()
        {
            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            var valueFirstAsString = QueryBuilderHelper.CoraxGetValueAsString(valueFirst);
            var valueSecondAsString = QueryBuilderHelper.CoraxGetValueAsString(valueSecond);

            if (builderParameters.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                builderParameters.HighlightingTerms[fieldName] = highlightingTerm;
            }

            return new CoraxBooleanItem(builderParameters.IndexSearcher, fieldName, fieldId, valueFirstAsString, valueSecondAsString, UnaryMatchOperation.Between, leftSideOperation, rightSideOperation, scoreFunction);
        }
    }

    private static IQueryMatch HandleExists<TScoreFunction>(Parameters builderParameters, MethodExpression expression, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);

        return builderParameters.IndexSearcher.ExistsQuery(fieldName, scoreFunction);
    }

    private static IQueryMatch HandleStartsWith<TScoreFunction>(Parameters builderParameters, MethodExpression expression, bool exact, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, queryParameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        CoraxHighlightingTermIndex highlightingTerm = null;
        if (builderParameters.HighlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
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

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);
        return builderParameters.IndexSearcher.StartWithQuery(fieldName, valueAsString, scoreFunction: scoreFunction, fieldId: fieldId);
    }

    private static IQueryMatch HandleEndsWith<TScoreFunction>(Parameters builderParameters, MethodExpression expression, TScoreFunction scoreFunction, bool exact)
        where TScoreFunction : IQueryScoreFunction
    {
        var indexSearcher = builderParameters.IndexSearcher;
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var highlightingTerms = builderParameters.HighlightingTerms;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, queryParameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);

        CoraxHighlightingTermIndex highlightingTerm = null;
        if (highlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
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

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch, exact: exact);
        return indexSearcher.EndsWithQuery(fieldName, valueAsString, scoreFunction: scoreFunction, fieldId: fieldId);
    }

    private static IQueryMatch HandleBoost(Parameters builderParameters, MethodExpression expression, bool exact, out bool hasBinary)
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


        var rawQuery = ToCoraxQuery(builderParameters, expression.Arguments[0], default(NullScoreFunction), out hasBinary, exact);

        if (rawQuery is CoraxBooleanItem cbi)
            rawQuery = cbi.Materialize();
        else if (rawQuery is CoraxBooleanQuery cbq)
        {
            rawQuery = cbq.Materialize();
            hasBinary = cbq.HasInnerBinary;
        }

        hasBinary = false;
        var scoreFunction = new ConstantScoreFunction(boost);

        return indexSearcher.Boost(rawQuery, scoreFunction);
    }

    private static IQueryMatch HandleSearch<TScoreFunction>(Parameters builderParameters, MethodExpression expression, int? proximity, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var metadata = builderParameters.Metadata;
        var highlightingTerms = builderParameters.HighlightingTerms;
        var queryParameters = builderParameters.QueryParameters;
        var indexSearcher = builderParameters.IndexSearcher;
        var index = builderParameters.Index;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;

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
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, builderParameters.Metadata.QueryText, builderParameters.QueryParameters);

        Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        if (highlightingTerms != null && highlightingTerms.TryGetValue(fieldName, out var highlightingTerm) == false)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { Values = valueAsString, };

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

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch);

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


        if (builderParameters.IndexFieldsMapping.TryGetByFieldId(fieldId, out var binding) && binding.Analyzer is not LuceneAnalyzerAdapter)
        {
            return indexSearcher.SearchQuery(fieldName, valueAsString, scoreFunction, @operator, fieldId, false, true);
        }

        return indexSearcher.SearchQuery(fieldName, valueAsString, scoreFunction, @operator, fieldId);
    }

    private static IQueryMatch HandleSpatial(Parameters builderParameters, MethodExpression expression, MethodType spatialMethod)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        
        string fieldName;
        if (metadata.IsDynamic == false)
            fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        else
        {
            var spatialExpression = (MethodExpression)expression.Arguments[0];
            fieldName = metadata.GetSpatialFieldName(spatialExpression, builderParameters.QueryParameters);
        }

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexFieldsMapping, fieldsToFetch);
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

        return builderParameters.IndexSearcher.SpatialQuery(fieldName, fieldId, distanceErrorPct, shape, spatialField.GetContext(), operation);
    }

    private static IQueryMatch HandleRegex<TScoreFunction>(Parameters builderParameters, MethodExpression expression, TScoreFunction scoreFunction = default)
    where TScoreFunction : IQueryScoreFunction
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
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("regex", ValueTokenType.String, valueType, builderParameters.Metadata.QueryText, builderParameters.QueryParameters);
        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        return builderParameters.IndexSearcher.RegexQuery<TScoreFunction>(fieldName, scoreFunction, builderParameters.Factories.GetRegexFactory(valueAsString));

        bool IsStringFamily(object value)
        {
            return value is string || value is StringSegment || value is LazyStringValue;
        }
    }

    public static OrderMetadata[] GetSortMetadata(Parameters builderParameters)
    {
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var getSpatialField = builderParameters.Factories.GetSpatialFieldFactory;
        var indexMapping = builderParameters.IndexFieldsMapping;
        var queryMapping = builderParameters.FieldsToFetch;


        var sort = ReadOnlySpan<OrderMetadata>.Empty;
        if (query.PageSize == 0) // no need to sort when counting only
            return null;

        var orderByFields = query.Metadata.OrderBy;

        if (orderByFields == null)
        {
            if (query.Metadata.HasBoost == false && index.HasBoostedFields == false)
                return null;
            return new[] { new OrderMetadata(true, MatchCompareFieldType.Score) };
        }

        int sortIndex = 0;
        var sortArray = new OrderMetadata[8];

        foreach (var field in orderByFields)
        {
            if (field.OrderingType == OrderByFieldType.Random)
            {
                throw new NotSupportedException($"{nameof(Corax)} doesn't support OrderByRandom.");
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                if (field.Ascending)
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, true);
                else
                    sortArray[sortIndex++] = sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score);

                continue;
            }

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                var spatialField = getSpatialField(field.Name);
                var distanceFieldId = QueryBuilderHelper.GetFieldIdForOrderBy(field.Name, index, indexMapping, queryMapping, false);

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

                sortArray[sortIndex++] = new OrderMetadata(field.Name, distanceFieldId, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
                    spatialField.Units is SpatialUnits.Kilometers ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers : global::Corax.Utils.Spatial.SpatialUnits.Miles);
                continue;
            }

            var fieldName = field.Name.Value;
            var fieldId = QueryBuilderHelper.GetFieldIdForOrderBy(fieldName, index, indexMapping, queryMapping, false);
            OrderMetadata? temporaryOrder = null;
            switch (field.OrderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(fieldName, fieldId, field.Ascending, MatchCompareFieldType.Alphanumeric);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(fieldName, fieldId, field.Ascending, MatchCompareFieldType.Integer);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(fieldName, fieldId, field.Ascending, MatchCompareFieldType.Floating);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(fieldName, fieldId, field.Ascending, MatchCompareFieldType.Sequence);
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
                {
                    var order = orderMetadata[0];
                    if (order.HasBoost)
                        return indexSearcher.OrderByScore(match, take: take);

                    return (order.FieldType, order.Ascending) switch
                    {
                        (MatchCompareFieldType.Spatial, _) => indexSearcher.OrderByDistance(in match, in order),
                        (_, true) => indexSearcher.OrderByAscending(match, order.FieldId, order.FieldType, take),
                        (_, false) => indexSearcher.OrderByDescending(match, order.FieldId, order.FieldType, take)
                    };
                }

            case 2:
                {
                    var firstComparerType = QueryBuilderHelper.GetComparerType(orderMetadata[0].Ascending, orderMetadata[0].FieldType, orderMetadata[0].FieldId);
                    var secondComparerType = QueryBuilderHelper.GetComparerType(orderMetadata[1].Ascending, orderMetadata[1].FieldType, orderMetadata[1].FieldId);
                    return (firstComparerType, secondComparerType) switch
                    {
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1])),

                        var (type1, type2) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2})")
                    };
                }
            case 3:
                {
                    return (QueryBuilderHelper.GetComparerType(orderMetadata[0].Ascending, orderMetadata[0].FieldType, orderMetadata[0].FieldId),
                            QueryBuilderHelper.GetComparerType(orderMetadata[1].Ascending, orderMetadata[1].FieldType, orderMetadata[1].FieldId),
                            QueryBuilderHelper.GetComparerType(orderMetadata[2].Ascending, orderMetadata[2].FieldType, orderMetadata[2].FieldId)
                        ) switch
                    {
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            default(BoostingComparer),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[0].FieldId, orderMetadata[0].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingSpatial
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric
                            ) => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[1].FieldId, orderMetadata[1].FieldType),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingSpatial) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingSpatial)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                default(BoostingComparer),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.AscendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Ascending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, orderMetadata[2].FieldId, orderMetadata[2].FieldType)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.Boosting)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                                new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .AscendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[2])),
                        (QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType.DescendingSpatial, QueryBuilderHelper.ComparerType
                            .DescendingSpatial) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[0]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[1]),
                            new SortingMatch.SpatialDescendingMatchComparer(indexSearcher, in orderMetadata[2])),

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
                (true, MatchCompareFieldType.Alphanumeric) => new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, order.FieldId, order.FieldType),
                (false, MatchCompareFieldType.Alphanumeric) => new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, order.FieldId, order.FieldType),
                (true, MatchCompareFieldType.Spatial) => new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[i]),
                (false, MatchCompareFieldType.Spatial) => new SortingMatch.SpatialAscendingMatchComparer(indexSearcher, in orderMetadata[i]),
                (_, MatchCompareFieldType.Score) => default(BoostingComparer),
                (true, _) => new SortingMatch.AscendingMatchComparer(indexSearcher, order.FieldId, order.FieldType),
                (false, _) => new SortingMatch.DescendingMatchComparer(indexSearcher, order.FieldId, order.FieldType),
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
