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
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.CoraxMoreLikeThisQuery;


namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static class CoraxQueryBuilder
{
    private const int TakeAll = -1;
    private const bool HasNoInnerBinary = false;

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

    internal static IQueryMatch BuildQuery(QueryEnvironment queryEnvironment, out bool isBinary)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;
            var metadata = queryEnvironment.Query.Metadata;
            var indexSearcher = queryEnvironment.IndexSearcher;
            var allEntries = indexSearcher.Memoize(indexSearcher.AllEntries());
            isBinary = false;

            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery<NullScoreFunction>(queryEnvironment, metadata.Query.Where, default, out isBinary);
                coraxQuery = MateralizeWhenNeeded(coraxQuery, ref isBinary);

            }
            else
            {
                coraxQuery = allEntries.Replay();
            }

            isBinary |= coraxQuery is BinaryMatch;

            if (metadata.Query.OrderBy is not null)
            {
                var sortMetadata = GetSortMetadata(queryEnvironment);
                coraxQuery = OrderBy(queryEnvironment, coraxQuery, sortMetadata);
            }
            // The parser already throws parse exception if there is a syntax error.
            // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
            return coraxQuery;
        }
    }



    private static IQueryMatch ToCoraxQuery<TScoreFunction>(QueryEnvironment queryEnvironment, QueryExpression expression, TScoreFunction scoreFunction, out bool hasBinary, bool exact = false, int? proximity = null)
        where TScoreFunction : IQueryScoreFunction
    {
        var indexSearcher = queryEnvironment.IndexSearcher;
        var metadata = queryEnvironment.Metadata;
        var parameters = queryEnvironment.Parameters;

        hasBinary = false;
        if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            QueryBuilderHelper.ThrowQueryTooComplexException(metadata, parameters);

        if (expression is null)
            return queryEnvironment.AllEntries.Replay();

        if (expression is BinaryExpression where)
        {
            queryEnvironment.BuildSteps?.Add($"Where: {expression.Type} - {expression} (operator: {where.Operator})");
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
                                return TranslateBetweenQuery(queryEnvironment, bq, scoreFunction, exact);
                        }

                        switch (@where.Left, @where.Right)
                        {
                            case (NegatedExpression ne1, NegatedExpression ne2):
                                left = ToCoraxQuery(queryEnvironment, ne1.Expression, scoreFunction, out var leftInnerBinary, exact);
                                right = ToCoraxQuery(queryEnvironment, ne2.Expression, scoreFunction, out var rightInnerBinary, exact);

                                TryMergeTwoNodes(indexSearcher, queryEnvironment.AllEntries, ref left, ref right, out var merged, scoreFunction, true);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.AndNot(queryEnvironment.AllEntries.Replay(), indexSearcher.Or(left, right));

                            case (NegatedExpression ne1, _):
                                left = ToCoraxQuery(queryEnvironment, @where.Right, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(queryEnvironment, ne1.Expression, scoreFunction, out rightInnerBinary, exact);

                                TryMergeTwoNodes(indexSearcher, queryEnvironment.AllEntries, ref left, ref right, out merged, scoreFunction, true);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.AndNot(right, left);

                            case (_, NegatedExpression ne1):
                                left = ToCoraxQuery(queryEnvironment, @where.Left, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(queryEnvironment, ne1.Expression, scoreFunction, out rightInnerBinary, exact);

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                TryMergeTwoNodes(indexSearcher, queryEnvironment.AllEntries, ref left, ref right, out merged, scoreFunction, true);
                                return indexSearcher.AndNot(left, right);

                            default:
                                left = ToCoraxQuery(queryEnvironment, @where.Left, scoreFunction, out leftInnerBinary, exact);
                                right = ToCoraxQuery(queryEnvironment, @where.Right, scoreFunction, out rightInnerBinary, exact);


                                if (TryMergeTwoNodes(indexSearcher, queryEnvironment.AllEntries, ref left, ref right, out merged, scoreFunction))
                                    return merged;

                                hasBinary = leftInnerBinary | rightInnerBinary;
                                return indexSearcher.And(left, right);
                        }
                    }
                case OperatorType.Or:
                    {
                        var left = ToCoraxQuery(queryEnvironment, @where.Left, scoreFunction, out var leftInnerBinary, exact);
                        var right = ToCoraxQuery(queryEnvironment, @where.Right, scoreFunction, out var rightInnerBinary, exact);

                        queryEnvironment.BuildSteps?.Add(
                            $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

                        TryMergeTwoNodes(indexSearcher, queryEnvironment.AllEntries, ref left, ref right, out var _, scoreFunction, true);
                        hasBinary = leftInnerBinary | rightInnerBinary;

                        return indexSearcher.Or(left, right);
                    }
                default:
                    {
                        var operation = QueryBuilderHelper.TranslateUnaryMatchOperation(where.Operator);

                        QueryExpression right = where.Right;

                        if (where.Right is MethodExpression rme)
                        {
                            right = QueryBuilderHelper.EvaluateMethod(queryEnvironment, rme);
                        }


                        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, where.Left);

                        exact = QueryBuilderHelper.IsExact(queryEnvironment, exact, fieldName);

                        var (value, valueType) = QueryBuilderHelper.GetValue(queryEnvironment, right, true);

                        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);

                        CoraxHighlightingTermIndex highlightingTerm = null;
                        bool? isHighlighting = queryEnvironment.HighlightingTerms?.TryGetValue(fieldName, out highlightingTerm);
                        if (isHighlighting.HasValue && isHighlighting.Value == false)
                        {
                            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
                            queryEnvironment.HighlightingTerms.TryAdd(fieldName, highlightingTerm);
                        }

                        var match = valueType switch
                        {
                            ValueTokenType.Double => new CoraxBooleanItem(indexSearcher, fieldName.Value, fieldId, value, operation, scoreFunction),
                            ValueTokenType.Long => new CoraxBooleanItem(indexSearcher, fieldName.Value, fieldId, value, operation, scoreFunction),
                            ValueTokenType.True or
                                ValueTokenType.False or
                                ValueTokenType.Null or
                                ValueTokenType.String or
                                ValueTokenType.Parameter => HandleStringUnaryMatch(queryEnvironment),
                            _ => throw new NotSupportedException($"Unhandled token type: {valueType}")

                        };

                        if (highlightingTerm != null && valueType is ValueTokenType.Double or ValueTokenType.Long)
                        {
                            highlightingTerm.Values = value.ToString();
                        }

                        return match;


                        CoraxBooleanItem HandleStringUnaryMatch(QueryEnvironment queryEnvironment)
                        {
                            if (exact && queryEnvironment.Metadata.IsDynamic)
                            {
                                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                                fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);
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
            queryEnvironment.BuildSteps?.Add($"Negated: {expression.Type} - {ne}");

            // 'not foo and bar' should be parsed as:
            // (not foo) and bar, instead of not (foo and bar)
            if (ne.Expression is BinaryExpression nbe &&
                nbe.Parenthesis == false &&
                (nbe.Operator == OperatorType.And || nbe.Operator == OperatorType.Or)
               )
            {
                var newExpr = new BinaryExpression(new NegatedExpression(nbe.Left),
                    nbe.Right, nbe.Operator);
                return ToCoraxQuery(queryEnvironment, newExpr, scoreFunction, out hasBinary, exact);
            }

            return ToCoraxQuery(queryEnvironment, ne.Expression, scoreFunction, out hasBinary, exact);
        }

        if (expression is BetweenExpression be)
        {
            queryEnvironment.BuildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(queryEnvironment, be, scoreFunction, exact);
        }

        if (expression is InExpression ie)
        {
            queryEnvironment.BuildSteps?.Add($"In: {expression.Type} - {ie}");

            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, ie.Source);

            CoraxHighlightingTermIndex highlightingTerm = null;
            if (queryEnvironment.HighlightingTerms != null)
            {
                highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
                queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
            }

            exact = QueryBuilderHelper.IsExact(queryEnvironment, exact, fieldName);
            if (exact && queryEnvironment.Metadata.IsDynamic)
            {
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                if (queryEnvironment.HighlightingTerms != null)
                {
                    highlightingTerm.DynamicFieldName = fieldName;
                    queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
                }
            }

            var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);

            if (ie.All)
            {
                var uniqueMatches = new HashSet<string>();
                foreach (var tuple in QueryBuilderHelper.GetValuesForIn(queryEnvironment, ie))
                {
                    if (exact && queryEnvironment.Metadata.IsDynamic)
                        fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                    uniqueMatches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
                }

                return indexSearcher.AllInQuery(fieldName, uniqueMatches, fieldId);
            }

            var matches = new List<string>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(queryEnvironment, ie))
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
            queryEnvironment.BuildSteps?.Add($"True: {expression.Type} - {expression}");

            return queryEnvironment.AllEntries.Replay();
        }

        if (expression is MethodExpression me)
        {
            var methodName = me.Name.Value;
            var methodType = QueryMethod.GetMethodType(methodName);

            queryEnvironment.BuildSteps?.Add($"Method: {expression.Type} - {me} - method: {methodType}, {methodName}");

            switch (methodType)
            {
                case MethodType.Search:
                    return HandleSearch(queryEnvironment, me, proximity, scoreFunction);
                case MethodType.Boost:
                    return HandleBoost(queryEnvironment, me, exact, out hasBinary);
                case MethodType.StartsWith:
                    return HandleStartsWith(queryEnvironment, me, exact, scoreFunction);
                case MethodType.EndsWith:
                    return HandleEndsWith(queryEnvironment, me, scoreFunction, exact);
                case MethodType.Exists:
                    return HandleExists(queryEnvironment, me, scoreFunction);
                case MethodType.Exact:
                    return HandleExact(queryEnvironment, me, scoreFunction, out hasBinary, proximity);
                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    return HandleSpatial(queryEnvironment, me, methodType);
                case MethodType.Regex:
                    return HandleRegex(queryEnvironment, me, scoreFunction);
                case MethodType.MoreLikeThis:
                    return queryEnvironment.AllEntries.Replay();
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", metadata.QueryText, parameters);
    }

    public static MoreLikeThisQuery BuildMoreLikeThisQuery(QueryEnvironment queryEnvironment, QueryExpression whereExpression, out bool isBinary)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            var filterQuery = BuildQuery(queryEnvironment, out isBinary);
            filterQuery = MateralizeWhenNeeded(filterQuery, ref isBinary);

            var moreLikeThisQuery = ToMoreLikeThisQuery(queryEnvironment, whereExpression, out isBinary, out var baseDocument, out var options);
            moreLikeThisQuery = MateralizeWhenNeeded(moreLikeThisQuery, ref isBinary);

            return new MoreLikeThisQuery { BaseDocument = baseDocument, BaseDocumentQuery = moreLikeThisQuery, FilterQuery = filterQuery, Options = options };
        }
    }

    private static IQueryMatch ToMoreLikeThisQuery(QueryEnvironment queryEnvironment, QueryExpression whereExpression, out bool isBinary, out string baseDocument, out BlittableJsonReaderObject options)
    {
        var indexSearcher = queryEnvironment.IndexSearcher;
        var metadata = queryEnvironment.Metadata;
        var parameters = queryEnvironment.Parameters;
        var serverContext = queryEnvironment.ServerContext;
        var context = queryEnvironment.Context;
        isBinary = false;
        baseDocument = null;
        options = null;

        var moreLikeThisExpression = QueryBuilderHelper.FindMoreLikeThisExpression(whereExpression);
        if (moreLikeThisExpression == null)
            throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

        if (moreLikeThisExpression.Arguments.Count == 2)
        {
            var value = QueryBuilderHelper.GetValue(queryEnvironment, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
            if (value.Type == ValueTokenType.String)
                options = IndexOperationBase.ParseJsonStringIntoBlittable(QueryBuilderHelper.GetValueAsString(value.Value), context);
            else
                options = value.Value as BlittableJsonReaderObject;
        }

        var firstArgument = moreLikeThisExpression.Arguments[0];
        if (firstArgument is BinaryExpression binaryExpression)
            return ToCoraxQuery(queryEnvironment, binaryExpression, default(NullScoreFunction), out isBinary);

        isBinary = false;
        var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(queryEnvironment, firstArgument).Value);
        if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
        {

            if (firstArgumentBool)
                return indexSearcher.AllEntries();

            return indexSearcher.EmptySet(); // empty boolean query yields 0 documents
        }

        baseDocument = firstArgumentValue;
        return null;
    }

    private static bool TryMergeTwoNodes<TScoreFunction>(IndexSearcher indexSearcher, MemoizationMatchProvider<AllEntriesMatch> allEntries, ref IQueryMatch lhs,
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


    private static IQueryMatch HandleExact<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, TScoreFunction scoreFunction, out bool hasBinary, int? proximity = null)
        where TScoreFunction : IQueryScoreFunction
    {
        return ToCoraxQuery(queryEnvironment, expression.Arguments[0], scoreFunction, out hasBinary, true, proximity);
    }

    private static CoraxBooleanItem TranslateBetweenQuery<TScoreFunction>(QueryEnvironment queryEnvironment, BetweenExpression be, TScoreFunction scoreFunction, bool exact)

        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, be.Source);
        var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(queryEnvironment, be.Min);
        var (valueSecond, valueSecondType) = QueryBuilderHelper.GetValue(queryEnvironment, be.Max);
        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);
        var leftSideOperation = be.MinInclusive ? UnaryMatchOperation.GreaterThanOrEqual : UnaryMatchOperation.GreaterThan;
        var rightSideOperation = be.MaxInclusive ? UnaryMatchOperation.LessThanOrEqual : UnaryMatchOperation.LessThan;



        if ((valueFirstType, valueSecondType) is (ValueTokenType.Double, ValueTokenType.Double) or (ValueTokenType.Long, ValueTokenType.Long))
        {
            if (queryEnvironment.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
            }
        }
        return (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(queryEnvironment),
            _ => new CoraxBooleanItem(queryEnvironment.IndexSearcher, fieldName, fieldId, valueFirst, valueSecond, UnaryMatchOperation.Between, leftSideOperation, rightSideOperation, scoreFunction)
        };

        CoraxBooleanItem HandleStringBetween(QueryEnvironment queryEnvironment)
        {
            exact = QueryBuilderHelper.IsExact(queryEnvironment.Index, exact, fieldName);
            var valueFirstAsString = QueryBuilderHelper.CoraxGetValueAsString(valueFirst);
            var valueSecondAsString = QueryBuilderHelper.CoraxGetValueAsString(valueSecond);

            if (queryEnvironment.HighlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
            }

            return new CoraxBooleanItem(queryEnvironment.IndexSearcher, fieldName, fieldId, valueFirstAsString, valueSecondAsString, UnaryMatchOperation.Between, leftSideOperation, rightSideOperation, scoreFunction);
        }
    }

    private static IQueryMatch HandleExists<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, expression.Arguments[0]);

        return queryEnvironment.IndexSearcher.ExistsQuery(fieldName, scoreFunction);
    }

    private static IQueryMatch HandleStartsWith<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, bool exact, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, expression.Arguments[0]);
        var (value, valueType) = QueryBuilderHelper.GetValue(queryEnvironment, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, queryEnvironment.Metadata.QueryText, queryEnvironment.Parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        CoraxHighlightingTermIndex highlightingTerm = null;
        if (queryEnvironment.HighlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
            queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(queryEnvironment.Index, exact, fieldName);
        if (exact && queryEnvironment.Metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (queryEnvironment.HighlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                queryEnvironment.HighlightingTerms[fieldName] = highlightingTerm;
            }
        }

        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);
        return queryEnvironment.IndexSearcher.StartWithQuery(fieldName, valueAsString, scoreFunction: scoreFunction, fieldId: fieldId);
    }

    private static IQueryMatch HandleEndsWith<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, TScoreFunction scoreFunction, bool exact)
        where TScoreFunction : IQueryScoreFunction
    {
        var indexSearcher = queryEnvironment.IndexSearcher;
        var metadata = queryEnvironment.Metadata;
        var parameters = queryEnvironment.Parameters;
        var highlightingTerms = queryEnvironment.HighlightingTerms;


        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, expression.Arguments[0]);
        var (value, valueType) = QueryBuilderHelper.GetValue(queryEnvironment, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);

        CoraxHighlightingTermIndex highlightingTerm = null;
        if (highlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
            highlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(queryEnvironment, exact, fieldName);
        if (exact && metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (highlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                highlightingTerms[fieldName] = highlightingTerm;
            }
        }

        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName, exact: exact);
        return indexSearcher.EndsWithQuery(fieldName, valueAsString, scoreFunction: scoreFunction, fieldId: fieldId);
    }

    private static IQueryMatch HandleBoost(QueryEnvironment queryEnvironment, MethodExpression expression, bool exact, out bool hasBinary)
    {
        var metadata = queryEnvironment.Metadata;
        var parameters = queryEnvironment.Parameters;
        var indexSearcher = queryEnvironment.IndexSearcher;

        if (expression.Arguments.Count != 2)
        {
            throw new InvalidQueryException($"Boost(expression, boostVal) requires two arguments, but was called with {expression.Arguments.Count}",
                metadata.QueryText, parameters);
        }


        float boost;
        var (val, type) = QueryBuilderHelper.GetValue(queryEnvironment, expression.Arguments[1]);
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
                        metadata.QueryText, parameters);
                }

                break;
            default:
                throw new InvalidQueryException($"Unable to find boost value: {val} ({type})",
                    metadata.QueryText, parameters);
        }


        var rawQuery = ToCoraxQuery(queryEnvironment, expression.Arguments[0], default(NullScoreFunction), out hasBinary, exact);

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

    private static IQueryMatch HandleSearch<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, int? proximity, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var metadata = queryEnvironment.Metadata;
        var highlightingTerms = queryEnvironment.HighlightingTerms;
        var parameters = queryEnvironment.Parameters;
        var indexSearcher = queryEnvironment.IndexSearcher;


        QueryFieldName fieldName;
        var isDocumentId = false;
        switch (expression.Arguments[0])
        {
            case FieldExpression ft:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, ft);
                break;
            case ValueExpression vt:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, vt);
                break;
            case MethodExpression me when QueryMethod.GetMethodType(me.Name.Value) == MethodType.Id:
                fieldName = QueryFieldName.DocumentId;
                isDocumentId = true;
                break;
            default:
                throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);
        }


        var (value, valueType) = QueryBuilderHelper.GetValue(queryEnvironment, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, queryEnvironment.Metadata.QueryText, queryEnvironment.Parameters);

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

        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName);

        if (proximity.HasValue)
        {
            throw new NotSupportedException($"{nameof(Corax)} doesn't support proximity over search() method");
        }

        CoraxConstants.Search.Operator @operator = CoraxConstants.Search.Operator.Or;
        if (expression.Arguments.Count == 3)
        {
            var fieldExpression = (FieldExpression)expression.Arguments[2];
            if (fieldExpression.Compound.Count != 1)
                QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);

            var op = fieldExpression.Compound[0];
            if (string.Equals("AND", op.Value, StringComparison.OrdinalIgnoreCase))
                @operator = Constants.Search.Operator.And;
            else if (string.Equals("OR", op.Value, StringComparison.OrdinalIgnoreCase))
                @operator = Constants.Search.Operator.Or;
            else
                QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);
        }


        if (queryEnvironment.IndexFieldsMapping.TryGetByFieldId(fieldId, out var binding) && binding.Analyzer is not LuceneAnalyzerAdapter)
        {
            return indexSearcher.SearchQuery(fieldName, valueAsString, scoreFunction, @operator, fieldId, false, true);
        }

        return indexSearcher.SearchQuery(fieldName, valueAsString, scoreFunction, @operator, fieldId);
    }

    private static IQueryMatch HandleSpatial(QueryEnvironment queryEnvironment, MethodExpression expression, MethodType spatialMethod)
    {
        var metadata = queryEnvironment.Metadata;
        string fieldName;
        if (metadata.IsDynamic == false)
            fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, expression.Arguments[0]);
        else
        {
            var spatialExpression = (MethodExpression)expression.Arguments[0];
            fieldName = metadata.GetSpatialFieldName(spatialExpression, queryEnvironment.Parameters);
        }

        var fieldId = QueryBuilderHelper.GetFieldId(queryEnvironment, fieldName);
        var shapeExpression = (MethodExpression)expression.Arguments[1];

        var distanceErrorPct = RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
        if (expression.Arguments.Count == 3)
        {
            var distanceErrorPctValue = QueryBuilderHelper.GetValue(queryEnvironment, (ValueExpression)expression.Arguments[2]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

            distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
        }

        var spatialField = queryEnvironment.Factories.GetSpatialFieldFactory(fieldName);

        var methodName = shapeExpression.Name;
        var methodType = QueryMethod.GetMethodType(methodName.Value);

        IShape shape = null;
        switch (methodType)
        {
            case MethodType.Spatial_Circle:
                shape = QueryBuilderHelper.HandleCircle(queryEnvironment, fieldName, shapeExpression, spatialField, out _);
                break;
            case MethodType.Spatial_Wkt:
                shape = QueryBuilderHelper.HandleWkt(queryEnvironment, fieldName, shapeExpression, spatialField, out _);
                break;
            default:
                QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, queryEnvironment.Parameters);
                break;
        }

        Debug.Assert(shape != null);

        var operation = spatialMethod switch
        {
            MethodType.Spatial_Within => global::Corax.Utils.Spatial.SpatialRelation.Within,
            MethodType.Spatial_Disjoint => global::Corax.Utils.Spatial.SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => global::Corax.Utils.Spatial.SpatialRelation.Intersects,
            MethodType.Spatial_Contains => global::Corax.Utils.Spatial.SpatialRelation.Contains,
            _ => (global::Corax.Utils.Spatial.SpatialRelation)QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, queryEnvironment.Parameters)
        };


        //var args = new SpatialArgs(operation, shape) {DistErrPct = distanceErrorPct};

        return queryEnvironment.IndexSearcher.SpatialQuery(fieldName, fieldId, distanceErrorPct, shape, spatialField.GetContext(), operation);
    }

    private static IQueryMatch HandleRegex<TScoreFunction>(QueryEnvironment queryEnvironment, MethodExpression expression, TScoreFunction scoreFunction = default)
    where TScoreFunction : IQueryScoreFunction
    {
        if (expression.Arguments.Count != 2)
            throw new ArgumentException(
                $"Regex method was invoked with {expression.Arguments.Count} arguments ({expression})" +
                " while it should be invoked with 2 arguments e.g. Regex(foo.Name,\"^[a-z]+?\")");

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(queryEnvironment, expression.Arguments[0]);
        var (value, valueType) = QueryBuilderHelper.GetValue(queryEnvironment, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String && !(valueType == ValueTokenType.Parameter && IsStringFamily(value)))
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("regex", ValueTokenType.String, valueType, queryEnvironment.Metadata.QueryText, queryEnvironment.Parameters);
        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        return queryEnvironment.IndexSearcher.RegexQuery<TScoreFunction>(fieldName, scoreFunction, queryEnvironment.Factories.GetRegexFactory(valueAsString));

        bool IsStringFamily(object value)
        {
            return value is string || value is StringSegment || value is LazyStringValue;
        }
    }

    public static OrderMetadata[] GetSortMetadata(QueryEnvironment queryEnvironment)
    {
        var query = queryEnvironment.Query;
        var index = queryEnvironment.Index;
        var getSpatialField = queryEnvironment.Factories.GetSpatialFieldFactory;
        var indexMapping = queryEnvironment.IndexFieldsMapping;
        var queryMapping = queryEnvironment.FieldsToFetch;


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

    private static IQueryMatch OrderBy(QueryEnvironment queryEnvironment, IQueryMatch match, ReadOnlySpan<OrderMetadata> orderMetadata)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = queryEnvironment.IndexSearcher;
        var take = queryEnvironment.Take;
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
