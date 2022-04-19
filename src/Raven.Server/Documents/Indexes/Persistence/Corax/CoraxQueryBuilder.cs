using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using Query = Raven.Server.Documents.Queries.AST.Query;
using CoraxConstants = Corax.Constants;


namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static class CoraxQueryBuilder
{
    private const int TakeAll = -1;

    public static IQueryMatch BuildQuery(IndexSearcher indexSearcher, TransactionOperationContext serverContext, DocumentsOperationContext context,
        QueryMetadata metadata,
        Index index, BlittableJsonReaderObject parameters, QueryBuilderFactories factories, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null,
        List<string> buildSteps = null, int take = TakeAll)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;

            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery<NullScoreFunction>(indexSearcher, serverContext, context, metadata.Query, metadata.Query.Where, metadata, index, parameters,
                    factories, default, false,
                    queryMapping: queryMapping,
                    buildSteps: buildSteps,
                    indexMapping: indexMapping);
            }
            else
            {
                coraxQuery = indexSearcher.AllEntries();
            }

            if (metadata.Query.OrderBy is not null)
            {
                coraxQuery = OrderBy(indexSearcher, coraxQuery, metadata.Query.OrderBy, metadata.Query, parameters, index, metadata, indexMapping, queryMapping, take);
            }

            // The parser already throws parse exception if there is a syntax error.
            // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
            return coraxQuery;
        }
    }

    private static IQueryMatch ToCoraxQuery<TScoreFunction>(IndexSearcher indexSearcher, TransactionOperationContext serverContext,
        DocumentsOperationContext documentsContext,
        Query query,
        QueryExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, QueryBuilderFactories factories, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping,
        FieldsToFetch queryMapping, bool exact = false, int? proximity = null, bool secondary = false,
        List<string> buildSteps = null, int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            QueryBuilderHelper.ThrowQueryTooComplexException(metadata, parameters);

        if (expression is null)
            return indexSearcher.AllEntries();

        if (expression is BinaryExpression where)
        {
            buildSteps?.Add($"Where: {expression.Type} - {expression} (operator: {where.Operator})");
            switch (where.Operator)
            {
                case OperatorType.And:
                {
                    IQueryMatch left = null;
                    IQueryMatch right = null;
                    switch (@where.Left, @where.Right)
                    {
                        case (NegatedExpression ne1, NegatedExpression ne2):
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne1.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne2.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            return indexSearcher.AndNot(indexSearcher.AllEntries(), indexSearcher.Or(left, right));
                            break;
                        case (NegatedExpression ne1, _):
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne1.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            return indexSearcher.AndNot(left, right);
                        case (_, NegatedExpression ne1):
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne1.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            return indexSearcher.AndNot(left, right);
                        default:
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);

                            return isNegated == false
                                ? indexSearcher.And(left, right)
                                : indexSearcher.Or(left, right);
                    }
                }
                case OperatorType.Or:
                {
                    var left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                        factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                    var right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                        factories, scoreFunction, isNegated, indexMapping, queryMapping, exact, secondary: true, buildSteps: buildSteps, take: take);

                    buildSteps?.Add(
                        $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

                    return isNegated == false
                        ? indexSearcher.Or(left, right)
                        : indexSearcher.And(left, right);
                }
                default:
                {
                    var operation = QueryBuilderHelper.TranslateUnaryMatchOperation(where.Operator);
                    QueryExpression right = where.Right;

                    if (where.Right is MethodExpression rme)
                    {
                        right = QueryBuilderHelper.EvaluateMethod(query, metadata, serverContext, documentsContext, rme, ref parameters);
                    }

                    var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, where.Left, metadata);

                    exact = QueryBuilderHelper.IsExact(index, exact, fieldName);

                    var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, right, true);

                    var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

                    var match = valueType switch
                    {
                        ValueTokenType.Double => indexSearcher.UnaryQuery(indexSearcher.AllEntries(), fieldId, (double)value, operation, take),
                        ValueTokenType.Long => indexSearcher.UnaryQuery(indexSearcher.AllEntries(), fieldId, (long)value, operation, take),
                        ValueTokenType.True or
                        ValueTokenType.False or
                        ValueTokenType.Null or
                        ValueTokenType.String or
                        ValueTokenType.Parameter => HandleStringUnaryMatch(),
                        _ => null
                    };

                    if (match is null)
                        QueryBuilderHelper.ThrowUnhandledValueTokenType(valueType);

                    return match;

                    IQueryMatch HandleStringUnaryMatch()
                    {
                        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                        
                        if (exact && metadata.IsDynamic)
                        {
                            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                            fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);
                        }

                        return indexSearcher.UnaryQuery(indexSearcher.AllEntries(), fieldId, valueAsString, operation, take);
                    }
                }
            }
        }

        if (expression is NegatedExpression ne)
        {
            buildSteps?.Add($"Negated: {expression.Type} - {ne}");

            // 'not foo and bar' should be parsed as:
            // (not foo) and bar, instead of not (foo and bar)
            if (ne.Expression is BinaryExpression nbe &&
                nbe.Parenthesis == false &&
                (nbe.Operator == OperatorType.And || nbe.Operator == OperatorType.Or)
               )
            {
                var newExpr = new BinaryExpression(new NegatedExpression(nbe.Left),
                    nbe.Right, nbe.Operator);
                return ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, newExpr, metadata, index, parameters, factories, scoreFunction, isNegated,
                    indexMapping, queryMapping, exact,
                    buildSteps: buildSteps, take: take);
            }

            return ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne.Expression, metadata, index, parameters, factories, scoreFunction, !isNegated,
                indexMapping, queryMapping, exact,
                buildSteps: buildSteps, take: take);
        }

        if (expression is BetweenExpression be)
        {
            buildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(indexSearcher, query, metadata, index, parameters, exact, be, secondary, scoreFunction, isNegated, indexMapping, queryMapping,
                take);
        }

        if (expression is InExpression ie)
        {
            buildSteps?.Add($"In: {expression.Type} - {ie}");

            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, ie.Source, metadata);
            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            if (exact && metadata.IsDynamic)
            {
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
            }

            var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);


            if (ie.All)
            {
                throw new NotImplementedException($"{nameof(Corax)} doesn't support AllIn");
            }

            var matches = new List<string>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(query, ie, metadata, parameters))
            {
                matches.Add(tuple.Value);
            }

            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => indexSearcher.InQuery(fieldName, matches, fieldId),
                (true, NullScoreFunction) => indexSearcher.NotInQuery(fieldName, indexSearcher.AllEntries(), matches, fieldId),
                (false, _) => indexSearcher.InQuery(fieldName, matches, scoreFunction, fieldId),
                (true, _) => indexSearcher.NotInQuery(fieldName, indexSearcher.AllEntries(), matches, fieldId, scoreFunction)
            };
        }

        if (expression is TrueExpression)
        {
            buildSteps?.Add($"True: {expression.Type} - {expression}");

            return new AllEntriesMatch();
        }

        if (expression is MethodExpression me)
        {
            var methodName = me.Name.Value;
            var methodType = QueryMethod.GetMethodType(methodName);

            buildSteps?.Add($"Method: {expression.Type} - {me} - method: {methodType}, {methodName}");

            switch (methodType)
            {
                case MethodType.Search:
                    return HandleSearch(indexSearcher, query, me, metadata, parameters, proximity, scoreFunction, isNegated, indexMapping, queryMapping, index, take);
                case MethodType.Boost:
                    return HandleBoost(indexSearcher, serverContext, documentsContext, query, me, metadata, index, parameters, factories, exact, isNegated,
                        indexMapping, queryMapping, take, buildSteps);
                case MethodType.StartsWith:
                    return HandleStartsWith(indexSearcher, query, me, metadata, index, parameters, exact, isNegated, scoreFunction, indexMapping, queryMapping, take);
                case MethodType.EndsWith:
                    return HandleEndsWith(indexSearcher, query, me, metadata, index, parameters, exact, isNegated, scoreFunction, indexMapping, queryMapping, take);
                case MethodType.Exists:
                    return HandleExists(indexSearcher, query, parameters, me, metadata, scoreFunction);

                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", query.QueryText, parameters);
    }

    private static IQueryMatch TranslateBetweenQuery<TScoreFunction>(IndexSearcher indexSearcher, Query query, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters,
        bool exact,
        BetweenExpression be, bool secondary, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, be.Source, metadata);
        var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Min);
        var (valueSecond, valueSecondType) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Max);
        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

        var match = (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.Long, ValueTokenType.Long) => indexSearcher.Between(indexSearcher.AllEntries(), fieldId, (long)valueFirst,
                (long)valueSecond, isNegated, take),
            (ValueTokenType.Double, ValueTokenType.Double) => indexSearcher.Between(indexSearcher.AllEntries(), fieldId, (double)valueFirst,
                (double)valueSecond, isNegated, take),
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(),
            _ => throw new ArgumentOutOfRangeException()
        };

        return scoreFunction is NullScoreFunction
            ? match
            : indexSearcher.Boost(match, scoreFunction);

        IQueryMatch HandleStringBetween()
        {
            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            var valueFirstAsString = QueryBuilderHelper.CoraxGetValueAsString(valueFirst);
            var valueSecondAsString = QueryBuilderHelper.CoraxGetValueAsString(valueSecond);
            return indexSearcher.Between(indexSearcher.AllEntries(), fieldId, valueFirstAsString, valueSecondAsString, isNegated, take);
        }
    }

    private static IQueryMatch HandleExists<TScoreFunction>(IndexSearcher indexSearcher, Query query, BlittableJsonReaderObject parameters, MethodExpression expression,
        QueryMetadata metadata, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);

        return indexSearcher.ExistsQuery(fieldName, scoreFunction);
    }

    private static IQueryMatch HandleStartsWith<TScoreFunction>(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata,
        Index index,
        BlittableJsonReaderObject parameters, bool exact, bool isNegated, TScoreFunction scoreFunction, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        exact = QueryBuilderHelper.IsExact(index, exact, fieldName);

        if (exact && metadata.IsDynamic)
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

        return indexSearcher.StartWithQuery(fieldName, valueAsString, scoreFunction, isNegated, fieldId);
    }

    private static IQueryMatch HandleEndsWith<TScoreFunction>(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, bool exact, bool isNegated, TScoreFunction scoreFunction, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        exact = QueryBuilderHelper.IsExact(index, exact, fieldName);

        if (exact && metadata.IsDynamic)
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

        return indexSearcher.EndsWithQuery(fieldName, valueAsString, scoreFunction, isNegated, fieldId);
    }

    private static IQueryMatch HandleBoost(IndexSearcher indexSearcher, TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
        MethodExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, QueryBuilderFactories factories, bool exact, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        int take = TakeAll,
        List<string> buildSteps = null)
    {
        if (expression.Arguments.Count != 2)
        {
            throw new InvalidQueryException($"Boost(expression, boostVal) requires two arguments, but was called with {expression.Arguments.Count}",
                metadata.QueryText, parameters);
        }


        float boost;
        var (val, type) = QueryBuilderHelper.GetValue(query, metadata, parameters, expression.Arguments[1]);
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

        var scoreFunction = new ConstantScoreFunction(boost);

        return ToCoraxQuery(indexSearcher, serverContext, context, query, expression.Arguments[0], metadata, index, parameters, factories, scoreFunction, isNegated,
            indexMapping, queryMapping, exact,
            buildSteps: buildSteps, take: take);
    }

    private static IQueryMatch HandleSearch<TScoreFunction>(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata,
        BlittableJsonReaderObject parameters, int? proximity, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        Index index, int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        QueryFieldName fieldName;
        var isDocumentId = false;
        switch (expression.Arguments[0])
        {
            case FieldExpression ft:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, ft, metadata);
                break;
            case ValueExpression vt:
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(vt, metadata, parameters);
                break;
            case MethodExpression me when QueryMethod.GetMethodType(me.Name.Value) == MethodType.Id:
                fieldName = QueryFieldName.DocumentId;
                isDocumentId = true;
                break;
            default:
                throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);
        }

        var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

        if (metadata.IsDynamic && isDocumentId == false)
            fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
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

        return indexSearcher.SearchQuery(fieldName, valueAsString, scoreFunction, @operator, fieldId, isNegated);
    }

    private static IQueryMatch OrderBy(IndexSearcher indexSearcher, IQueryMatch match,
        List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orders, Query query, BlittableJsonReaderObject parameters, Index index,
        QueryMetadata metadata, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping, int take)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (orders.Count)
        {
            //Note: we want to use generics up to 3 comparers. This way we gonna avoid virtual calls in most cases.
            case 1:
            {
                var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[0].Expression, metadata);
                var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, false);
                var orderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[0].FieldType);
                var sortingType = orders[0].FieldType == OrderByFieldType.AlphaNumeric ? SortingType.Alphanumerical : SortingType.Normal;
                match = (fieldId) switch
                {
                    (QueryBuilderHelper.ScoreId) => indexSearcher.OrderByScore(match, take),
                    (>= 0) => orders[0].Ascending
                        ? indexSearcher.OrderByAscending(match, fieldId, sortingType, orderTypeField, take)
                        : indexSearcher.OrderByDescending(match, fieldId, sortingType, orderTypeField, take),

                    _ => throw new InvalidQueryException("Unknown field in ORDER BY clause.")
                };

                return match;
            }

            case 2:
            {
                var firstFieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[0].Expression, metadata);
                var firstFieldId = QueryBuilderHelper.GetFieldId(firstFieldName, index, indexMapping, queryMapping, false);
                var firstOrderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[0].FieldType);

                var secondFieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[1].Expression, metadata);
                var secondFieldId = QueryBuilderHelper.GetFieldId(secondFieldName, index, indexMapping, queryMapping, false);
                var secondOrderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[1].FieldType);


                return (QueryBuilderHelper.GetComparerType(orders[0].Ascending, orders[0].FieldType, firstFieldId),
                        QueryBuilderHelper.GetComparerType(orders[1].Ascending, orders[1].FieldType, secondFieldId)) switch
                    {
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField)),
                        var (type1, type2) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2})")
                    };
            }
            case 3:
            {
                var firstFieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[0].Expression, metadata);
                var firstFieldId = QueryBuilderHelper.GetFieldId(firstFieldName, index, indexMapping, queryMapping, false);
                var firstOrderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[0].FieldType);

                var secondFieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[1].Expression, metadata);
                var secondFieldId = QueryBuilderHelper.GetFieldId(secondFieldName, index, indexMapping, queryMapping, false);
                var secondOrderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[1].FieldType);

                var thirdFieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[2].Expression, metadata);
                var thirdFieldId = QueryBuilderHelper.GetFieldId(secondFieldName, index, indexMapping, queryMapping, false);
                var thirdOrderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[2].FieldType);


                return (QueryBuilderHelper.GetComparerType(orders[0].Ascending, orders[0].FieldType, firstFieldId),
                        QueryBuilderHelper.GetComparerType(orders[1].Ascending, orders[1].FieldType, secondFieldId),
                        QueryBuilderHelper.GetComparerType(orders[2].Ascending, orders[2].FieldType, thirdFieldId)
                    ) switch
                    {
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.DescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                default(BoostingComparer),
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Ascending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Descending)
                            => SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Descending, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Ascending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Descending) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType.Boosting) =>
                            SortingMultiMatch.Create(indexSearcher, match,
                                new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                                default(BoostingComparer),
                                default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.Boosting, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            default(BoostingComparer),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.AscendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Ascending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Descending) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.DescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .Boosting) => SortingMultiMatch.Create(indexSearcher,
                            match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            default(BoostingComparer)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .AscendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),
                        (QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType.DescendingAlphanumeric, QueryBuilderHelper.ComparerType
                            .DescendingAlphanumeric) => SortingMultiMatch.Create(
                            indexSearcher, match,
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, firstFieldId, firstOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, secondFieldId, secondOrderTypeField),
                            new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, thirdFieldId, thirdOrderTypeField)),

                        var (type1, type2, type3) => throw new NotSupportedException($"Currently, we do not support sorting by tuple ({type1}, {type2}, {type3})")
                    };
            }
        }

        var comparers = new IMatchComparer[orders.Count];
        for (int i = 0; i < orders.Count; ++i)
        {
            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, orders[0].Expression, metadata);
            var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, false);
            var orderTypeField = QueryBuilderHelper.TranslateOrderByForCorax(orders[0].FieldType);
            var sortingType = orders[0].FieldType == OrderByFieldType.AlphaNumeric ? SortingType.Alphanumerical : SortingType.Normal;


            comparers[i] = (orders[i].Ascending, sortingType) switch
            {
                (true, SortingType.Normal) => new SortingMatch.AscendingMatchComparer(indexSearcher, fieldId, orderTypeField),
                (false, SortingType.Normal) => new SortingMatch.DescendingMatchComparer(indexSearcher, fieldId, orderTypeField),
                (true, SortingType.Alphanumerical) => new SortingMatch.AlphanumericAscendingMatchComparer(indexSearcher, fieldId, orderTypeField),
                (false, SortingType.Alphanumerical) => new SortingMatch.AlphanumericDescendingMatchComparer(indexSearcher, fieldId, orderTypeField),
                _ => throw new InvalidDataException($"Unknown {typeof(SortingMatch)}: {sortingType} at {nameof(CoraxQueryBuilder)}")
            };
        }

        return orders.Count switch
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
