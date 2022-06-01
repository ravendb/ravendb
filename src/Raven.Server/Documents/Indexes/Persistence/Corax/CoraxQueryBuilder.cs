using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Nest;
using Lucene.Net.Spatial.Queries;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Spatial4n.Shapes;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.IndexSearcher;
using Query = Raven.Server.Documents.Queries.AST.Query;
using CoraxConstants = Corax.Constants;
using SpatialRelation = Spatial4n.Shapes.SpatialRelation;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;


namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static class CoraxQueryBuilder
{
    private const int TakeAll = -1;

    public static IQueryMatch BuildQuery(IndexSearcher indexSearcher, TransactionOperationContext serverContext, DocumentsOperationContext context,
        IndexQueryServerSide query,
        Index index, BlittableJsonReaderObject parameters, QueryBuilderFactories factories, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
        List<string> buildSteps = null, int take = TakeAll)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;
            var metadata = query.Metadata;
            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery<NullScoreFunction>(indexSearcher, serverContext, context, metadata.Query, metadata.Query.Where, metadata, index, parameters,
                    factories, default, false,
                    queryMapping: queryMapping,
                    buildSteps: buildSteps,
                    indexMapping: indexMapping,
                    highlightingTerms: highlightingTerms);
            }
            else
            {
                coraxQuery = indexSearcher.AllEntries();
            }

            if (metadata.Query.OrderBy is not null)
            {
                var sortMetadata = GetSortMetadata(query, index, factories.GetSpatialFieldFactory, indexMapping, queryMapping);
                coraxQuery = OrderBy(indexSearcher, coraxQuery, sortMetadata, take);
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
        BlittableJsonReaderObject parameters, QueryBuilderFactories factories, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
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
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne2.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);

                            return indexSearcher.AndNot(indexSearcher.AllEntries(), indexSearcher.Or(left, right));

                        case (NegatedExpression ne1, _):
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                                factories, scoreFunction, !isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne1.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);

                            return indexSearcher.AndNot(right, left);

                        case (_, NegatedExpression ne1):
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne1.Expression, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);

                            return indexSearcher.AndNot(left, right);

                        default:
                            left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                            right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                                factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);

                            return isNegated == false
                                ? indexSearcher.And(left, right)
                                : indexSearcher.Or(left, right);
                    }
                }
                case OperatorType.Or:
                {
                    var left = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Left, metadata, index, parameters,
                        factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: secondary, buildSteps: buildSteps, take: take);
                    var right = ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, @where.Right, metadata, index, parameters,
                        factories, scoreFunction, isNegated, indexMapping, highlightingTerms, queryMapping, exact, secondary: true, buildSteps: buildSteps, take: take);

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

                    var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);

                    CoraxHighlightingTermIndex highlightingTerm = null;
                    bool? isHighlighting = highlightingTerms?.TryGetValue(fieldName, out highlightingTerm);
                    if (isHighlighting.HasValue && isHighlighting.Value == false)
                    {
                        highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
                        highlightingTerms.TryAdd(fieldName, highlightingTerm);
                    }

                    if (operation is UnaryMatchOperation.Equals)
                    {
                        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                        var rawMatch = indexSearcher.TermQuery(fieldName, valueAsString, fieldId);
                        if (highlightingTerm is not null)
                            highlightingTerm.Values = valueAsString;

                        // This is TermMatch case
                        if (scoreFunction is not NullScoreFunction)
                            return indexSearcher.Boost(rawMatch, scoreFunction);


                        return rawMatch;
                    }


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

                    if (highlightingTerm != null && valueType is ValueTokenType.Double or ValueTokenType.Long)
                    {
                        highlightingTerm.Values = value.ToString();
                    }

                    if (match is null)
                        QueryBuilderHelper.ThrowUnhandledValueTokenType(valueType);

                    return match;

                    IQueryMatch HandleStringUnaryMatch()
                    {
                        if (exact && metadata.IsDynamic)
                        {
                            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                            fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);
                        }

                        if (value == null)
                        {
                            return indexSearcher.EqualsNull(indexSearcher.AllEntries(), fieldId, operation, take);
                        }
                        else
                        {
                            var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                            if (highlightingTerm != null)
                                highlightingTerm.Values = valueAsString;
                            
                            return indexSearcher.UnaryQuery(indexSearcher.AllEntries(), fieldId, valueAsString, operation, take);
                        }
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
                    indexMapping, highlightingTerms, queryMapping, exact,
                    buildSteps: buildSteps, take: take);
            }

            return ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, ne.Expression, metadata, index, parameters, factories, scoreFunction, !isNegated,
                indexMapping, highlightingTerms, queryMapping, exact,
                buildSteps: buildSteps, take: take);
        }

        if (expression is BetweenExpression be)
        {
            buildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(indexSearcher, query, metadata, index, parameters, exact, be, secondary, scoreFunction, isNegated, indexMapping, queryMapping, highlightingTerms,
                take);
        }

        if (expression is InExpression ie)
        {
            buildSteps?.Add($"In: {expression.Type} - {ie}");

            var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, ie.Source, metadata);
            
            CoraxHighlightingTermIndex highlightingTerm = null;
            if (highlightingTerms != null)
            {
                highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName };
                highlightingTerms[fieldName] = highlightingTerm;
            }

            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            if (exact && metadata.IsDynamic)
            {
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName), fieldName.IsQuoted);
                if (highlightingTerms != null)
                {
                    highlightingTerm.DynamicFieldName = fieldName;
                    highlightingTerms[fieldName] = highlightingTerm;
                }
            }

            var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);

            var hasGotTheRealType = false;

            if (ie.All)
            {
                var uniqueMatches = new HashSet<string>();
                foreach (var tuple in QueryBuilderHelper.GetValuesForIn(query, ie, metadata, parameters))
                {
                    if (exact && metadata.IsDynamic)
                        fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                    uniqueMatches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
                }

                return indexSearcher.AllInQuery(fieldName, uniqueMatches, fieldId);

            }
            
            var matches = new List<string>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(query, ie, metadata, parameters))
            {
                matches.Add(QueryBuilderHelper.CoraxGetValueAsString(tuple.Value));
            }

            if (highlightingTerm != null )
                highlightingTerm.Values = matches;

            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => indexSearcher.InQuery(fieldName, matches, fieldId),
                (true, NullScoreFunction) => indexSearcher.InQuery(fieldName, matches, fieldId),
                (false, _) => indexSearcher.InQuery(fieldName, matches, scoreFunction, fieldId),
                (true, _) => indexSearcher.InQuery(fieldName, matches, fieldId)
            };
        }

        if (expression is TrueExpression)
        {
            buildSteps?.Add($"True: {expression.Type} - {expression}");

            return indexSearcher.AllEntries();
        }

        if (expression is MethodExpression me)
        {
            var methodName = me.Name.Value;
            var methodType = QueryMethod.GetMethodType(methodName);

            buildSteps?.Add($"Method: {expression.Type} - {me} - method: {methodType}, {methodName}");

            switch (methodType)
            {
                case MethodType.Search:
                    return HandleSearch(indexSearcher, query, me, metadata, parameters, proximity, scoreFunction, isNegated, indexMapping, queryMapping, index, highlightingTerms, take);
                case MethodType.Boost:
                    return HandleBoost(indexSearcher, serverContext, documentsContext, query, me, metadata, index, parameters, factories, exact, isNegated,
                        indexMapping, queryMapping, highlightingTerms, take, buildSteps);
                case MethodType.StartsWith:
                    return HandleStartsWith(indexSearcher, query, me, metadata, index, parameters, exact, isNegated, scoreFunction, indexMapping, queryMapping, highlightingTerms, take);
                case MethodType.EndsWith:
                    return HandleEndsWith(indexSearcher, query, me, metadata, index, parameters, exact, isNegated, scoreFunction, indexMapping, queryMapping, highlightingTerms, take);
                case MethodType.Exists:
                    return HandleExists(indexSearcher, query, parameters, me, metadata, scoreFunction);
                case MethodType.Exact:
                    return HandleExact(indexSearcher, serverContext, documentsContext, query, me, metadata, index, parameters, factories, scoreFunction, isNegated, indexMapping, queryMapping, proximity, secondary, buildSteps, highlightingTerms, take);
                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    return HandleSpatial(indexSearcher, query, me, metadata, parameters, methodType, factories.GetSpatialFieldFactory, index, indexMapping, queryMapping);
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", query.QueryText, parameters);
    }

    private static IQueryMatch HandleExact<TScoreFunction>(IndexSearcher indexSearcher, TransactionOperationContext serverContext,
        DocumentsOperationContext documentsContext,
        Query query,
        MethodExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, QueryBuilderFactories factories, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping,
        FieldsToFetch queryMapping, int? proximity = null, bool secondary = false,
        List<string> buildSteps = null, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null, int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        return ToCoraxQuery(indexSearcher, serverContext, documentsContext, query, expression.Arguments[0], metadata, index, parameters, factories, scoreFunction,
            isNegated, indexMapping, highlightingTerms, queryMapping, true, proximity, secondary, buildSteps);
    }

    private static IQueryMatch TranslateBetweenQuery<TScoreFunction>(IndexSearcher indexSearcher, Query query, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters,
        bool exact,
        BetweenExpression be, bool secondary, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, be.Source, metadata);
        var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Min);
        var (valueSecond, valueSecondType) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Max);
        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);

        var match = (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.Long, ValueTokenType.Long) => indexSearcher.Between(indexSearcher.AllEntries(), fieldId, (long)valueFirst,
                (long)valueSecond, isNegated, take),
            (ValueTokenType.Double, ValueTokenType.Double) => indexSearcher.Between(indexSearcher.AllEntries(), fieldId, (double)valueFirst,
                (double)valueSecond, isNegated, take),
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(),
            _ => throw new ArgumentOutOfRangeException()
        };

        if ((valueFirstType, valueSecondType) is (ValueTokenType.Double, ValueTokenType.Double) or (ValueTokenType.Long, ValueTokenType.Long))
        {
            if (highlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                highlightingTerms[fieldName] = highlightingTerm;
            }
        }

        return scoreFunction is NullScoreFunction
            ? match
            : indexSearcher.Boost(match, scoreFunction);

        IQueryMatch HandleStringBetween()
        {
            exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
            var valueFirstAsString = QueryBuilderHelper.CoraxGetValueAsString(valueFirst);
            var valueSecondAsString = QueryBuilderHelper.CoraxGetValueAsString(valueSecond);

            if (highlightingTerms != null)
            {
                var highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = (valueFirst, valueSecond) };
                highlightingTerms[fieldName] = highlightingTerm;
            }

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
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        CoraxHighlightingTermIndex highlightingTerm = null;
        if (highlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
            highlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
        if (exact && metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (highlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                highlightingTerms[fieldName] = highlightingTerm;
            }
        }            

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);        
        return indexSearcher.StartWithQuery(fieldName, valueAsString, scoreFunction, isNegated, fieldId);
    }

    private static IQueryMatch HandleEndsWith<TScoreFunction>(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, bool exact, bool isNegated, TScoreFunction scoreFunction, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
        int take = TakeAll)
        where TScoreFunction : IQueryScoreFunction
    {
        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
        var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
        if (valueType != ValueTokenType.String)
            QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);

        CoraxHighlightingTermIndex highlightingTerm = null;
        if (highlightingTerms != null)
        {
            highlightingTerm = new CoraxHighlightingTermIndex { FieldName = fieldName, Values = valueAsString };
            highlightingTerms[fieldName] = highlightingTerm;
        }

        exact = QueryBuilderHelper.IsExact(index, exact, fieldName);
        if (exact && metadata.IsDynamic)
        {
            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
            if (highlightingTerms != null)
            {
                highlightingTerm.DynamicFieldName = fieldName;
                highlightingTerms[fieldName] = highlightingTerm;
            }
        }
           
        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping, exact);
        return indexSearcher.EndsWithQuery(fieldName, valueAsString, scoreFunction, isNegated, fieldId);
    }

    private static IQueryMatch HandleBoost(IndexSearcher indexSearcher, TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
        MethodExpression expression, QueryMetadata metadata, Index index,
        BlittableJsonReaderObject parameters, QueryBuilderFactories factories, bool exact, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
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
            indexMapping, highlightingTerms, queryMapping, exact,
            buildSteps: buildSteps,
            take: take);
    }

    private static IQueryMatch HandleSearch<TScoreFunction>(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata,
        BlittableJsonReaderObject parameters, int? proximity, TScoreFunction scoreFunction, bool isNegated, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping,
        Index index,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms = null,
        int take = TakeAll)
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

        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
        if (highlightingTerms != null && highlightingTerms.TryGetValue(fieldName, out var highlightingTerm) == false)
        {
            highlightingTerm = new CoraxHighlightingTermIndex
            {
                Values = valueAsString,
            };
            
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

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

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

    private static IQueryMatch HandleSpatial(IndexSearcher indexSearcher, Query query, MethodExpression expression, QueryMetadata metadata,
        BlittableJsonReaderObject parameters,
        MethodType spatialMethod, Func<string, SpatialField> getSpatialField, Index index, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null)
    {
        string fieldName;
        if (metadata.IsDynamic == false)
            fieldName = QueryBuilderHelper.ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
        else
        {
            var spatialExpression = (MethodExpression)expression.Arguments[0];
            fieldName = metadata.GetSpatialFieldName(spatialExpression, parameters);
        }

        var fieldId = QueryBuilderHelper.GetFieldId(fieldName, index, indexMapping, queryMapping);

        var shapeExpression = (MethodExpression)expression.Arguments[1];

        var distanceErrorPct = RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
        if (expression.Arguments.Count == 3)
        {
            var distanceErrorPctValue = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

            distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
        }

        var spatialField = getSpatialField(fieldName);

        var methodName = shapeExpression.Name;
        var methodType = QueryMethod.GetMethodType(methodName.Value);

        IShape shape = null;
        switch (methodType)
        {
            case MethodType.Spatial_Circle:
                shape = QueryBuilderHelper.HandleCircle(query, shapeExpression, metadata, parameters, fieldName, spatialField, out _);
                break;
            case MethodType.Spatial_Wkt:
                shape = QueryBuilderHelper.HandleWkt(query, shapeExpression, metadata, parameters, fieldName, spatialField, out _);
                break;
            default:
                QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                break;
        }

        Debug.Assert(shape != null);

        var operation = spatialMethod switch
        {
            MethodType.Spatial_Within => global::Corax.Utils.Spatial.SpatialRelation.Within,
            MethodType.Spatial_Disjoint => global::Corax.Utils.Spatial.SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => global::Corax.Utils.Spatial.SpatialRelation.Intersects,
            MethodType.Spatial_Contains => global::Corax.Utils.Spatial.SpatialRelation.Contains,
            _ => (global::Corax.Utils.Spatial.SpatialRelation)QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, parameters)
        };


        //var args = new SpatialArgs(operation, shape) {DistErrPct = distanceErrorPct};

        return indexSearcher.SpatialQuery(fieldName, fieldId, Double.Epsilon, shape, spatialField.GetContext(), operation);
    }

    public static ReadOnlySpan<OrderMetadata> GetSortMetadata(IndexQueryServerSide query, Index index, Func<string, SpatialField> getSpatialField,
        IndexFieldsMapping indexMapping, FieldsToFetch queryMapping)
    {
        var sort = ReadOnlySpan<OrderMetadata>.Empty;
        if (query.PageSize == 0) // no need to sort when counting only
            return null;

        var orderByFields = query.Metadata.OrderBy;

        if (orderByFields == null)
        {
            if (query.Metadata.HasBoost == false && index.HasBoostedFields == false)
                return null;
            return new[] {new OrderMetadata(true, MatchCompareFieldType.Score)};
        }

        int sortIndex = 0;
        Span<OrderMetadata> sortArray = new OrderMetadata[8];

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

        return sortArray.Slice(0, sortIndex);
    }

    private static IQueryMatch OrderBy(IndexSearcher indexSearcher, IQueryMatch match, ReadOnlySpan<OrderMetadata> orderMetadata, int take)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (orderMetadata.Length)
        {
            //Note: we want to use generics up to 3 comparers. This way we gonna avoid virtual calls in most cases.
            case 0:
                return match;
            case 1:
            {
                var order = orderMetadata[0];
                if (order.HasBoost)
                    return indexSearcher.OrderByScore(match, take);

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
