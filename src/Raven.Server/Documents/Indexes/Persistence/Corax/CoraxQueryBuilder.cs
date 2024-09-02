using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
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
using IndexSearcher = Corax.Querying.IndexSearcher;
using CoraxConstants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static class CoraxQueryBuilder
{
    internal const int TakeAll = -1;

    public sealed class Parameters
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
        public readonly CancellationToken Token;
        public readonly List<string> BuildSteps;
        public readonly MemoizationMatchProvider<AllEntriesMatch> AllEntries;
        public readonly QueryMetadata Metadata;
        public readonly bool HasDynamics;
        public readonly Lazy<List<string>> DynamicFields;
        public readonly ByteStringContext Allocator;
        public readonly bool HasBoost;
        public readonly IndexReadOperationBase IndexReadOperation;
        public StreamingOptimization StreamingDisabled;

        internal Parameters(IndexSearcher searcher, ByteStringContext allocator, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext,
            IndexQueryServerSide query, Index index, BlittableJsonReaderObject queryParameters, QueryBuilderFactories factories, IndexFieldsMapping indexFieldsMapping,
            FieldsToFetch fieldsToFetch, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, int take, IndexReadOperationBase indexReadOperation = null, List<string> buildSteps = null, CancellationToken token = default)
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
            Token = token;
            BuildSteps = buildSteps;
            AllEntries = IndexSearcher.Memoize(IndexSearcher.AllEntries());
            Metadata = query.Metadata;
            HasDynamics = index.Definition.HasDynamicFields;
            DynamicFields = HasDynamics
                ? new Lazy<List<string>>(() => IndexSearcher.GetFields())
                : null;
            
            // in case when we've implicit boosting we've build primitives with scoring enabled
            HasBoost = index.HasBoostedFields || query.Metadata.HasBoost ||
                       (index.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved && 
                        HasBoostingAsOrderingType(query.Metadata.OrderBy)); 
            Allocator = allocator;
            IndexReadOperation = indexReadOperation;
        }

        private static bool HasBoostingAsOrderingType(OrderByField[] orderBy)
        {
            if (orderBy is null)
                return false;
            
            foreach (var field in orderBy)
                if (field.OrderingType == OrderByFieldType.Score)
                    return true;

            return false;
        }
    }

    public struct StreamingOptimization
    {
        public readonly OrderMetadata SortField;
        public readonly bool OptimizationIsPossible;
        public bool SkipOrderByClause;
        public bool MatchedByCompoundField;
        public bool BinaryMatchInQuery = false;
        public FieldMetadata CompoundField;
        public bool Forward => SortField.Ascending;

        public StreamingOptimization(IndexSearcher searcher, OrderMetadata[] orderMetadata, bool hasBoosting)
        {
            bool hasSpecialSorter = false;
            foreach (var order in orderMetadata ?? Array.Empty<OrderMetadata>())
            {
                hasSpecialSorter |= order.FieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating);
                if (hasSpecialSorter)
                    break;
            }
            
            if (orderMetadata is null or {Length: 0}
                || hasSpecialSorter
                || searcher.HasMultipleTermsInField(orderMetadata[0].Field) 
                || hasBoosting)
            {
                SortField = default;
                OptimizationIsPossible = false;
                return;
            }

            SortField = orderMetadata[0];
            OptimizationIsPossible = true;
        }

        public void BinaryMatchTraversed() => BinaryMatchInQuery = true;

        public bool TrySetMultiTermMatchAsStreamingField(IndexSearcher indexSearcher, in FieldMetadata queryField, in MethodType methodType)
        {
            if (OptimizationIsPossible == false)
                return false;

            if (FieldMetadataFieldEquals(queryField) == false)
                return false;

            if (BinaryMatchInQuery)
                return false;
            
            SkipOrderByClause = methodType switch
            {
                MethodType.StartsWith => SortField.FieldType is MatchCompareFieldType.Sequence,
                MethodType.EndsWith => SortField.FieldType is MatchCompareFieldType.Sequence,
                MethodType.Exists => SortField.FieldType is MatchCompareFieldType.Sequence,
                MethodType.Regex => SortField.FieldType is MatchCompareFieldType.Sequence,
                _ => false
            };

            return SkipOrderByClause;
        }
        
        public bool TrySetAsStreamingField(Parameters builderParameters, in CoraxBooleanItem cbi)
        {
            if (OptimizationIsPossible == false)
                return false;

            if (cbi.Operation is UnaryMatchOperation.NotEquals)
                return false;
            
            if (SkipOrderByClause && OptimizationIsPossible)
                ThrowFieldIsAlreadyChosen();

            if (cbi.Field.Equals(SortField.Field) == false)
            {
                if ( builderParameters.Index.HasCompoundField(cbi.Field.FieldName, SortField.Field.FieldName, out var bindingId))
                {
                    var indexFieldBinding = builderParameters.IndexFieldsMapping.GetByFieldId(bindingId);
                    CompoundField = FieldMetadata.Build(indexFieldBinding.FieldName, indexFieldBinding.FieldTermTotalSumField,
                        indexFieldBinding.FieldId, indexFieldBinding.FieldIndexingMode, indexFieldBinding.Analyzer);
                    MatchedByCompoundField = true;
                    return true;
                }
                return false;
            }

            //In case of MultiTermMatch we cannot apply optimization when:
            //- MTM is inside BinaryMatch
            if (cbi.Operation is not UnaryMatchOperation.Equals &&  BinaryMatchInQuery)
                return false;
            
            // In case of TermMatch `order by` has to have exactly the same type as queried field.
            // Since we store number as tuple of (String, Double, Long) we could write query like this:
            // where Numeric = 1 order by Numeric as double. Where clause will match all documents where Numeric casted to long is equal 1 (e.g. 1.2, 1.3, 1.9) but order by will sort with precision.
            // This also valid for MultiTermMatch.
            bool matchOnType = cbi.Term switch
            {
                long => SortField.FieldType is MatchCompareFieldType.Integer,
                double or float => SortField.FieldType is MatchCompareFieldType.Floating,
                _ => SortField.FieldType is MatchCompareFieldType.Sequence
            };
            
            //Todo: when any document contains list in sorted field we may consider disabling streaming optimization.  
            
            if (matchOnType == false)
                return false;

            SkipOrderByClause = true;
            return true;
        }

        public bool FieldMetadataFieldEquals(in FieldMetadata otherMetadata) => SortField.Field.Equals(otherMetadata);

        private static void ThrowFieldIsAlreadyChosen()
        {
            throw new InvalidOperationException($"We match TermMatch with the OrderByField but somehow we still wanted to check that. This is bug.");
        }
    }

    private static IQueryMatch MaterializeWhenNeeded(Parameters builderParameters, IQueryMatch source, ref StreamingOptimization streamingConfiguration)
    {
        switch (source)
        {
            case CoraxBooleanQueryBase cbq:
                source = cbq.Materialize();
                break;
            case CoraxBooleanItem cbi:
                streamingConfiguration.TrySetAsStreamingField(builderParameters,cbi);
                if (streamingConfiguration.MatchedByCompoundField)
                    return cbi.OptimizeCompoundField(ref streamingConfiguration);

                source = cbi.Materialize(ref streamingConfiguration);
                break;
        }

        return source;
    }

    internal static IQueryMatch BuildQuery(Parameters builderParameters, out OrderMetadata[] sortMetadata)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            IQueryMatch coraxQuery;
            var metadata = builderParameters.Query.Metadata;
            var indexSearcher = builderParameters.IndexSearcher;
            sortMetadata = GetSortMetadata(builderParameters);
            var streamingOptimization = new StreamingOptimization(indexSearcher, sortMetadata, builderParameters.HasBoost);
            
            if (metadata.Query.Where is not null)
            {
                coraxQuery = ToCoraxQuery(builderParameters, metadata.Query.Where, ref streamingOptimization);
                coraxQuery = MaterializeWhenNeeded(builderParameters, coraxQuery, ref streamingOptimization);
            }
            // We sort on known field types, we'll optimize based on the first one to get the rest
            // Non-existing posting list isn't aware of dynamic fields, so we can't use this optimization for them
            else if (sortMetadata is [{ FieldType: MatchCompareFieldType.Floating or MatchCompareFieldType.Integer or MatchCompareFieldType.Sequence, Field.FieldId: not CoraxConstants.IndexWriter.DynamicField } sortBy, ..])
            {
                var maxTermToScan = builderParameters.Take switch
                {
                    < 0 => int.MaxValue, // meaning, take all
                    // We cannot apply this optimization when we are returning statistics (RavenDB-21525).
                    var take when builderParameters.Query.SkipStatistics => (long)take + 1, 
                    int.MaxValue => (long)int.MaxValue + 1, // avoid overflow
                    _ => int.MaxValue
                };

                // We have no where clause and a sort by index, can just scan over the relevant index if there is a single order by clause
                // if we have multiple clauses, we'll get the first $TAKE+1 terms from the index, then sort just those, leading to the same
                // behavior, but far faster
                var betweenQuery = sortBy.FieldType switch
                {
                     MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(sortBy.Field, long.MinValue, long.MaxValue, forward: sortBy.Ascending, streamingEnabled: true, maxNumberOfTerms: maxTermToScan),
                     MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(sortBy.Field, double.MinValue, double.MaxValue, forward: sortBy.Ascending, streamingEnabled: true, maxNumberOfTerms: maxTermToScan),
                     MatchCompareFieldType.Sequence => indexSearcher.BetweenQuery(sortBy.Field, Constants.BeforeAllKeys, Constants.AfterAllKeys, forward: sortBy.Ascending, streamingEnabled: true, maxNumberOfTerms: maxTermToScan),
                     _ => throw new ArgumentOutOfRangeException("Already checked the FieldType, but was: " + sortBy.FieldType)
                };

                var queryWithNullMatches = indexSearcher.IncludeNullMatch(in sortBy.Field, betweenQuery, sortBy.Ascending);

                var indexVersion = builderParameters.Index.Definition.Version;

                if (IndexDefinitionBaseServerSide.IndexVersion.IsNonExistingPostingListSupported(indexVersion))
                {
                    var queryWithNullAndNonExistingMatches = indexSearcher.IncludeNonExistingMatch(in sortBy.Field, queryWithNullMatches, sortBy.Ascending);
                    coraxQuery = queryWithNullAndNonExistingMatches;
                }

                else
                    coraxQuery = queryWithNullMatches;
                
                streamingOptimization.SkipOrderByClause = true; // manually turn off the order by
            }
            else 
            {
                coraxQuery = indexSearcher.Memoize(indexSearcher.AllEntries()).Replay();
            }

            if (sortMetadata is not null)
            {
                // In the case of ordering by multiple fields, we still want to benefit from the streaming
                // optimization, but we have to sort again anyway, this ensure that this happens
                if (streamingOptimization.SkipOrderByClause == false || sortMetadata.Length > 1)
                    coraxQuery = OrderBy(builderParameters, coraxQuery, sortMetadata);
            } 

            // The parser already throws parse exception if there is a syntax error.
            // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
            return coraxQuery;
        }
    }

    private static IQueryMatch ToCoraxQuery(Parameters builderParameters, QueryExpression expression, ref StreamingOptimization leftOnlyOptimization, bool exact = false, int? proximity = null)
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
                    
                    leftOnlyOptimization.BinaryMatchTraversed();
                    switch (@where.Left, @where.Right)
                    {
                        case (NegatedExpression ne1, NegatedExpression ne2):
                            left = ToCoraxQuery(builderParameters, ne1.Expression, ref builderParameters.StreamingDisabled, exact);
                            right = ToCoraxQuery(builderParameters, ne2.Expression, ref builderParameters.StreamingDisabled, exact);

                            TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out var merged, ref builderParameters.StreamingDisabled, true);

                            return indexSearcher.AndNot(builderParameters.AllEntries.Replay(), indexSearcher.Or(left, right), token: builderParameters.Token);

                        case (NegatedExpression ne1, _):
                            left = ToCoraxQuery(builderParameters, @where.Right, ref builderParameters.StreamingDisabled, exact);
                            if (TryUseNegatedQuery(builderParameters, ne1, out right, exact) == false)
                            {
                                right = ToCoraxQuery(builderParameters, ne1.Expression, ref builderParameters.StreamingDisabled, exact);
                                TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out merged,
                                    ref builderParameters.StreamingDisabled,  requiredMaterialization: true);
                                return indexSearcher.AndNot(left, right, token: builderParameters.Token);
                            }

                            if (@where.Right is TrueExpression)
                                return right; // true and not... optimization
                            
                            if (TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out merged, ref builderParameters.StreamingDisabled))
                                return merged;

                            return indexSearcher.And(left, right);

                        case (_, NegatedExpression ne1):
                            left = ToCoraxQuery(builderParameters, @where.Left, ref builderParameters.StreamingDisabled, exact);
                            if (TryUseNegatedQuery(builderParameters, ne1, out right, exact) == false)
                            {
                                right = ToCoraxQuery(builderParameters, ne1.Expression, ref builderParameters.StreamingDisabled, exact);
                                TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out merged, ref builderParameters.StreamingDisabled,  requiredMaterialization: true);
                                return indexSearcher.AndNot(left, right, token: builderParameters.Token);
                            }

                            if (@where.Left is TrueExpression)
                                return right; // true and not... optimization

                            if (TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out merged, ref builderParameters.StreamingDisabled))
                                return merged;

                            return indexSearcher.And(left, right);


                        default:
                            left = ToCoraxQuery(builderParameters, @where.Left, ref leftOnlyOptimization, exact);
                            right = ToCoraxQuery(builderParameters, @where.Right, ref builderParameters.StreamingDisabled, exact);
                            // in case of AND we can materialize only TermMatches, we push streamingOptimization there only for changing order for MultiTermMatch;
                            if (left is CoraxBooleanItem cbi && leftOnlyOptimization.TrySetAsStreamingField(builderParameters, cbi))
                                left = cbi.Materialize(ref leftOnlyOptimization); 
                            
                            if (TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out merged, ref builderParameters.StreamingDisabled))
                                return merged;

                            // When we do not optimize this node for streaming operation:.
                            // We don't want an unknown size multiterm match to be subject to this optimization. When faced with one that is unknown just execute as
                            // it was written in the query. If we don't have statistics the confidence will be Low, so the optimization wont happen.
                            //This was part of Corax's IndexSearcher optimization (https://github.com/ravendb/ravendb/blob/12b874c06a0003520cd8f188467488cdc526f96b/src/Corax/IndexSearcher/IndexSearcher.BinaryMatches.cs#L16C8-L19)
                            if (leftOnlyOptimization.OptimizationIsPossible == false && left.Count < right.Count && left.Confidence >= QueryCountConfidence.Normal)
                                return indexSearcher.And(right, left, token: builderParameters.Token);
                            
                            return indexSearcher.And(left, right, token: builderParameters.Token);
                    }
                }
                case OperatorType.Or:
                {
                    leftOnlyOptimization.BinaryMatchTraversed();
                    var left = ToCoraxQuery(builderParameters, @where.Left, ref builderParameters.StreamingDisabled, exact);
                    var right = ToCoraxQuery(builderParameters, @where.Right, ref builderParameters.StreamingDisabled, exact);

                    builderParameters.BuildSteps?.Add(
                        $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assembly location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlbly: {right.GetType().Assembly.FullName} assembly location: {right.GetType().Assembly.Location}");

                    var match = new CoraxOrQueries(indexSearcher, builderParameters);
                    if (match.TryAddItem(left) && match.TryAddItem(right))
                        return match;

                    TryMergeTwoNodesForAnd(indexSearcher, builderParameters, ref left, ref right, out var _, ref builderParameters.StreamingDisabled, true);

                    return indexSearcher.Or(left, right, token: builderParameters.Token);
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
                        ValueTokenType.Double => CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, value, operation, ref builderParameters.StreamingDisabled),
                        ValueTokenType.Long => CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, value, operation, ref builderParameters.StreamingDisabled),
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
                                return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, null, UnaryMatchOperation.Equals, ref builderParameters.StreamingDisabled);
                            else if (operation is UnaryMatchOperation.NotEquals)
                                //Please consider if we ever need to support this.
                                return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, null, UnaryMatchOperation.NotEquals, ref builderParameters.StreamingDisabled);
                            else
                                throw new NotSupportedException($"Unhandled operation: {operation}");
                        }

                        var valueAsString = QueryBuilderHelper.CoraxGetValueAsString(value);
                        if (highlightingTerm != null)
                            highlightingTerm.Values = valueAsString;

                        return CoraxBooleanItem.Build(indexSearcher, index, fieldMetadata, valueAsString, operation, ref builderParameters.StreamingDisabled);
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
                return ToCoraxQuery(builderParameters, newExpr, ref builderParameters.StreamingDisabled, exact);
            }

            //e.g. or (not exists(Field))
            var inner = ToCoraxQuery(builderParameters, ne.Expression, ref builderParameters.StreamingDisabled, exact);
            inner = MaterializeWhenNeeded(builderParameters, inner, ref builderParameters.StreamingDisabled);
            return builderParameters.IndexSearcher.AndNot(builderParameters.IndexSearcher.AllEntries(), inner);
        }

        if (expression is BetweenExpression be)
        {
            builderParameters.BuildSteps?.Add($"Between: {expression.Type} - {be}");

            return TranslateBetweenQuery(builderParameters, be, exact);
        }

        if (expression is InExpression ie)
        {
            return HandleIn(builderParameters, ie, exact);
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
                    return HandleStartsWith(builderParameters, me, exact, ref leftOnlyOptimization);
                case MethodType.EndsWith:
                    return HandleEndsWith(builderParameters, me, exact, ref leftOnlyOptimization);
                case MethodType.Exists:
                    return HandleExists(builderParameters, me, ref leftOnlyOptimization);
                case MethodType.Exact:
                    return HandleExact(builderParameters, me, ref leftOnlyOptimization, proximity);
                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    return HandleSpatial(builderParameters, me, methodType);
                case MethodType.Regex:
                    return HandleRegex(builderParameters, me, ref leftOnlyOptimization);
                case MethodType.MoreLikeThis:
                    return builderParameters.AllEntries.Replay();
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, queryParameters);
                    return null; // never hit
            }
        }

        throw new InvalidQueryException("Unable to understand query", metadata.QueryText, queryParameters);
    }

    private static IQueryMatch HandleIn(Parameters builderParameters, InExpression ie, bool exact)
    {
        builderParameters.BuildSteps?.Add($"In: {ie.Type} - {ie}");

        QueryMetadata metadata = builderParameters.Metadata;
        BlittableJsonReaderObject queryParameters = builderParameters.QueryParameters;
        var allocator = builderParameters.Allocator;

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

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, builderParameters.Index, builderParameters.IndexFieldsMapping, builderParameters.FieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, exact: exact);
        
        var hasTime = builderParameters.Index.IndexFieldsPersistence.HasTimeValues(fieldName);
        
        if (ie.All)
        {
            var uniqueMatches = new HashSet<(string Term, bool Exact)>();
            foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
            {
                if (exact && builderParameters.Metadata.IsDynamic)
                    fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);
                
                bool isTime = hasTime && tuple.Value != null &&  QueryBuilderHelper.TryGetTime(builderParameters.Index, tuple.Value, out var _);
                uniqueMatches.Add((QueryBuilderHelper.CoraxGetValueAsString(tuple.Value), isTime));
            }

            return builderParameters.IndexSearcher.AllInQuery(fieldMetadata, uniqueMatches);
        }

        var matches = new List<(string Term, bool Exact)>();
        foreach (var tuple in QueryBuilderHelper.GetValuesForIn(metadata.Query, ie, metadata, queryParameters))
        {
            bool isTime = hasTime && tuple.Value != null && QueryBuilderHelper.TryGetTime(builderParameters.Index, tuple.Value, out var _);
            matches.Add((QueryBuilderHelper.CoraxGetValueAsString(tuple.Value), isTime));
        }

        if (highlightingTerm != null)
            highlightingTerm.Values = matches;

        return builderParameters.IndexSearcher.InQuery(fieldMetadata, matches);
    }

    private static bool TryUseNegatedQuery(Parameters builderParameters, NegatedExpression ne1, out IQueryMatch match, bool exact)
    {
        if (ne1.Expression is not MethodExpression inner)
            goto NoOpt;
        
        var methodName = inner.Name.Value;
        var methodType = QueryMethod.GetMethodType(methodName);
        
        switch (methodType)
        {
            case MethodType.StartsWith:
                match = HandleStartsWith(builderParameters, inner,  exact, ref builderParameters.StreamingDisabled, negated: true);
                return true;
            case MethodType.EndsWith:
                match = HandleEndsWith(builderParameters, inner,  exact, ref builderParameters.StreamingDisabled, negated: true);
                return true;
            default:
                goto NoOpt;
        }
        
        NoOpt:
        match = null;
        return false;
    }

    public static MoreLikeThisQuery.MoreLikeThisQuery BuildMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            var filterQuery = BuildQuery(builderParameters, out _);
            filterQuery = MaterializeWhenNeeded(builderParameters, filterQuery, ref builderParameters.StreamingDisabled);

            var moreLikeThisQuery = ToMoreLikeThisQuery(builderParameters, whereExpression, out var baseDocument, out var options);
            moreLikeThisQuery = MaterializeWhenNeeded(builderParameters,moreLikeThisQuery, ref builderParameters.StreamingDisabled);

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
            return ToCoraxQuery(builderParameters, binaryExpression, ref builderParameters.StreamingDisabled);

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

    private static bool TryMergeTwoNodesForAnd(IndexSearcher indexSearcher, Parameters parameters, ref IQueryMatch lhs, ref IQueryMatch rhs, out CoraxBooleanQueryBase merged, ref StreamingOptimization streamingOptimization, bool requiredMaterialization = false)
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
                    merged = new CoraxAndQueries(indexSearcher, parameters, lhsBq, rhsBq);
                    return true;
                }

                return false;
            default:
                if (lhs is CoraxBooleanItem cbi)
                    lhs = cbi.Materialize(ref streamingOptimization);
                else if (lhs is CoraxBooleanQueryBase cbq)
                    lhs = cbq.Materialize();
                if (rhs is CoraxBooleanItem cbi1)
                    rhs = cbi1.Materialize(ref streamingOptimization);
                else if (rhs is CoraxBooleanQueryBase cbq1)
                    rhs = cbq1.Materialize();
                return false;
        }
    }

    private static IQueryMatch HandleExact(Parameters builderParameters, MethodExpression expression, ref StreamingOptimization streamingConfiguration, int? proximity = null)
    {
        return ToCoraxQuery(builderParameters, expression.Arguments[0], ref streamingConfiguration, exact: true, proximity);
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
        

        return (valueFirstType, valueSecondType) switch
        {
            (ValueTokenType.String, ValueTokenType.String) => HandleStringBetween(),
            _ => CoraxBooleanItem.Build(builderParameters.IndexSearcher, index, fieldMetadata, valueFirst, valueSecond, UnaryMatchOperation.Between, leftSideOperation,
                rightSideOperation, ref builderParameters.StreamingDisabled)
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
                leftSideOperation, rightSideOperation, ref builderParameters.StreamingDisabled);
        }
    }

    private static IQueryMatch HandleExists(Parameters builderParameters, MethodExpression expression, ref StreamingOptimization streamingOptimization)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;

        var fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters.Allocator, fieldName, builderParameters.Index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);
        
        return streamingOptimization.TrySetMultiTermMatchAsStreamingField(builderParameters.IndexSearcher, fieldMetadata, MethodType.Exists) 
            ? builderParameters.IndexSearcher.ExistsQuery(fieldMetadata, forward: streamingOptimization.Forward, streamingEnabled: true) 
            : builderParameters.IndexSearcher.ExistsQuery(fieldMetadata);
        
    }

    private static IQueryMatch HandleStartsWith(Parameters builderParameters, MethodExpression expression, bool exact, ref StreamingOptimization streamingOptimization,
        bool negated = false)
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

        return streamingOptimization.TrySetMultiTermMatchAsStreamingField(builderParameters.IndexSearcher, fieldMetadata, MethodType.StartsWith) 
            ? builderParameters.IndexSearcher.StartWithQuery(fieldMetadata, valueAsString, forward: streamingOptimization.Forward, streamingEnabled: true, isNegated: negated) 
            : builderParameters.IndexSearcher.StartWithQuery(fieldMetadata, valueAsString, isNegated: negated);
    }

    private static MultiTermMatch HandleEndsWith(Parameters builderParameters, MethodExpression expression, bool exact, ref StreamingOptimization streamingOptimization,
        bool negated = false)
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
        
        return streamingOptimization.TrySetMultiTermMatchAsStreamingField(builderParameters.IndexSearcher, fieldMetadata, MethodType.EndsWith) 
            ? builderParameters.IndexSearcher.EndsWithQuery(fieldMetadata, valueAsString, forward: streamingOptimization.Forward, streamingEnabled: true, isNegated: negated) 
            : builderParameters.IndexSearcher.EndsWithQuery(fieldMetadata, valueAsString, isNegated: negated);
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


        var query = ToCoraxQuery(builderParameters, expression.Arguments[0], ref builderParameters.StreamingDisabled, exact);
        
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
            throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support proximity over search() method");
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
        
        return indexSearcher.SearchQuery(fieldMetadata, GetValues(), @operator, builderParameters.Index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes, builderParameters.Token);
        
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

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), operation, token: builderParameters.Token);
    }

    private static IQueryMatch HandleRegex(Parameters builderParameters, MethodExpression expression, ref StreamingOptimization streamingOptimization)
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
        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters.Allocator, fieldName, builderParameters.Index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);

        return streamingOptimization.TrySetMultiTermMatchAsStreamingField(builderParameters.IndexSearcher, fieldMetadata, MethodType.Regex) 
            ? builderParameters.IndexSearcher.RegexQuery(fieldMetadata, builderParameters.Factories.GetRegexFactory(valueAsString), streamingOptimization.Forward, streamingEnabled: true, token: builderParameters.Token) 
            : builderParameters.IndexSearcher.RegexQuery(fieldMetadata, builderParameters.Factories.GetRegexFactory(valueAsString), token: builderParameters.Token);

        bool IsStringFamily(object value)
        {
            return value is string or StringSegment or LazyStringValue;
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
            {
                builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved();
                return new[] {new OrderMetadata(true, MatchCompareFieldType.Score)};
            }

            return null;
        }

        int sortIndex = 0;
        var sortArray = new OrderMetadata[8];

        foreach (var field in orderByFields)
        {
            if (field.OrderingType == OrderByFieldType.Random)
            {
                var seed = field.Arguments is {Length: > 0} ? 
                    (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) :
                    Random.Shared.Next(); // use a random seed if none is provided  
                sortArray[sortIndex++] = new OrderMetadata(seed);
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                if (field.Ascending)
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score);
                else
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, ascending: false);

                continue;
            }
            
            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields, indexMapping, queryMapping, false);
            
            if (builderParameters.IndexSearcher.GetTermAmountInField(fieldMetadata) == 0)
                continue;
            
            if (field.OrderingType == OrderByFieldType.Distance)
            {
                var spatialField = getSpatialField(field.Name);

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
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields,
                indexMapping, queryMapping, false);
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
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

    private static IQueryMatch OrderBy(Parameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadata)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;
        switch (orderMetadata.Length)
        {
            case 0:
                return match;
            case 1:
                return indexSearcher.OrderBy(match, orderMetadata[0], take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, take, builderParameters.Token);
        }
    }
}
