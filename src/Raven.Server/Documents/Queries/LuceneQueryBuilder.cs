using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Queries;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Spatial4n.Shapes;
using FuzzyQuery = Lucene.Net.Search.FuzzyQuery;
using Index = Raven.Server.Documents.Indexes.Index;
using KeywordAnalyzer = Lucene.Net.Analysis.KeywordAnalyzer;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Lucene;
using Query = Raven.Server.Documents.Queries.AST.Query;
using Term = Lucene.Net.Index.Term;
using TermQuery = Lucene.Net.Search.TermQuery;
using TermRangeQuery = Lucene.Net.Search.TermRangeQuery;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries
{
    public sealed class LuceneQueryBuilder
    {
        private static readonly KeywordAnalyzer KeywordAnalyzer = new KeywordAnalyzer();

        private static readonly MultiTermQuery.ConstantScoreAutoRewrite _secondaryBetweenRewriteMethod = new MultiTermQuery.ConstantScoreAutoRewrite
        {
            TermCountCutoff = BooleanQuery.MaxClauseCount, // can be set using Query.MaxClauseCount
            DocCountPercent = 200 // disable
        };

        public static Lucene.Net.Search.Query BuildQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata,
            QueryExpression whereExpression,
            Index index, BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, List<string> buildSteps = null)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var luceneQuery = ToLuceneQuery(serverContext, context, metadata.Query, whereExpression, metadata, index, parameters, analyzer, factories,
                    buildSteps: buildSteps);

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        public static MoreLikeThisQuery.MoreLikeThisQuery BuildMoreLikeThisQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata,
            Index index, QueryExpression whereExpression, BlittableJsonReaderObject parameters, LuceneRavenPerFieldAnalyzerWrapper analyzer,
            QueryBuilderFactories factories)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var filterQuery = BuildQuery(serverContext, context, metadata, whereExpression, index, parameters, analyzer, factories);
                var moreLikeThisQuery = ToMoreLikeThisQuery(serverContext, context, metadata.Query, whereExpression, metadata, index, parameters, analyzer, factories,
                    out var baseDocument, out var options);

                return new MoreLikeThisQuery.MoreLikeThisQuery { BaseDocument = baseDocument, BaseDocumentQuery = moreLikeThisQuery, FilterQuery = filterQuery, Options = options };
            }
        }

        private static Lucene.Net.Search.Query ToMoreLikeThisQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
            QueryExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, out string baseDocument, out BlittableJsonReaderObject options)
        {
            baseDocument = null;
            options = null;

            var moreLikeThisExpression = QueryBuilderHelper.FindMoreLikeThisExpression(expression);
            if (moreLikeThisExpression == null)
                throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

            if (moreLikeThisExpression.Arguments.Count == 2)
            {
                var value = QueryBuilderHelper.GetValue(query, metadata, parameters, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
                if (value.Type == ValueTokenType.String)
                    options = IndexOperationBase.ParseJsonStringIntoBlittable(QueryBuilderHelper.GetValueAsString(value.Value), context);
                else
                    options = value.Value as BlittableJsonReaderObject;
            }

            var firstArgument = moreLikeThisExpression.Arguments[0];
            if (firstArgument is BinaryExpression binaryExpression)
                return ToLuceneQuery(serverContext, context, query, binaryExpression, metadata, index, parameters, analyzer, factories);

            var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(query, metadata, parameters, firstArgument).Value);
            if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
            {
                if (firstArgumentBool)
                    return new MatchAllDocsQuery();

                return new BooleanQuery(); // empty boolean query yields 0 documents
            }

            baseDocument = firstArgumentValue;
            return null;
        }

        private static Lucene.Net.Search.Query ToLuceneQuery(TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, Query query,
            QueryExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact = false, int? proximity = null, bool secondary = false,
            List<string> buildSteps = null)
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
                QueryBuilderHelper.ThrowQueryTooComplexException(metadata, parameters);

            if (expression == null)
                return new MatchAllDocsQuery();

            if (expression is BinaryExpression where)
            {
                buildSteps?.Add($"Where: {expression.Type} - {expression} (operator: {where.Operator})");

                switch (where.Operator)
                {
                    case OperatorType.Equal:
                    case OperatorType.NotEqual:
                    case OperatorType.GreaterThan:
                    case OperatorType.LessThan:
                    case OperatorType.LessThanEqual:
                    case OperatorType.GreaterThanEqual:
                        {
                            QueryExpression right = where.Right;

                            if (where.Right is MethodExpression rme)
                            {
                                right = EvaluateMethod(query, metadata, serverContext, documentsContext.DocumentDatabase.CompareExchangeStorage, rme, ref parameters);
                            }

                            var fieldName = ExtractIndexFieldName(query, parameters, where.Left, metadata);

                            exact = IsExact(index, exact, fieldName);

                            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, right, true);

                            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueType);

                            switch (fieldType)
                            {
                                case IndexFieldType.String:
                                    if (exact && metadata.IsDynamic)
                                        luceneFieldName = AutoIndexField.GetExactAutoIndexFieldName(luceneFieldName);

                                    exact |= valueType == ValueTokenType.Parameter;

                                    if (QueryBuilderHelper.TryUseTime(index, fieldName, value, exact, out var ticks))
                                    {
                                        return TranslateDateRangeQuery(index, where, fieldName, ticks);
                                    }

                                    var valueAsString = QueryBuilderHelper.GetValueAsString(value);

                                    switch (where.Operator)
                                    {
                                        case OperatorType.Equal:
                                            return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.NotEqual:
                                            return LuceneQueryHelper.NotEqual(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.LessThan:
                                            return LuceneQueryHelper.LessThan(index, luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.GreaterThan:
                                            return LuceneQueryHelper.GreaterThan(index, luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.LessThanEqual:
                                            return LuceneQueryHelper.LessThanOrEqual(index, luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.GreaterThanEqual:
                                            return LuceneQueryHelper.GreaterThanOrEqual(index, luceneFieldName, termType, valueAsString, exact);
                                    }

                                    break;

                                case IndexFieldType.Long:
                                    return TranslateOperationOnLong(index, luceneFieldName, (long)value, @where.Operator);

                                case IndexFieldType.Double:
                                    var valueAsDouble = (double)value;

                                    switch (where.Operator)
                                    {
                                        case OperatorType.Equal:
                                            return LuceneQueryHelper.Equal(index, luceneFieldName, valueAsDouble);
                                        case OperatorType.NotEqual:
                                            return LuceneQueryHelper.NotEqual(index, luceneFieldName, valueAsDouble);
                                        case OperatorType.LessThan:
                                            return LuceneQueryHelper.LessThan(index, luceneFieldName, valueAsDouble);
                                        case OperatorType.GreaterThan:
                                            return LuceneQueryHelper.GreaterThan(index, luceneFieldName, valueAsDouble);
                                        case OperatorType.LessThanEqual:
                                            return LuceneQueryHelper.LessThanOrEqual(index, luceneFieldName, valueAsDouble);
                                        case OperatorType.GreaterThanEqual:
                                            return LuceneQueryHelper.GreaterThanOrEqual(index, luceneFieldName, valueAsDouble);
                                    }

                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(fieldType), fieldType, null);
                            }

                            throw new NotSupportedException("Should not happen!");
                        }
                    case OperatorType.And:
                        {
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
                                    return TranslateBetweenQuery(query, metadata, index, parameters, exact, bq, secondary);
                            }

                            var left = ToLuceneQuery(serverContext, documentsContext, query, @where.Left, metadata, index, parameters, analyzer,
                                factories, exact, secondary: secondary, buildSteps: buildSteps);
                            var right = ToLuceneQuery(serverContext, documentsContext, query, @where.Right, metadata, index, parameters, analyzer,
                                factories, exact, secondary: true, buildSteps: buildSteps);

                            if (left is RavenBooleanQuery rbq)
                            {
                                if (rbq.TryAnd(right, buildSteps) == false)
                                    rbq = new RavenBooleanQuery(left, right, OperatorType.And, buildSteps);
                            }
                            else
                            {
                                rbq = new RavenBooleanQuery(OperatorType.And);
                                rbq.And(left, right, buildSteps);
                            }

                            return rbq;
                        }
                    case OperatorType.Or:
                        {
                            var left = ToLuceneQuery(serverContext, documentsContext, query, @where.Left, metadata, index, parameters, analyzer,
                                factories, exact, secondary: secondary, buildSteps: buildSteps);
                            var right = ToLuceneQuery(serverContext, documentsContext, query, @where.Right, metadata, index, parameters, analyzer,
                                factories, exact, secondary: true, buildSteps: buildSteps);

                            buildSteps?.Add(
                                $"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

                            if (left is RavenBooleanQuery rbq)
                            {
                                if (rbq.TryOr(right, buildSteps) == false)
                                {
                                    rbq = new RavenBooleanQuery(left, right, OperatorType.Or, buildSteps);

                                    buildSteps?.Add($"Created RavenBooleanQuery because TryOr returned false - {rbq}");
                                }
                            }
                            else
                            {
                                rbq = new RavenBooleanQuery(OperatorType.Or);
                                rbq.Or(left, right, buildSteps);

                                buildSteps?.Add($"Created RavenBooleanQuery - {rbq}");
                            }

                            return rbq;
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
                    return ToLuceneQuery(serverContext, documentsContext, query, newExpr, metadata, index, parameters, analyzer, factories, exact,
                        buildSteps: buildSteps);
                }

                var inner = ToLuceneQuery(serverContext, documentsContext, query, ne.Expression,
                    metadata, index, parameters, analyzer, factories, exact, secondary: secondary, buildSteps: buildSteps);
                return new BooleanQuery { { inner, Occur.MUST_NOT }, { new MatchAllDocsQuery(), Occur.SHOULD } };
            }

            if (expression is BetweenExpression be)
            {
                buildSteps?.Add($"Between: {expression.Type} - {be}");

                return TranslateBetweenQuery(query, metadata, index, parameters, exact, be, secondary);
            }

            if (expression is InExpression ie)
            {
                buildSteps?.Add($"In: {expression.Type} - {ie}");

                var fieldName = ExtractIndexFieldName(query, parameters, ie.Source, metadata);
                exact = IsExact(index, exact, fieldName);
                var termType = LuceneTermType.Null;
                var hasGotTheRealType = false;

                if (ie.All)
                {
                    var allInQuery = new BooleanQuery();
                    foreach (var value in GetValuesForIn(query, ie, metadata, parameters))
                    {
                        if (hasGotTheRealType == false)
                        {
                            // here we assume that all the values are of the same type
                            termType = GetLuceneField(fieldName, value.Type).LuceneTermType;
                            hasGotTheRealType = true;
                        }

                        if (exact && metadata.IsDynamic)
                            fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

                        allInQuery.Add(LuceneQueryHelper.Equal(fieldName, termType, value.Value, exact), Occur.MUST);
                    }

                    return allInQuery;
                }

                var matches = new List<string>();
                foreach (var tuple in GetValuesForIn(query, ie, metadata, parameters))
                {
                    if (hasGotTheRealType == false)
                    {
                        // we assume that the type of all the parameters is the same
                        termType = GetLuceneField(fieldName, tuple.Type).LuceneTermType;
                        hasGotTheRealType = true;
                    }

                    matches.Add(LuceneQueryHelper.GetTermValue(tuple.Value, termType, exact || tuple.Type == ValueTokenType.Parameter));
                }

                return new InQuery(fieldName, matches);
            }

            if (expression is TrueExpression)
            {
                buildSteps?.Add($"True: {expression.Type} - {expression}");

                return new MatchAllDocsQuery();
            }

            if (expression is MethodExpression me)
            {

                var methodName = me.Name.Value;
                var methodType = QueryMethod.GetMethodType(methodName);

                buildSteps?.Add($"Method: {expression.Type} - {me} - method: {methodType}, {methodName}");

                switch (methodType)
                {
                    case MethodType.Search:
                        return HandleSearch(query, me, metadata, parameters, analyzer, proximity);
                    case MethodType.Fuzzy:
                        return HandleFuzzy(serverContext, documentsContext, query, me, metadata, index, parameters, analyzer, factories, exact);
                    case MethodType.Proximity:
                        return HandleProximity(serverContext, documentsContext, query, me, metadata, index, parameters, analyzer, factories, exact);
                    case MethodType.Boost:
                        return HandleBoost(serverContext, documentsContext, query, me, metadata, index, parameters, analyzer, factories, exact, buildSteps);
                    case MethodType.Regex:
                        return HandleRegex(query, me, metadata, parameters, factories);
                    case MethodType.StartsWith:
                        return HandleStartsWith(query, me, metadata, index, parameters, exact);
                    case MethodType.EndsWith:
                        return HandleEndsWith(query, me, metadata, index, parameters, exact);
                    case MethodType.Lucene:
                        return HandleLucene(query, me, metadata, parameters, analyzer, exact);
                    case MethodType.Exists:
                        return HandleExists(query, parameters, me, metadata);
                    case MethodType.Exact:
                        return HandleExact(serverContext, documentsContext, query, me, metadata, index, parameters, analyzer, factories);
                    case MethodType.Spatial_Within:
                    case MethodType.Spatial_Contains:
                    case MethodType.Spatial_Disjoint:
                    case MethodType.Spatial_Intersects:
                        return HandleSpatial(query, me, metadata, parameters, methodType, factories.GetSpatialFieldFactory);
                    case MethodType.MoreLikeThis:
                        return new MatchAllDocsQuery();
                    case MethodType.Vector_Search:
                        throw new InvalidQueryException("Vector Search is not available for the Lucene indexing engine, only in the Corax indexing engine.", query.QueryText, parameters);
                        break;
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                        return null; // never hit
                }
            }

            throw new InvalidQueryException("Unable to understand query", query.QueryText, parameters);
        }

        private static Lucene.Net.Search.Query TranslateOperationOnLong(Index index, string luceneFieldName, long valueAsLong, OperatorType whereOperator)
        {
            return whereOperator switch
            {
                OperatorType.Equal => LuceneQueryHelper.Equal(index, luceneFieldName, valueAsLong),
                OperatorType.NotEqual => LuceneQueryHelper.NotEqual(index, luceneFieldName, valueAsLong),
                OperatorType.LessThan => LuceneQueryHelper.LessThan(index, luceneFieldName, valueAsLong),
                OperatorType.GreaterThan => LuceneQueryHelper.GreaterThan(index, luceneFieldName, valueAsLong),
                OperatorType.LessThanEqual => LuceneQueryHelper.LessThanOrEqual(index, luceneFieldName, valueAsLong),
                OperatorType.GreaterThanEqual => LuceneQueryHelper.GreaterThanOrEqual(index, luceneFieldName, valueAsLong),
                _ => throw new ArgumentOutOfRangeException(whereOperator.ToString())
            };
        }

        private static Lucene.Net.Search.Query TranslateDateRangeQuery(Index index, BinaryExpression @where, QueryFieldName fieldName, long ticks)
        {
            string luceneFieldName = fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix;

            switch (@where.Operator)
            {
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                    return TranslateOperationOnLong(index, luceneFieldName, ticks, @where.Operator);
            }

            var ticksAligned = ticks - (ticks % TimeSpan.TicksPerDay);
            if (ticksAligned == ticks ||                      // aligned already on a day boundary
                index.Configuration.QueryClauseCacheDisabled) // if query clause cache disabled, no point in segmenting query
                return TranslateOperationOnLong(index, luceneFieldName, ticks, @where.Operator);
            var bq = new BooleanQuery();

            switch (@where.Operator)
            {
                case OperatorType.LessThan:
                case OperatorType.LessThanEqual:
                    bq.Add(LuceneQueryHelper.Between(index, luceneFieldName, ticksAligned, true, ticks, @where.Operator == OperatorType.LessThanEqual), Occur.SHOULD);
                    bq.Add(TranslateOperationOnLong(index, luceneFieldName, ticksAligned, OperatorType.LessThanEqual), Occur.SHOULD);
                    break;
                case OperatorType.GreaterThan:
                case OperatorType.GreaterThanEqual:
                    ticksAligned += TimeSpan.TicksPerDay; // move to next day boundary
                    bq.Add(LuceneQueryHelper.Between(index, luceneFieldName, ticks, @where.Operator == OperatorType.GreaterThanEqual, ticksAligned, true), Occur.SHOULD);
                    bq.Add(TranslateOperationOnLong(index, luceneFieldName, ticksAligned, OperatorType.GreaterThanEqual), Occur.SHOULD);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(@where.Operator.ToString());
            }

            return bq;

        }

        private static Lucene.Net.Search.Query TranslateBetweenQuery(Query query, QueryMetadata metadata, Index index, BlittableJsonReaderObject parameters, bool exact,
            BetweenExpression be, bool secondary)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, be.Source, metadata);
            var (valueFirst, valueFirstType) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Min);
            var (valueSecond, _) = QueryBuilderHelper.GetValue(query, metadata, parameters, be.Max);

            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueFirstType);

            Lucene.Net.Search.Query betweenQuery;
            switch (fieldType)
            {
                case IndexFieldType.String:
                    exact = IsExact(index, exact, fieldName);

                    if (QueryBuilderHelper.TryUseTime(index, fieldName, valueFirst, valueSecond, exact, out var ticksFirst, out var ticksSecond))
                    {
                        luceneFieldName += Constants.Documents.Indexing.Fields.TimeFieldSuffix;
                        betweenQuery = index.Configuration.QueryClauseCacheDisabled
                            ? LuceneQueryHelper.Between(index, luceneFieldName, ticksFirst, be.MinInclusive, ticksSecond, be.MaxInclusive)
                            : TranslateDateTimeBetween(index, be, fieldName, ticksFirst, ticksSecond);
                    }
                    else
                    {
                        var valueFirstAsString = QueryBuilderHelper.GetValueAsString(valueFirst);
                        var valueSecondAsString = QueryBuilderHelper.GetValueAsString(valueSecond);
                        betweenQuery = LuceneQueryHelper.Between(index, luceneFieldName, termType, valueFirstAsString, be.MinInclusive, valueSecondAsString,
                            be.MaxInclusive, exact);
                    }

                    break;
                case IndexFieldType.Long:
                    var valueFirstAsLong = (long)valueFirst;
                    var valueSecondAsLong = (long)valueSecond;
                    betweenQuery = LuceneQueryHelper.Between(index, luceneFieldName, valueFirstAsLong, be.MinInclusive, valueSecondAsLong, be.MaxInclusive);
                    break;
                case IndexFieldType.Double:
                    var valueFirstAsDouble = (double)valueFirst;
                    var valueSecondAsDouble = (double)valueSecond;
                    betweenQuery = LuceneQueryHelper.Between(index, luceneFieldName, valueFirstAsDouble, be.MinInclusive, valueSecondAsDouble, be.MaxInclusive);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(fieldType), fieldType, null);
            }

            if (secondary && betweenQuery is TermRangeQuery q)
            {
                q.RewriteMethod = _secondaryBetweenRewriteMethod;
            }

            return betweenQuery;
        }

        private static Lucene.Net.Search.Query TranslateDateTimeBetween(Index index, BetweenExpression be, QueryFieldName fieldName, long ticksFirst, long ticksSecond)
        {
            string luceneFieldName = fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix;

            var ticksFirstAligned = ticksFirst - (ticksFirst % TimeSpan.TicksPerDay);
            var ticksSecondAligned = ticksSecond - (ticksSecond % TimeSpan.TicksPerDay);
            if (ticksFirstAligned == ticksFirst && ticksSecond == ticksSecondAligned || // already aligned on day boundary 
                ticksFirstAligned == ticksSecondAligned) // or belonging to the same day...
            {
                return LuceneQueryHelper.Between(index, luceneFieldName, ticksFirst, be.MinInclusive, ticksSecond, be.MaxInclusive);
            }

            var bq = new BooleanQuery();
            var startInclusive = be.MinInclusive;
            if (ticksFirst != ticksFirstAligned)
            {
                ticksFirstAligned += TimeSpan.TicksPerDay; // move to tne _next_ day boundary
                bq.Add(LuceneQueryHelper.Between(index, luceneFieldName, ticksFirst, be.MinInclusive, ticksFirstAligned, true), Occur.SHOULD);
                startInclusive = true;
            }

            var endInclusive = be.MaxInclusive || ticksSecond != ticksSecondAligned;
            bq.Add(LuceneQueryHelper.Between(index, luceneFieldName, ticksFirstAligned, startInclusive, ticksSecondAligned, endInclusive), Occur.SHOULD);
            if (ticksSecond != ticksSecondAligned)
            {
                bq.Add(LuceneQueryHelper.Between(index, luceneFieldName, ticksSecondAligned, true, ticksSecond, be.MaxInclusive), Occur.SHOULD);
            }

            return bq;
        }

        private static bool IsExact(Index index, bool exact, QueryFieldName fieldName)
        {
            if (exact)
                return true;

            if (index?.Definition?.IndexFields != null && index.Definition.IndexFields.TryGetValue(fieldName, out var indexingOptions))
                return indexingOptions.Indexing == FieldIndexing.Exact;

            return false;
        }

        public static QueryExpression EvaluateMethod(Query query, QueryMetadata metadata, TransactionOperationContext serverContext, AbstractCompareExchangeStorage compareExchangeStorage, MethodExpression method, ref BlittableJsonReaderObject parameters)
        {
            var methodType = QueryMethod.GetMethodType(method.Name.Value);

            switch (methodType)
            {
                case MethodType.CompareExchange:
                    var v = QueryBuilderHelper.GetValue(query, metadata, parameters, method.Arguments[0]);
                    if (v.Type != ValueTokenType.String)
                        throw new InvalidQueryException("Expected value of type string, but got: " + v.Type, query.QueryText, parameters);

                    object value = null;
                    compareExchangeStorage.GetCompareExchangeValue(serverContext, v.Value.ToString()).Value?.TryGetMember(Constants.CompareExchange.ObjectFieldName, out value);

                    if (value == null)
                        return new ValueExpression(string.Empty, ValueTokenType.Null);

                    return new ValueExpression(value.ToString(), ValueTokenType.String);
            }

            throw new ArgumentException($"Unknown method {method.Name}");
        }

        private static IEnumerable<(string Value, ValueTokenType Type)> GetValuesForIn(
            Query query,
            InExpression expression,
            QueryMetadata metadata,
            BlittableJsonReaderObject parameters)
        {
            foreach (var val in expression.Values)
            {
                var valueToken = val as ValueExpression;
                if (valueToken == null)
                    ThrowInvalidInValue(query, parameters, val);

                foreach (var (value, type) in QueryBuilderHelper.GetValues(query, metadata, parameters, valueToken))
                {
                    string valueAsString;
                    switch (type)
                    {
                        case ValueTokenType.Long:
                            var valueAsLong = (long)value;
                            valueAsString = valueAsLong.ToString(CultureInfo.InvariantCulture);
                            break;
                        case ValueTokenType.Double:
                            var valueAsDbl = (double)value;
                            valueAsString = valueAsDbl.ToString("G");
                            break;
                        default:
                            valueAsString = value?.ToString();
                            break;
                    }

                    yield return (valueAsString, type);
                }
            }
        }

        [DoesNotReturn]
        private static void ThrowInvalidInValue(Query query, BlittableJsonReaderObject parameters, QueryExpression val)
        {
            throw new InvalidQueryException("Expected in argument to be value, but was: " + val, query.QueryText, parameters);
        }

        private static QueryFieldName ExtractIndexFieldName(Query query, BlittableJsonReaderObject parameters, QueryExpression field, QueryMetadata metadata)
        {
            if (field is FieldExpression fe)
                return metadata.GetIndexFieldName(fe, parameters);

            if (field is ValueExpression ve)
                return metadata.GetIndexFieldName(new QueryFieldName(ve.Token.Value, false), parameters);

            if (field is MethodExpression me)
            {
                var methodType = QueryMethod.GetMethodType(me.Name.Value);
                switch (methodType)
                {
                    case MethodType.Id:
                        if (me.Arguments == null || me.Arguments.Count == 0)
                            return QueryFieldName.DocumentId;
                        if (me.Arguments[0] is FieldExpression docAlias && docAlias.Compound.Count == 1 && docAlias.Compound[0].Equals(query.From.Alias))
                            return QueryFieldName.DocumentId;
                        throw new InvalidQueryException("id() can only be used on the root query alias but got: " + me.Arguments[0], query.QueryText, parameters);
                    case MethodType.Count:
                        if (me.Arguments == null || me.Arguments.Count == 0)
                            return QueryFieldName.Count;
                        if (me.Arguments[0] is FieldExpression countAlias && countAlias.Compound.Count == 1 && countAlias.Compound[0].Equals(query.From.Alias))
                            return QueryFieldName.Count;

                        throw new InvalidQueryException("count() can only be used on the root query alias but got: " + me.Arguments[0], query.QueryText, parameters);
                    case MethodType.Sum:
                        if (me.Arguments != null && me.Arguments.Count == 1 &&
                            me.Arguments[0] is FieldExpression f &&
                            f.Compound.Count == 1)
                            return new QueryFieldName(f.Compound[0].Value, f.IsQuoted);

                        throw new InvalidQueryException("sum() must be called with a single field name, but was called: " + me, query.QueryText, parameters);

                    default:
                        throw new InvalidQueryException("Method " + me.Name.Value + " cannot be used in an expression in this manner", query.QueryText, parameters);
                }
            }

            throw new InvalidQueryException("Expected field, got: " + field, query.QueryText, parameters);
        }

        private static QueryFieldName ExtractIndexFieldName(ValueExpression field, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            return metadata.GetIndexFieldName(new QueryFieldName(field.Token.Value, field.Value == ValueTokenType.String), parameters);
        }

        private static Lucene.Net.Search.Query HandleExists(Query query, BlittableJsonReaderObject parameters, MethodExpression expression, QueryMetadata metadata)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);

            return LuceneQueryHelper.Term(fieldName, LuceneQueryHelper.Asterisk, LuceneTermType.WildCard);
        }

        private static Lucene.Net.Search.Query HandleLucene(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters,
            Analyzer analyzer, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String && valueType != ValueTokenType.Null)
                QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("lucene", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            if (metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            if (valueType == ValueTokenType.Null)
                return LuceneQueryHelper.Equal(fieldName, LuceneTermType.Null, null, exact);

            if (exact)
                analyzer = KeywordAnalyzer;

            var parser = new Lucene.Net.QueryParsers.QueryParser(Version.LUCENE_29, fieldName, analyzer) { AllowLeadingWildcard = true };

            return parser.Parse(QueryBuilderHelper.GetValueAsString(value));
        }

        private static Lucene.Net.Search.Query HandleStartsWith(Query query, MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = QueryBuilderHelper.GetValueAsString(value);
            if (string.IsNullOrEmpty(valueAsString))
                valueAsString = LuceneQueryHelper.Asterisk;
            else
                valueAsString += LuceneQueryHelper.Asterisk;

            exact = IsExact(index, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.Prefix, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleEndsWith(Query query, MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = QueryBuilderHelper.GetValueAsString(value);
            valueAsString = string.IsNullOrEmpty(valueAsString)
                ? LuceneQueryHelper.Asterisk
                : valueAsString.Insert(0, LuceneQueryHelper.Asterisk);

            exact = IsExact(index, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.WildCard, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleProximity(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
            MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var proximity = int.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index, parameters, analyzer, factories, exact, proximity);
        }

        private static Lucene.Net.Search.Query HandleFuzzy(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
            MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var similarity = float.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index, parameters, analyzer, factories, exact);
            var tq = q as TermQuery;
            if (tq == null)
                throw new InvalidQueryException("Fuzzy only works on term queries", metadata.QueryText, parameters); // should not happen

            return new FuzzyQuery(tq.Term, similarity);
        }

        private static Lucene.Net.Search.Query HandleBoost(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
            MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact, List<string> buildSteps = null)
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

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index, parameters, analyzer, factories, exact,
                buildSteps: buildSteps);
            q.Boost = boost;

            return q;
        }

        private static Lucene.Net.Search.Query HandleRegex(Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, QueryBuilderFactories factories)
        {
            if (expression.Arguments.Count != 2)
                throw new ArgumentException(
                    $"Regex method was invoked with {expression.Arguments.Count} arguments ({expression})" +
                    " while it should be invoked with 2 arguments e.g. Regex(foo.Name,\"^[a-z]+?\")");

            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
            if (valueType != ValueTokenType.String && !(valueType == ValueTokenType.Parameter && IsStringFamily(value)))
                QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("regex", ValueTokenType.String, valueType, metadata.QueryText, parameters);
            var valueAsString = QueryBuilderHelper.GetValueAsString(value);
            return new RegexQuery(new Term(fieldName, valueAsString), factories.GetRegexFactory(valueAsString));
        }

        private static bool IsStringFamily(object value)
        {
            return value is string || value is StringSegment || value is LazyStringValue;
        }

        private static Lucene.Net.Search.Query HandleSearch(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters,
            Analyzer analyzer, int? proximity)
        {

            QueryFieldName fieldName;
            var isDocumentId = false;
            switch (expression.Arguments[0])
            {
                case FieldExpression ft:
                    fieldName = ExtractIndexFieldName(query, parameters, ft, metadata);
                    break;
                case ValueExpression vt:
                    fieldName = ExtractIndexFieldName(vt, metadata, parameters);
                    break;
                case MethodExpression me when QueryMethod.GetMethodType(me.Name.Value) == MethodType.Id:
                    fieldName = QueryFieldName.DocumentId;
                    isDocumentId = true;
                    break;
                default:
                    throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " +
                                                        expression.Arguments[0]);
            }

            var (value, valueType) = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                QueryBuilderHelper.ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

            if (metadata.IsDynamic && isDocumentId == false)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            var valueAsString = QueryBuilderHelper.GetValueAsString(value);
            if (proximity.HasValue)
            {
                var type = GetTermType(valueAsString);
                if (type != LuceneTermType.String)
                    throw new InvalidQueryException("Proximity search works only on simple string terms, not wildcard or prefix ones", metadata.QueryText, parameters);

                var pq = LuceneQueryHelper.AnalyzedTerm(fieldName, valueAsString, type, analyzer) as PhraseQuery; // this will return PQ, unless there is a single term
                if (pq == null)
                    throw new InvalidQueryException("Proximity search works only on multiple search terms", metadata.QueryText, parameters);

                pq.Slop = proximity.Value;

                return pq;
            }

            BooleanQuery q = null;
            var occur = Occur.SHOULD;
            Lucene.Net.Search.Query firstQuery = null;
            foreach (var v in GetValues())
            {
                var t = LuceneQueryHelper.AnalyzedTerm(fieldName, v, GetTermType(v), analyzer);
                if (firstQuery == null && q == null)
                {
                    firstQuery = t;
                    continue;
                }

                if (q == null)
                {
                    q = new BooleanQuery();

                    if (expression.Arguments.Count == 3)
                    {
                        var fieldExpression = (FieldExpression)expression.Arguments[2];
                        if (fieldExpression.Compound.Count != 1)
                            QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);

                        var op = fieldExpression.Compound[0];

                        if (string.Equals("AND", op.Value, StringComparison.OrdinalIgnoreCase))
                            occur = Occur.MUST;
                        else if (string.Equals("OR", op.Value, StringComparison.OrdinalIgnoreCase))
                            occur = Occur.SHOULD;
                        else
                            QueryBuilderHelper.ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);
                    }

                    q.Add(firstQuery, occur);
                    firstQuery = null;
                }

                q.Add(t, occur);
            }

            if (firstQuery != null)
                return firstQuery;

            if (q == null)
                q = new BooleanQuery(); // RavenDB-16439

            return q;

            LuceneTermType GetTermType(string termValue)
            {
                if (string.IsNullOrEmpty(termValue))
                    return LuceneTermType.String;

                if (termValue[0] == LuceneQueryHelper.AsteriskChar)
                    return LuceneTermType.WildCard;

                if (termValue[termValue.Length - 1] == LuceneQueryHelper.AsteriskChar)
                {
                    if (termValue[termValue.Length - 2] != '\\')
                        return LuceneTermType.Prefix;
                }

                return LuceneTermType.String;
            }

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

        private static Lucene.Net.Search.Query HandleSpatial(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters,
            MethodType spatialMethod, Func<string, SpatialField> getSpatialField)
        {
            string fieldName;
            if (metadata.IsDynamic == false)
                fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            else
            {
                var spatialExpression = (MethodExpression)expression.Arguments[0];
                fieldName = metadata.GetSpatialFieldName(spatialExpression, parameters);
            }

            var shapeExpression = (MethodExpression)expression.Arguments[1];

            var distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
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
                    shape = HandleCircle(query, shapeExpression, metadata, parameters, fieldName, spatialField, out _);
                    break;
                case MethodType.Spatial_Wkt:
                    shape = HandleWkt(query, shapeExpression, metadata, parameters, fieldName, spatialField, out _);
                    break;
                default:
                    QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                    break;
            }

            Debug.Assert(shape != null);

            SpatialOperation operation = null;
            switch (spatialMethod)
            {
                case MethodType.Spatial_Within:
                    operation = SpatialOperation.IsWithin;
                    break;
                case MethodType.Spatial_Contains:
                    operation = SpatialOperation.Contains;
                    break;
                case MethodType.Spatial_Disjoint:
                    operation = SpatialOperation.IsDisjointTo;
                    break;
                case MethodType.Spatial_Intersects:
                    operation = SpatialOperation.Intersects;
                    break;
                default:
                    QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, parameters);
                    break;
            }

            Debug.Assert(operation != null);

            var args = new SpatialArgs(operation, shape) { DistErrPct = distanceErrorPct };

            return spatialField.Strategy.MakeQuery(args);
        }

        internal static IShape HandleWkt(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField, out SpatialUnits units)
        {
            var wktValue = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            QueryBuilderHelper.AssertValueIsString(fieldName, wktValue.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 2)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[1] as ValueExpression, metadata, parameters, fieldName);

            units = spatialUnits ?? spatialField.Units;

            var wkt = QueryBuilderHelper.GetValueAsString(wktValue.Value);

            try
            {
                return spatialField.ReadShape(wkt, spatialUnits);
            }
            catch (Exception e)
            {
                throw new InvalidQueryException($"Value '{wkt}' is not a valid WKT value.", query.QueryText, parameters, e);
            }
        }

        internal static IShape HandleCircle(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField, out SpatialUnits units)
        {
            var radius = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, radius.Type);

            var latitude = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, latitude.Type);

            var longitude = QueryBuilderHelper.GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, longitude.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 4)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[3] as ValueExpression, metadata, parameters, fieldName);

            units = spatialUnits ?? spatialField.Units;

            return spatialField.ReadCircle(Convert.ToDouble(radius.Value), Convert.ToDouble(latitude.Value), Convert.ToDouble(longitude.Value), spatialUnits);
        }

        private static SpatialUnits? GetSpatialUnits(Query query, ValueExpression value, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var spatialUnitsValue = QueryBuilderHelper.GetValue(query, metadata, parameters, value);
            QueryBuilderHelper.AssertValueIsString(fieldName, spatialUnitsValue.Type);

            var spatialUnitsValueAsString = QueryBuilderHelper.GetValueAsString(spatialUnitsValue.Value);
            if (Enum.TryParse(typeof(SpatialUnits), spatialUnitsValueAsString, true, out var su) == false)
                throw new InvalidOperationException(
                    $"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{spatialUnitsValueAsString}'.");

            return (SpatialUnits)su;
        }

        private static Lucene.Net.Search.Query HandleExact(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query,
            MethodExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index, parameters, analyzer, factories, exact: true);
        }

        internal static (string LuceneFieldName, IndexFieldType LuceneFieldType, LuceneTermType LuceneTermType) GetLuceneField(string fieldName, ValueTokenType valueType)
        {
            switch (valueType)
            {
                case ValueTokenType.String:
                    return (fieldName, IndexFieldType.String, LuceneTermType.String);
                case ValueTokenType.Double:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble, IndexFieldType.Double, LuceneTermType.Double);
                case ValueTokenType.Long:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong, IndexFieldType.Long, LuceneTermType.Long);
                case ValueTokenType.True:
                case ValueTokenType.False:
                    return (fieldName, IndexFieldType.String, LuceneTermType.String);
                case ValueTokenType.Null:
                case ValueTokenType.Parameter:
                    return (fieldName, IndexFieldType.String, LuceneTermType.String);
                default:
                    QueryBuilderHelper.ThrowUnhandledValueTokenType(valueType);
                    break;
            }

            Debug.Assert(false);

            return (null, IndexFieldType.String, LuceneTermType.String);
        }
    }
}
