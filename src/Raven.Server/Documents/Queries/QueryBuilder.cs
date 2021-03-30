using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Queries;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Spatial4n.Core.Shapes;
using Index = Raven.Server.Documents.Indexes.Index;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.MoreLikeThisQuery;
using Query = Raven.Server.Documents.Queries.AST.Query;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        private static readonly KeywordAnalyzer KeywordAnalyzer = new KeywordAnalyzer();

        private static readonly MultiTermQuery.ConstantScoreAutoRewrite _secondaryBetweenRewriteMethod = new MultiTermQuery.ConstantScoreAutoRewrite
        {
            TermCountCutoff = BooleanQuery.MaxClauseCount, // can be set using Query.MaxClauseCount
            DocCountPercent = 200 // disable
        };

        public static Lucene.Net.Search.Query BuildQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression,
            Index index, BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, List<string> buildSteps = null)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var luceneQuery = ToLuceneQuery(serverContext, context, metadata.Query, whereExpression, metadata, index, parameters, analyzer, factories, buildSteps: buildSteps);

                if (luceneQuery != null)
                    Console.WriteLine(luceneQuery.ToString());

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        public static MoreLikeThisQuery BuildMoreLikeThisQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters, RavenPerFieldAnalyzerWrapper analyzer, QueryBuilderFactories factories)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var filterQuery = BuildQuery(serverContext, context, metadata, whereExpression, index: null, parameters, analyzer, factories);
                var moreLikeThisQuery = ToMoreLikeThisQuery(serverContext, context, metadata.Query, whereExpression, metadata, parameters, analyzer, factories, out var baseDocument, out var options);

                return new MoreLikeThisQuery
                {
                    BaseDocument = baseDocument,
                    BaseDocumentQuery = moreLikeThisQuery,
                    FilterQuery = filterQuery,
                    Options = options
                };
            }
        }

        private static Lucene.Net.Search.Query ToMoreLikeThisQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, QueryExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, out string baseDocument, out BlittableJsonReaderObject options)
        {
            baseDocument = null;
            options = null;

            var moreLikeThisExpression = FindMoreLikeThisExpression(expression);
            if (moreLikeThisExpression == null)
                throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

            if (moreLikeThisExpression.Arguments.Count == 2)
            {
                var value = GetValue(query, metadata, parameters, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
                if (value.Type == ValueTokenType.String)
                    options = IndexReadOperation.ParseJsonStringIntoBlittable(GetValueAsString(value.Value), context);
                else
                    options = value.Value as BlittableJsonReaderObject;
            }

            var firstArgument = moreLikeThisExpression.Arguments[0];
            if (firstArgument is BinaryExpression binaryExpression)
                return ToLuceneQuery(serverContext, context, query, binaryExpression, metadata, index: null, parameters, analyzer, factories);

            var firstArgumentValue = GetValueAsString(GetValue(query, metadata, parameters, firstArgument).Value);
            if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
            {
                if (firstArgumentBool)
                    return new MatchAllDocsQuery();

                return new BooleanQuery(); // empty boolean query yields 0 documents
            }

            baseDocument = firstArgumentValue;
            return null;
        }

        private static MethodExpression FindMoreLikeThisExpression(QueryExpression expression)
        {
            if (expression == null)
                return null;

            if (expression is BinaryExpression where)
            {
                switch (where.Operator)
                {
                    case OperatorType.And:
                    case OperatorType.Or:
                        var leftExpression = FindMoreLikeThisExpression(where.Left);
                        if (leftExpression != null)
                            return leftExpression;

                        var rightExpression = FindMoreLikeThisExpression(where.Right);
                        if (rightExpression != null)
                            return rightExpression;

                        return null;
                    default:
                        return null;
                }
            }

            if (expression is MethodExpression me)
            {
                var methodName = me.Name.Value;
                var methodType = QueryMethod.GetMethodType(methodName);

                switch (methodType)
                {
                    case MethodType.MoreLikeThis:
                        return me;
                    default:
                        return null;
                }
            }

            return null;
        }

        private static Lucene.Net.Search.Query ToLuceneQuery(TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, Query query,
            QueryExpression expression, QueryMetadata metadata, Index index,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact = false, int? proximity = null, bool secondary = false, List<string> buildSteps = null)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

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
                                right = EvaluateMethod(query, metadata, serverContext, documentsContext, rme, ref parameters);
                            }

                            var fieldName = ExtractIndexFieldName(query, parameters, where.Left, metadata);

                            exact = IsExact(index, exact, fieldName);

                            var (value, valueType) = GetValue(query, metadata, parameters, right, true);

                            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueType);

                            switch (fieldType)
                            {
                                case LuceneFieldType.String:
                                    if (exact && metadata.IsDynamic)
                                        luceneFieldName = AutoIndexField.GetExactAutoIndexFieldName(luceneFieldName);

                                    exact |= valueType == ValueTokenType.Parameter;

                                    if (CanUseTimeRanges(index))
                                    {
                                        if (TryUseTimeRanges(index, fieldName, value, exact, out var date))
                                        {
                                            var booleanQuery = new RavenBooleanQuery(OperatorType.And);

                                            var ticksQuery = ToLong(where.Operator, fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix, date.Ticks);
                                            var yearMonthDayQuery = ToYearMonthDayQuery(where.Operator, fieldName, date);
                                            var yearMonthQuery = ToYearMonthQuery(where.Operator, fieldName, date);
                                            var yearQuery = ToYearQuery(where.Operator, fieldName, date);

                                            //booleanQuery.TryAnd(yearQuery, buildSteps);
                                            //booleanQuery.TryAnd(yearMonthQuery, buildSteps);
                                            //booleanQuery.TryAnd(yearMonthDayQuery, buildSteps);
                                            booleanQuery.TryAnd(ticksQuery, buildSteps);

                                            return booleanQuery;

                                            static Lucene.Net.Search.Query ToYearQuery(OperatorType operatorType, string fieldName, DateTime date)
                                            {
                                                operatorType = AdjustOperator(operatorType);
                                                return ToLong(operatorType, fieldName + Constants.Documents.Indexing.Fields.TimeYearFieldSuffix, date.Year);
                                            }

                                            static Lucene.Net.Search.Query ToYearMonthQuery(OperatorType operatorType, string fieldName, DateTime date)
                                            {
                                                operatorType = AdjustOperator(operatorType);
                                                var yearMonth = new DateTime(date.Year, date.Month, 1, 0, 0, 0, date.Kind);

                                                return ToLong(operatorType, fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthFieldSuffix, yearMonth.Ticks);
                                            }

                                            static Lucene.Net.Search.Query ToYearMonthDayQuery(OperatorType operatorType, string fieldName, DateTime date)
                                            {
                                                operatorType = AdjustOperator(operatorType);
                                                var yearMonthDay = date.Date;

                                                return ToLong(operatorType, fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthDayFieldSuffix, yearMonthDay.Ticks);
                                            }

                                            static OperatorType AdjustOperator(OperatorType operatorType)
                                            {
                                                switch (operatorType)
                                                {
                                                    case OperatorType.Equal:
                                                    case OperatorType.NotEqual:
                                                    case OperatorType.LessThanEqual:
                                                    case OperatorType.GreaterThanEqual:
                                                        return operatorType;
                                                    case OperatorType.LessThan:
                                                        return OperatorType.LessThanEqual;
                                                    case OperatorType.GreaterThan:
                                                        return OperatorType.GreaterThanEqual;
                                                    default:
                                                        throw new NotSupportedException("Should not happen!");
                                                }
                                            }
                                        }
                                    }
                                    else if (CanUseTimeTicks(index))
                                    {
                                        if (TryUseTime(index, fieldName, value, exact, out var ticks))
                                            return ToLong(where.Operator, fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix, ticks);
                                    }

                                    var valueAsString = GetValueAsString(value);

                                    return ToString(where.Operator, luceneFieldName, termType, valueAsString, exact);
                                case LuceneFieldType.Long:
                                    var valueAsLong = (long)value;
                                    return ToLong(where.Operator, luceneFieldName, valueAsLong);
                                case LuceneFieldType.Double:
                                    var valueAsDouble = (double)value;
                                    return ToDouble(where.Operator, luceneFieldName, valueAsDouble);
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(fieldType), fieldType, null);
                            }

                            static Lucene.Net.Search.Query ToString(OperatorType operatorType, string luceneFieldName, LuceneTermType termType, string value, bool exact)
                            {
                                switch (operatorType)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, termType, value, exact);
                                    case OperatorType.NotEqual:
                                        return LuceneQueryHelper.NotEqual(luceneFieldName, termType, value, exact);
                                    case OperatorType.LessThan:
                                        return LuceneQueryHelper.LessThan(luceneFieldName, termType, value, exact);
                                    case OperatorType.GreaterThan:
                                        return LuceneQueryHelper.GreaterThan(luceneFieldName, termType, value, exact);
                                    case OperatorType.LessThanEqual:
                                        return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, termType, value, exact);
                                    case OperatorType.GreaterThanEqual:
                                        return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, termType, value, exact);
                                    default:
                                        throw new NotSupportedException("Should not happen!");
                                }
                            }

                            static Lucene.Net.Search.Query ToLong(OperatorType operatorType, string luceneFieldName, long value)
                            {
                                switch (operatorType)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, value);
                                    case OperatorType.NotEqual:
                                        return LuceneQueryHelper.NotEqual(luceneFieldName, value);
                                    case OperatorType.LessThan:
                                        return LuceneQueryHelper.LessThan(luceneFieldName, value);
                                    case OperatorType.GreaterThan:
                                        return LuceneQueryHelper.GreaterThan(luceneFieldName, value);
                                    case OperatorType.LessThanEqual:
                                        return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, value);
                                    case OperatorType.GreaterThanEqual:
                                        return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, value);
                                    default:
                                        throw new NotSupportedException("Should not happen!");
                                }
                            }

                            static Lucene.Net.Search.Query ToDouble(OperatorType operatorType, string luceneFieldName, double value)
                            {
                                switch (operatorType)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, value);
                                    case OperatorType.NotEqual:
                                        return LuceneQueryHelper.NotEqual(luceneFieldName, value);
                                    case OperatorType.LessThan:
                                        return LuceneQueryHelper.LessThan(luceneFieldName, value);
                                    case OperatorType.GreaterThan:
                                        return LuceneQueryHelper.GreaterThan(luceneFieldName, value);
                                    case OperatorType.LessThanEqual:
                                        return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, value);
                                    case OperatorType.GreaterThanEqual:
                                        return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, value);
                                    default:
                                        throw new NotSupportedException("Should not happen!");
                                }
                            }
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
                                    return TranslateBetweenQuery(query, metadata, index, parameters, exact, bq, secondary, buildSteps);
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

                            buildSteps?.Add($"OR operator: left - {left.GetType().FullName} ({left}) assembly: {left.GetType().Assembly.FullName} assemby location: {left.GetType().Assembly.Location} , right - {right.GetType().FullName} ({right}) assemlby: {right.GetType().Assembly.FullName} assemby location: {right.GetType().Assembly.Location}");

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
                    return ToLuceneQuery(serverContext, documentsContext, query, newExpr, metadata, index, parameters, analyzer, factories, exact, buildSteps: buildSteps);
                }

                var inner = ToLuceneQuery(serverContext, documentsContext, query, ne.Expression,
                    metadata, index, parameters, analyzer, factories, exact, secondary: secondary, buildSteps: buildSteps);
                return new BooleanQuery
                {
                    {inner,  Occur.MUST_NOT},
                    {new MatchAllDocsQuery(), Occur.SHOULD}
                };
            }
            if (expression is BetweenExpression be)
            {
                buildSteps?.Add($"Between: {expression.Type} - {be}");

                return TranslateBetweenQuery(query, metadata, index, parameters, exact, be, secondary, buildSteps);
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
                        return HandleFuzzy(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Proximity:
                        return HandleProximity(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Boost:
                        return HandleBoost(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact, buildSteps);
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
                        return HandleExact(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories);
                    case MethodType.Spatial_Within:
                    case MethodType.Spatial_Contains:
                    case MethodType.Spatial_Disjoint:
                    case MethodType.Spatial_Intersects:
                        return HandleSpatial(query, me, metadata, parameters, methodType, factories.GetSpatialFieldFactory);
                    case MethodType.MoreLikeThis:
                        return new MatchAllDocsQuery();
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                        return null; // never hit
                }
            }

            throw new InvalidQueryException("Unable to understand query", query.QueryText, parameters);
        }

        private static Lucene.Net.Search.Query TranslateBetweenQuery(Query query, QueryMetadata metadata, Index index, BlittableJsonReaderObject parameters, bool exact, BetweenExpression be, bool secondary, List<string> buildSteps)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, be.Source, metadata);
            var (valueFirst, valueFirstType) = GetValue(query, metadata, parameters, be.Min);
            var (valueSecond, _) = GetValue(query, metadata, parameters, be.Max);

            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueFirstType);

            Lucene.Net.Search.Query betweenQuery;
            switch (fieldType)
            {
                case LuceneFieldType.String:
                    exact = IsExact(index, exact, fieldName);

                    if (CanUseTimeRanges(index))
                    {
                        if (TryUseTimeRanges(index, fieldName, valueFirst, valueSecond, exact, out var start, out var end))
                        {
                            var booleanQuery = new RavenBooleanQuery(OperatorType.Or);

                            if (false) // apply only if e.g. range is more than 7 days
                            {
                                //betweenQuery = ticksQuery;
                                break;
                            }

                            var rangeEnd = start.AddDays(1).Date; // the first day
                            booleanQuery.TryOr(LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix, start.Ticks, be.MinInclusive, rangeEnd.Ticks, toInclusive: true), buildSteps);
                            AddDays();
                            AddMonths();
                            AddYears();
                            AddMonths();
                            AddDays();
                            booleanQuery.TryOr(LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix, start.Ticks, be.MinInclusive, end.Ticks, be.MaxInclusive), buildSteps);

                            betweenQuery = booleanQuery;
                            break;

                            void AddDays()
                            {
                                var last = new DateTime(rangeEnd.Year, rangeEnd.Month, 1).AddMonths(1).AddDays(-1);
                                if (last.Year == end.Year && last.Month == end.Month)
                                {
                                    last = end.Date.AddDays(-1);
                                }
                                if (last >= start)
                                {
                                    rangeEnd = last;
                                    if (last == start)
                                    {
                                        booleanQuery.TryOr(LuceneQueryHelper.Equal(fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthDayFieldSuffix, last.Ticks), buildSteps);
                                    }
                                    else
                                    {
                                        booleanQuery.TryOr(LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthDayFieldSuffix, start.Date.Ticks, fromInclusive: true, rangeEnd.Ticks, toInclusive: true), buildSteps);
                                    }
                                    start = rangeEnd = rangeEnd.AddDays(1); // place at start of next day
                                }
                            }

                            void AddMonths()
                            {
                                var last = new DateTime(rangeEnd.Year, 12, 1);
                                if (last.Year == end.Year)
                                {
                                    last = new DateTime(end.Year, end.Month,1).AddMonths(-1);
                                }
                                if (last >= start) // to the end of the year
                                {
                                    rangeEnd = last;
                                    if (start == rangeEnd)
                                    {
                                        booleanQuery.TryOr(LuceneQueryHelper.Equal(fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthFieldSuffix, last.Ticks), buildSteps);
                                    }
                                    else
                                    {
                                        booleanQuery.TryOr(LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeYearMonthFieldSuffix, start.Date.Ticks, fromInclusive: true, rangeEnd.Ticks, toInclusive: true), buildSteps);
                                    }
                                    start = rangeEnd = rangeEnd.AddMonths(1); // place on start of next month
                                }
                            }

                            void AddYears()
                            {
                                if (start.Year < end.Year - 1) // need to cover the range in years
                                {
                                    rangeEnd = new DateTime(end.Year-1, 1, 1);
                                    booleanQuery.TryOr(LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeYearFieldSuffix, start.Year, fromInclusive: true, rangeEnd.Year, toInclusive: true), buildSteps);
                                    start = rangeEnd = rangeEnd.AddYears(1); // place on start of next year
                                }
                            }
                        }
                    }
                    else if (CanUseTimeTicks(index))
                    {
                        if (TryUseTime(index, fieldName, valueFirst, valueSecond, exact, out var ticksFirst, out var ticksSecond))
                        {
                            betweenQuery = LuceneQueryHelper.Between(fieldName + Constants.Documents.Indexing.Fields.TimeFieldSuffix, ticksFirst, be.MinInclusive, ticksSecond, be.MaxInclusive);
                            break;
                        }
                    }

                    var valueFirstAsString = GetValueAsString(valueFirst);
                    var valueSecondAsString = GetValueAsString(valueSecond);
                    betweenQuery = LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsString, be.MinInclusive, valueSecondAsString, be.MaxInclusive, exact);
                    break;
                case LuceneFieldType.Long:
                    var valueFirstAsLong = (long)valueFirst;
                    var valueSecondAsLong = (long)valueSecond;
                    betweenQuery = LuceneQueryHelper.Between(luceneFieldName, valueFirstAsLong, be.MinInclusive, valueSecondAsLong, be.MaxInclusive);
                    break;
                case LuceneFieldType.Double:
                    var valueFirstAsDouble = (double)valueFirst;
                    var valueSecondAsDouble = (double)valueSecond;
                    betweenQuery = LuceneQueryHelper.Between(luceneFieldName, valueFirstAsDouble, be.MinInclusive, valueSecondAsDouble, be.MaxInclusive);
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

        private static bool CanUseTimeTicks(Index index)
        {

            if (index == null)
                return false;

            return index.Definition.Version >= IndexDefinitionBase.IndexVersion.TimeTicks;
        }

        private static bool CanUseTimeRanges(Index index)
        {
            if (index == null)
                return false;

            return index.Definition.Version >= IndexDefinitionBase.IndexVersion.TimeRanges;
        }

        private static bool TryUseTimeRanges(Index index, string fieldName, object valueFirst, object valueSecond, bool exact, out DateTime dateFirst, out DateTime dateSecond)
        {
            dateFirst = default;
            dateSecond = default;

            if (exact || valueFirst == null || valueSecond == null || CanUseTimeRanges(index) == false)
                return false;

            if (index.IndexFieldsPersistence.HasTimeValues(fieldName))
            {
                var resultFirst = TryGetTime(valueFirst, out var dtFirst, out var dtoFirst);
                if (resultFirst == LazyStringParser.Result.Failed)
                    return false;

                var resultSecond = TryGetTime(valueSecond, out var dtSecond, out var dtoSecond);
                if (resultSecond == LazyStringParser.Result.Failed)
                    return false;

                dateFirst = TimeToDate(resultFirst, dtFirst, dtoFirst);
                dateSecond = TimeToDate(resultSecond, dtSecond, dtoSecond);
                return true;
            }

            return false;
        }

        private static bool TryUseTimeRanges(Index index, string fieldName, object value, bool exact, out DateTime date)
        {
            date = default;

            if (exact || value == null || CanUseTimeRanges(index) == false)
                return false;

            if (index.IndexFieldsPersistence.HasTimeValues(fieldName))
            {
                var result = TryGetTime(value, out var dt, out var dto);
                if (result == LazyStringParser.Result.Failed)
                    return false;

                date = TimeToDate(result, dt, dto);
                return true;
            }

            return false;
        }

        private static bool TryUseTime(Index index, string fieldName, object valueFirst, object valueSecond, bool exact, out long ticksFirst, out long ticksSecond)
        {
            ticksFirst = -1;
            ticksSecond = -1;

            if (exact || valueFirst == null || valueSecond == null || CanUseTimeTicks(index) == false)
                return false;

            if (index.IndexFieldsPersistence.HasTimeValues(fieldName))
            {
                var resultFirst = TryGetTime(valueFirst, out var dtFirst, out var dtoFirst);
                if (resultFirst == LazyStringParser.Result.Failed)
                    return false;

                var resultSecond = TryGetTime(valueSecond, out var dtSecond, out var dtoSecond);
                if (resultSecond == LazyStringParser.Result.Failed)
                    return false;

                ticksFirst = TimeToTicks(resultFirst, dtFirst, dtoFirst);
                ticksSecond = TimeToTicks(resultSecond, dtSecond, dtoSecond);
                return true;
            }

            return false;
        }

        private static bool TryUseTime(Index index, string fieldName, object value, bool exact, out long ticks)
        {
            ticks = -1;

            if (exact || value == null || CanUseTimeTicks(index) == false)
                return false;

            if (index.IndexFieldsPersistence.HasTimeValues(fieldName))
            {
                var result = TryGetTime(value, out var dt, out var dto);
                if (result == LazyStringParser.Result.Failed)
                    return false;

                ticks = TimeToTicks(result, dt, dto);
                return true;
            }

            return false;
        }

        private static unsafe LazyStringParser.Result TryGetTime(object value, out DateTime dt, out DateTimeOffset dto)
        {
            switch (value)
            {
                case LazyStringValue lsv:
                    return LazyStringParser.TryParseDateTime(lsv.Buffer, lsv.Size, out dt, out dto);
                case string valueAsString:
                    fixed (char* buffer = valueAsString)
                        return LazyStringParser.TryParseDateTime(buffer, valueAsString.Length, out dt, out dto);
                default:
                    var otherAsString = value.ToString();
                    fixed (char* buffer = otherAsString)
                        return LazyStringParser.TryParseDateTime(buffer, otherAsString.Length, out dt, out dto);
            }
        }

        private static DateTime TimeToDate(LazyStringParser.Result result, DateTime dt, DateTimeOffset dto)
        {
            switch (result)
            {
                case LazyStringParser.Result.DateTime:
                    return dt;
                case LazyStringParser.Result.DateTimeOffset:
                    return dto.UtcDateTime;
                default:
                    throw new InvalidOperationException("Should not happen!");
            }
        }

        private static long TimeToTicks(LazyStringParser.Result result, DateTime dt, DateTimeOffset dto)
        {
            switch (result)
            {
                case LazyStringParser.Result.DateTime:
                    return dt.Ticks;
                case LazyStringParser.Result.DateTimeOffset:
                    return dto.UtcDateTime.Ticks;
                default:
                    throw new InvalidOperationException("Should not happen!");
            }
        }

        private static bool IsExact(Index index, bool exact, QueryFieldName fieldName)
        {
            if (exact)
                return true;

            if (index?.Definition?.IndexFields != null && index.Definition.IndexFields.TryGetValue(fieldName, out var indexingOptions))
                return indexingOptions.Indexing == FieldIndexing.Exact;

            return false;
        }

        public static QueryExpression EvaluateMethod(Query query, QueryMetadata metadata, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, MethodExpression method, ref BlittableJsonReaderObject parameters)
        {
            var methodType = QueryMethod.GetMethodType(method.Name.Value);

            var server = documentsContext.DocumentDatabase.ServerStore;
            switch (methodType)
            {
                case MethodType.CompareExchange:
                    var v = GetValue(query, metadata, parameters, method.Arguments[0]);
                    if (v.Type != ValueTokenType.String)
                        throw new InvalidQueryException("Expected value of type string, but got: " + v.Type, query.QueryText, parameters);

                    var prefix = CompareExchangeKey.GetStorageKey(documentsContext.DocumentDatabase.Name, v.Value.ToString());
                    object value = null;
                    server.Cluster.GetCompareExchangeValue(serverContext, prefix).Value?.TryGetMember(Constants.CompareExchange.ObjectFieldName, out value);

                    if (value == null)
                        return new ValueExpression(string.Empty, ValueTokenType.Null);

                    return new ValueExpression(value.ToString(), ValueTokenType.String);
            }

            throw new ArgumentException($"Unknown method {method.Name}");
        }

        private static string GetValueAsString(object value)
        {
            if (!(value is string valueAsString))
            {
                if (value is StringSegment s)
                {
                    valueAsString = s.Value;
                }
                else
                {
                    valueAsString = value?.ToString();
                }
            }
            return valueAsString;
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

                foreach (var (value, type) in GetValues(query, metadata, parameters, valueToken))
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
            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String && valueType != ValueTokenType.Null)
                ThrowMethodExpectsArgumentOfTheFollowingType("lucene", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            if (metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            if (valueType == ValueTokenType.Null)
                return LuceneQueryHelper.Equal(fieldName, LuceneTermType.Null, null, exact);

            if (exact)
                analyzer = KeywordAnalyzer;

            var parser = new Lucene.Net.QueryParsers.QueryParser(Version.LUCENE_29, fieldName, analyzer)
            {
                AllowLeadingWildcard = true
            };

            return parser.Parse(GetValueAsString(value));
        }

        private static Lucene.Net.Search.Query HandleStartsWith(Query query, MethodExpression expression, QueryMetadata metadata, Index index, BlittableJsonReaderObject parameters, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = GetValueAsString(value);
            if (string.IsNullOrEmpty(valueAsString))
                valueAsString = LuceneQueryHelper.Asterisk;
            else
                valueAsString += LuceneQueryHelper.Asterisk;

            exact = IsExact(index, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.Prefix, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleEndsWith(Query query, MethodExpression expression, QueryMetadata metadata, Index index, BlittableJsonReaderObject parameters, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = GetValueAsString(value);
            valueAsString = string.IsNullOrEmpty(valueAsString)
                ? LuceneQueryHelper.Asterisk
                : valueAsString.Insert(0, LuceneQueryHelper.Asterisk);

            exact = IsExact(index, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.WildCard, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleProximity(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var proximity = int.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index: null, parameters, analyzer, factories, exact, proximity);
        }

        private static Lucene.Net.Search.Query HandleFuzzy(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var similarity = float.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index: null, parameters, analyzer, factories, exact);
            var tq = q as TermQuery;
            if (tq == null)
                throw new InvalidQueryException("Fuzzy only works on term queries", metadata.QueryText, parameters); // should not happen

            return new FuzzyQuery(tq.Term, similarity);
        }

        private static Lucene.Net.Search.Query HandleBoost(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact, List<string> buildSteps = null)
        {
            if (expression.Arguments.Count != 2)
            {
                throw new InvalidQueryException($"Boost(expression, boostVal) requires two arguments, but was called with {expression.Arguments.Count}",
                    metadata.QueryText, parameters);
            }


            float boost;
            var (val, type) = GetValue(query, metadata, parameters, expression.Arguments[1]);
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

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index: null, parameters, analyzer, factories, exact, buildSteps: buildSteps);
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
            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
            if (valueType != ValueTokenType.String && !(valueType == ValueTokenType.Parameter && IsStringFamily(value)))
                ThrowMethodExpectsArgumentOfTheFollowingType("regex", ValueTokenType.String, valueType, metadata.QueryText, parameters);
            var valueAsString = GetValueAsString(value);
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
                    throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);
            }

            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

            if (metadata.IsDynamic && isDocumentId == false)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            var valueAsString = GetValueAsString(value);
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
                            ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);

                        var op = fieldExpression.Compound[0];

                        if (string.Equals("AND", op.Value, StringComparison.OrdinalIgnoreCase))
                            occur = Occur.MUST;
                        else if (string.Equals("OR", op.Value, StringComparison.OrdinalIgnoreCase))
                            occur = Occur.SHOULD;
                        else
                            ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);
                    }

                    q.Add(firstQuery, occur);
                    firstQuery = null;
                }

                q.Add(t, occur);
            }

            if (firstQuery != null)
                return firstQuery;

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

                            yield return YieldValue(valueAsString, lastWordStart, i - lastWordStart, escapePositions);
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
                var distanceErrorPctValue = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
                AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

                distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
            }

            var spatialField = getSpatialField(fieldName);

            var methodName = shapeExpression.Name;
            var methodType = QueryMethod.GetMethodType(methodName.Value);

            IShape shape = null;
            switch (methodType)
            {
                case MethodType.Spatial_Circle:
                    shape = HandleCircle(query, shapeExpression, metadata, parameters, fieldName, spatialField);
                    break;
                case MethodType.Spatial_Wkt:
                    shape = HandleWkt(query, shapeExpression, metadata, parameters, fieldName, spatialField);
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

            var args = new SpatialArgs(operation, shape)
            {
                DistErrPct = distanceErrorPct
            };

            return spatialField.Strategy.MakeQuery(args);
        }

        private static IShape HandleWkt(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField)
        {
            var wktValue = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            AssertValueIsString(fieldName, wktValue.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 2)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[1] as ValueExpression, metadata, parameters, fieldName);

            return spatialField.ReadShape(GetValueAsString(wktValue.Value), spatialUnits);
        }

        private static IShape HandleCircle(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField)
        {
            var radius = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            AssertValueIsNumber(fieldName, radius.Type);

            var latitude = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
            AssertValueIsNumber(fieldName, latitude.Type);

            var longitude = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
            AssertValueIsNumber(fieldName, longitude.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 4)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[3] as ValueExpression, metadata, parameters, fieldName);

            return spatialField.ReadCircle(Convert.ToDouble(radius.Value), Convert.ToDouble(latitude.Value), Convert.ToDouble(longitude.Value), spatialUnits);
        }

        private static SpatialUnits? GetSpatialUnits(Query query, ValueExpression value, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var spatialUnitsValue = GetValue(query, metadata, parameters, value);
            AssertValueIsString(fieldName, spatialUnitsValue.Type);

            var spatialUnitsValueAsString = GetValueAsString(spatialUnitsValue.Value);
            if (Enum.TryParse(typeof(SpatialUnits), spatialUnitsValueAsString, true, out var su) == false)
                throw new InvalidOperationException(
                    $"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{spatialUnitsValueAsString}'.");

            return (SpatialUnits)su;
        }

        private static Lucene.Net.Search.Query HandleExact(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, index: null, parameters, analyzer, factories, exact: true);
        }

        public static IEnumerable<(object Value, ValueTokenType Type)> GetValues(Query query, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, ValueExpression value)
        {
            if (value.Value == ValueTokenType.Parameter)
            {
                var parameterName = value.Token.Value;

                if (parameters == null)
                    ThrowParametersWereNotProvided(metadata.QueryText);

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    ThrowParameterValueWasNotProvided(parameterName, metadata.QueryText, parameters);

                if (parameterValue is BlittableJsonReaderArray array)
                {
                    ValueTokenType? expectedValueType = null;
                    var unwrappedArray = UnwrapArray(array, metadata.QueryText, parameters);
                    foreach (var item in unwrappedArray)
                    {
                        if (expectedValueType == null)
                            expectedValueType = item.Type;
                        else
                        {
                            if (AreValueTokenTypesValid(expectedValueType.Value, item.Type) == false)
                                ThrowInvalidParameterType(expectedValueType.Value, item, metadata.QueryText, parameters);
                        }

                        yield return item;
                    }

                    yield break;
                }

                var parameterValueType = GetValueTokenType(parameterValue, metadata.QueryText, parameters);

                yield return (UnwrapParameter(parameterValue, parameterValueType), parameterValueType);
                yield break;
            }

            switch (value.Value)
            {
                case ValueTokenType.String:
                    yield return (value.Token.Value, ValueTokenType.String);
                    yield break;
                case ValueTokenType.Long:
                    var valueAsLong = ParseInt64WithSeparators(value.Token.Value);
                    yield return (valueAsLong, ValueTokenType.Long);
                    yield break;
                case ValueTokenType.Double:
                    var valueAsDouble = double.Parse(value.Token.Value, CultureInfo.InvariantCulture);
                    yield return (valueAsDouble, ValueTokenType.Double);
                    yield break;
                case ValueTokenType.True:
                    yield return (LuceneDocumentConverterBase.TrueString, ValueTokenType.String);
                    yield break;
                case ValueTokenType.False:
                    yield return (LuceneDocumentConverterBase.FalseString, ValueTokenType.String);
                    yield break;
                case ValueTokenType.Null:
                    yield return (null, ValueTokenType.String);
                    yield break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value.Type), value.Type, null);
            }
        }

        public static long ParseInt64WithSeparators(string token)
        {
            long l = 0;
            // this is known to be 0-9 with possibly _
            bool isNegative = token[0] == '-';

            for (var index = isNegative ? 1 : 0; index < token.Length; index++)
            {
                var ch = token[index];
                if (ch == '_')
                    continue;
                if (ch < '0' || ch > '9')
                    ThrowInvalidInt64(token);
                l = (l * 10) + (ch - '0');
            }

            return isNegative ? -l : l;
        }

        private static void ThrowInvalidInt64(string token)
        {
            throw new ArgumentException("Expected valid number, but got: " + token, nameof(token));
        }

        public static long GetLongValue(Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, QueryExpression expression, long nullValue)
        {
            var value = GetValue(query, metadata, parameters, expression);
            switch (value.Type)
            {
                case ValueTokenType.Long:
                    return (long)value.Value;
                case ValueTokenType.Null:
                    return nullValue;
                default:
                    ThrowValueTypeMismatch(value.Type, ValueTokenType.Long);
                    return -1;
            }
        }

        public static (object Value, ValueTokenType Type) GetValue(Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, QueryExpression expression, bool allowObjectsInParameters = false)
        {
            var value = expression as ValueExpression;
            if (value == null)
                throw new InvalidQueryException("Expected value, but got: " + expression, query.QueryText, parameters);

            if (value.Value == ValueTokenType.Parameter)
            {
                var parameterName = value.Token.Value;

                if (parameters == null)
                    ThrowParametersWereNotProvided(metadata.QueryText);

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    ThrowParameterValueWasNotProvided(parameterName, metadata.QueryText, parameters);

                if (allowObjectsInParameters && parameterValue is BlittableJsonReaderObject)
                    return (parameterValue, ValueTokenType.Parameter);

                var parameterValueType = GetValueTokenType(parameterValue, metadata.QueryText, parameters);

                return (UnwrapParameter(parameterValue, parameterValueType), parameterValueType);
            }

            switch (value.Value)
            {
                case ValueTokenType.String:
                    return (value.Token, ValueTokenType.String);
                case ValueTokenType.Long:
                    var valueAsLong = ParseInt64WithSeparators(value.Token.Value);
                    return (valueAsLong, ValueTokenType.Long);
                case ValueTokenType.Double:
                    var valueAsDouble = double.Parse(value.Token.Value, CultureInfo.InvariantCulture);
                    return (valueAsDouble, ValueTokenType.Double);
                case ValueTokenType.True:
                    return (LuceneDocumentConverterBase.TrueString, ValueTokenType.String);
                case ValueTokenType.False:
                    return (LuceneDocumentConverterBase.FalseString, ValueTokenType.String);
                case ValueTokenType.Null:
                    return (null, ValueTokenType.Null);
                default:
                    throw new ArgumentOutOfRangeException(nameof(value.Type), value.Type, null);
            }
        }

        private static (string LuceneFieldName, LuceneFieldType LuceneFieldType, LuceneTermType LuceneTermType) GetLuceneField(string fieldName, ValueTokenType valueType)
        {
            switch (valueType)
            {
                case ValueTokenType.String:
                    return (fieldName, LuceneFieldType.String, LuceneTermType.String);
                case ValueTokenType.Double:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble, LuceneFieldType.Double, LuceneTermType.Double);
                case ValueTokenType.Long:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong, LuceneFieldType.Long, LuceneTermType.Long);
                case ValueTokenType.True:
                case ValueTokenType.False:
                    return (fieldName, LuceneFieldType.String, LuceneTermType.String);
                case ValueTokenType.Null:
                case ValueTokenType.Parameter:
                    return (fieldName, LuceneFieldType.String, LuceneTermType.String);
                default:
                    ThrowUnhandledValueTokenType(valueType);
                    break;
            }

            Debug.Assert(false);

            return (null, LuceneFieldType.String, LuceneTermType.String);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static object UnwrapParameter(object parameterValue, ValueTokenType parameterType)
        {
            switch (parameterType)
            {
                case ValueTokenType.Long:
                    return parameterValue;
                case ValueTokenType.Double:
                    var dlnv = (LazyNumberValue)parameterValue;
                    return dlnv.ToDouble(CultureInfo.InvariantCulture);
                case ValueTokenType.String:
                    if (parameterValue == null)
                        return null;

                    var lsv = parameterValue as LazyStringValue;
                    if (lsv != null)
                        return lsv.ToString();

                    if (parameterValue is LazyCompressedStringValue lcsv)
                        return lcsv.ToString();

                    return parameterValue.ToString();
                case ValueTokenType.True:
                    return LuceneDocumentConverterBase.TrueString;
                case ValueTokenType.False:
                    return LuceneDocumentConverterBase.FalseString;
                case ValueTokenType.Null:
                    return null;
                case ValueTokenType.Parameter:
                    return parameterValue;
                default:
                    throw new ArgumentOutOfRangeException(nameof(parameterType), parameterType, null);
            }
        }

        private static IEnumerable<(object Value, ValueTokenType Type)> UnwrapArray(BlittableJsonReaderArray array, string queryText,
            BlittableJsonReaderObject parameters)
        {
            foreach (var item in array)
            {
                if (item is BlittableJsonReaderArray innerArray)
                {
                    foreach (var innerItem in UnwrapArray(innerArray, queryText, parameters))
                        yield return innerItem;

                    continue;
                }

                var parameterType = GetValueTokenType(item, queryText, parameters);
                yield return (UnwrapParameter(item, parameterType), parameterType);
            }
        }

        public static ValueTokenType GetValueTokenType(object parameterValue, string queryText, BlittableJsonReaderObject parameters, bool unwrapArrays = false)
        {
            if (parameterValue == null)
                return ValueTokenType.Null;

            if (parameterValue is LazyStringValue || parameterValue is LazyCompressedStringValue)
                return ValueTokenType.String;

            if (parameterValue is LazyNumberValue)
                return ValueTokenType.Double;

            if (parameterValue is long)
                return ValueTokenType.Long;

            if (parameterValue is bool b)
                return b ? ValueTokenType.True : ValueTokenType.False;

            if (unwrapArrays)
            {
                if (parameterValue is BlittableJsonReaderArray array)
                {
                    if (array.Length == 0)
                        return ValueTokenType.Null;

                    return GetValueTokenType(array[0], queryText, parameters, unwrapArrays: true);
                }
            }

            if (parameterValue is BlittableJsonReaderObject)
                return ValueTokenType.Parameter;

            ThrowUnexpectedParameterValue(parameterValue, queryText, parameters);

            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool AreValueTokenTypesValid(ValueTokenType previous, ValueTokenType current)
        {
            if (previous == ValueTokenType.Null)
                return true;

            if (current == ValueTokenType.Null)
                return true;

            return previous == current;
        }

        private static void AssertValueIsString(string fieldName, ValueTokenType fieldType)
        {
            if (fieldType != ValueTokenType.String)
                ThrowValueTypeMismatch(fieldName, fieldType, ValueTokenType.String);
        }

        private static void AssertValueIsNumber(string fieldName, ValueTokenType fieldType)
        {
            if (fieldType != ValueTokenType.Double && fieldType != ValueTokenType.Long)
                ThrowValueTypeMismatch(fieldName, fieldType, ValueTokenType.Double);
        }

        private static void ThrowUnhandledValueTokenType(ValueTokenType type)
        {
            throw new NotSupportedException($"Unhandled token type: {type}");
        }

        private static void ThrowInvalidOperatorInSearch(QueryMetadata metadata, BlittableJsonReaderObject parameters, FieldExpression fieldExpression)
        {
            throw new InvalidQueryException($"Supported operators in search() method are 'OR' or 'AND' but was '{fieldExpression.FieldValue}'", metadata.QueryText, parameters);
        }

        private static void ThrowInvalidParameterType(ValueTokenType expectedValueType, (object Value, ValueTokenType Type) item, string queryText,
            BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException("Expected query parameter to be " + expectedValueType + " but was " + item.Type + ": " + item.Value, queryText, parameters);
        }

        private static void ThrowMethodExpectsArgumentOfTheFollowingType(string methodName, ValueTokenType expectedType, ValueTokenType gotType, string queryText,
            BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Method {methodName}() expects to get an argument of type {expectedType} while it got {gotType}", queryText, parameters);
        }

        public static void ThrowParametersWereNotProvided(string queryText)
        {
            throw new InvalidQueryException("The query is parametrized but the actual values of parameters were not provided", queryText, null);
        }

        public static void ThrowParameterValueWasNotProvided(string parameterName, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Value of parameter '{parameterName}' was not provided", queryText, parameters);
        }

        private static void ThrowUnexpectedParameterValue(object parameter, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Parameter value '{parameter}' of type {parameter.GetType().FullName} is not supported", queryText, parameters);
        }

        private static void ThrowValueTypeMismatch(string fieldName, ValueTokenType fieldType, ValueTokenType expectedType)
        {
            throw new InvalidOperationException($"Field '{fieldName}' should be a '{expectedType}' but was '{fieldType}'.");
        }

        private static void ThrowValueTypeMismatch(ValueTokenType fieldType, ValueTokenType expectedType)
        {
            throw new InvalidOperationException($"Value should be a '{expectedType}' but was '{fieldType}'.");
        }
    }
}
