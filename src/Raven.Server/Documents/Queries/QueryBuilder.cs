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
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Utils;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Spatial4n.Core.Shapes;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.MoreLikeThisQuery;
using Query = Raven.Server.Documents.Queries.AST.Query;
using Version = Lucene.Net.Util.Version;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        private static readonly KeywordAnalyzer KeywordAnalyzer = new KeywordAnalyzer();

        public static Lucene.Net.Search.Query BuildQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression,
            IndexDefinitionBase indexDef, BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var luceneQuery = ToLuceneQuery(serverContext, context, metadata.Query, whereExpression, metadata, indexDef, parameters, analyzer, factories);

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        public static MoreLikeThisQuery BuildMoreLikeThisQuery(TransactionOperationContext serverContext, DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters, RavenPerFieldAnalyzerWrapper analyzer, QueryBuilderFactories factories)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var filterQuery = BuildQuery(serverContext, context, metadata, whereExpression, indexDef: null, parameters, analyzer, factories);
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
                return ToLuceneQuery(serverContext, context, query, binaryExpression, metadata, indexDef: null, parameters, analyzer, factories);

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
            QueryExpression expression, QueryMetadata metadata, IndexDefinitionBase indexDef,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact = false, int? proximity = null)
        {
            if (expression == null)
                return new MatchAllDocsQuery();

            if (expression is BinaryExpression where)
            {
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

                            exact = IsExact(indexDef, exact, fieldName);

                            var (value, valueType) = GetValue(query, metadata, parameters, right, true);

                            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueType);

                            switch (fieldType)
                            {
                                case LuceneFieldType.String:
                                    var valueAsString = GetValueAsString(value);

                                    if (exact && metadata.IsDynamic)
                                        luceneFieldName = AutoIndexField.GetExactAutoIndexFieldName(luceneFieldName);

                                    exact |= valueType == ValueTokenType.Parameter;

                                    switch (where.Operator)
                                    {
                                        case OperatorType.Equal:
                                            return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.NotEqual:
                                            return LuceneQueryHelper.NotEqual(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.LessThan:
                                            return LuceneQueryHelper.LessThan(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.GreaterThan:
                                            return LuceneQueryHelper.GreaterThan(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.LessThanEqual:
                                            return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, termType, valueAsString, exact);
                                        case OperatorType.GreaterThanEqual:
                                            return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, termType, valueAsString, exact);
                                    }
                                    break;
                                case LuceneFieldType.Long:
                                    var valueAsLong = (long)value;

                                    switch (where.Operator)
                                    {
                                        case OperatorType.Equal:
                                            return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsLong);
                                        case OperatorType.NotEqual:
                                            return LuceneQueryHelper.NotEqual(luceneFieldName, termType, valueAsLong);
                                        case OperatorType.LessThan:
                                            return LuceneQueryHelper.LessThan(luceneFieldName, termType, valueAsLong);
                                        case OperatorType.GreaterThan:
                                            return LuceneQueryHelper.GreaterThan(luceneFieldName, termType, valueAsLong);
                                        case OperatorType.LessThanEqual:
                                            return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, termType, valueAsLong);
                                        case OperatorType.GreaterThanEqual:
                                            return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, termType, valueAsLong);
                                    }
                                    break;
                                case LuceneFieldType.Double:
                                    var valueAsDouble = (double)value;

                                    switch (where.Operator)
                                    {
                                        case OperatorType.Equal:
                                            return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsDouble);
                                        case OperatorType.NotEqual:
                                            return LuceneQueryHelper.NotEqual(luceneFieldName, termType, valueAsDouble);
                                        case OperatorType.LessThan:
                                            return LuceneQueryHelper.LessThan(luceneFieldName, termType, valueAsDouble);
                                        case OperatorType.GreaterThan:
                                            return LuceneQueryHelper.GreaterThan(luceneFieldName, termType, valueAsDouble);
                                        case OperatorType.LessThanEqual:
                                            return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, termType, valueAsDouble);
                                        case OperatorType.GreaterThanEqual:
                                            return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, termType, valueAsDouble);
                                    }
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(fieldType), fieldType, null);
                            }

                            throw new NotSupportedException("Should not happen!");
                        }
                    case OperatorType.And:
                        return LuceneQueryHelper.And(
                            ToLuceneQuery(serverContext, documentsContext, query, where.Left, metadata, indexDef, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None,
                            ToLuceneQuery(serverContext, documentsContext, query, where.Right, metadata, indexDef, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None);
                    case OperatorType.Or:
                        return LuceneQueryHelper.Or(
                            ToLuceneQuery(serverContext, documentsContext, query, where.Left, metadata, indexDef, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None,
                            ToLuceneQuery(serverContext, documentsContext, query, where.Right, metadata, indexDef, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None);
                }
            }
            if (expression is NegatedExpression ne)
            {
                var inner = ToLuceneQuery(serverContext, documentsContext, query, ne.Expression,
                    metadata, indexDef, parameters, analyzer, factories, exact);
                return new BooleanQuery
                {
                    {inner,  Occur.MUST_NOT},
                    {new MatchAllDocsQuery(), Occur.SHOULD}
                };
            }
            if (expression is BetweenExpression be)
            {
                var fieldName = ExtractIndexFieldName(query, parameters, be.Source, metadata);
                var (valueFirst, valueFirstType) = GetValue(query, metadata, parameters, be.Min);
                var (valueSecond, _) = GetValue(query, metadata, parameters, be.Max);

                var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueFirstType);

                switch (fieldType)
                {
                    case LuceneFieldType.String:
                        var valueFirstAsString = GetValueAsString(valueFirst);
                        var valueSecondAsString = GetValueAsString(valueSecond);
                        return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsString, valueSecondAsString, exact);
                    case LuceneFieldType.Long:
                        var valueFirstAsLong = (long)valueFirst;
                        var valueSecondAsLong = (long)valueSecond;
                        return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsLong, valueSecondAsLong);
                    case LuceneFieldType.Double:
                        var valueFirstAsDouble = (double)valueFirst;
                        var valueSecondAsDouble = (double)valueSecond;
                        return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsDouble, valueSecondAsDouble);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(fieldType), fieldType, null);
                }
            }
            if (expression is InExpression ie)
            {
                var fieldName = ExtractIndexFieldName(query, parameters, ie.Source, metadata);
                LuceneTermType termType = LuceneTermType.Null;
                bool hasGotTheRealType = false;

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

                return new TermsMatchQuery(fieldName, matches);
            }
            if (expression is TrueExpression)
            {
                return new MatchAllDocsQuery();
            }
            if (expression is MethodExpression me)
            {
                var methodName = me.Name.Value;
                var methodType = QueryMethod.GetMethodType(methodName);

                switch (methodType)
                {
                    case MethodType.Search:
                        return HandleSearch(query, me, metadata, parameters, analyzer, proximity);
                    case MethodType.Fuzzy:
                        return HandleFuzzy(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Proximity:
                        return HandleProximity(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Boost:
                        return HandleBoost(serverContext, documentsContext, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Regex:
                        return HandleRegex(query, me, metadata, parameters, factories);
                    case MethodType.StartsWith:
                        return HandleStartsWith(query, me, metadata, indexDef, parameters, exact);
                    case MethodType.EndsWith:
                        return HandleEndsWith(query, me, metadata, indexDef, parameters, exact);
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

        private static bool IsExact(IndexDefinitionBase indexDefinition, bool exact, QueryFieldName fieldName)
        {
            if (exact)
                return true;

            if (indexDefinition?.IndexFields != null && indexDefinition.IndexFields.TryGetValue(fieldName, out var indexingOptions))
                return indexingOptions.Indexing == FieldIndexing.Exact;

            return false;
        }

        public static QueryExpression EvaluateMethod(Query query, QueryMetadata metadata, TransactionOperationContext serverContext, DocumentsOperationContext documentsContext, MethodExpression method, ref BlittableJsonReaderObject parameters)
        {
            var methodType = QueryMethod.GetMethodType(method.Name);

            var server = documentsContext.DocumentDatabase.ServerStore;
            switch (methodType)
            {
                case MethodType.CompareExchange:
                    var v = GetValue(query, metadata, parameters, method.Arguments[0]);
                    if (v.Type != ValueTokenType.String)
                        throw new InvalidQueryException("Expected value of type string, but got: " + v.Type, query.QueryText, parameters);

                    var prefix = CompareExchangeCommandBase.GetActualKey(documentsContext.DocumentDatabase.Name, v.Value.ToString());
                    object value = null;
                    server.Cluster.GetCompareExchangeValue(serverContext, prefix).Value?.TryGetMember("Object", out value);

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
                return metadata.GetIndexFieldName(new QueryFieldName(ve.Token, false), parameters);

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
                            return new QueryFieldName(f.Compound[0], f.IsQuoted);

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

        private static Lucene.Net.Search.Query HandleStartsWith(Query query, MethodExpression expression, QueryMetadata metadata, IndexDefinitionBase indexDefinition, BlittableJsonReaderObject parameters, bool exact)
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

            exact = IsExact(indexDefinition, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.Prefix, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleEndsWith(Query query, MethodExpression expression, QueryMetadata metadata, IndexDefinitionBase indexDefinition, BlittableJsonReaderObject parameters, bool exact)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = GetValueAsString(value);
            valueAsString = string.IsNullOrEmpty(valueAsString)
                ? LuceneQueryHelper.Asterisk
                : valueAsString.Insert(0, LuceneQueryHelper.Asterisk);

            exact = IsExact(indexDefinition, exact, fieldName);

            if (exact && metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetExactAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.WildCard, exact: exact);
        }

        private static Lucene.Net.Search.Query HandleProximity(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var proximity = int.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, indexDef: null, parameters, analyzer, factories, exact, proximity);
        }

        private static Lucene.Net.Search.Query HandleFuzzy(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var similarity = float.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, indexDef: null, parameters, analyzer, factories, exact);
            var tq = q as TermQuery;
            if (tq == null)
                throw new InvalidQueryException("Fuzzy only works on term queries", metadata.QueryText, parameters); // should not happen

            return new FuzzyQuery(tq.Term, similarity);
        }

        private static Lucene.Net.Search.Query HandleBoost(TransactionOperationContext serverContext, DocumentsOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var boost = float.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            var q = ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, indexDef: null, parameters, analyzer, factories, exact);
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
            if (expression.Arguments[0] is FieldExpression ft)
                fieldName = ExtractIndexFieldName(query, parameters, ft, metadata);
            else if (expression.Arguments[0] is ValueExpression vt)
                fieldName = ExtractIndexFieldName(vt, metadata, parameters);
            else
                throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);

            var (value, valueType) = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

            if (metadata.IsDynamic)
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

            var values = valueAsString.Split(' ');
            if (values.Length == 1)
            {
                var nValue = values[0];
                return LuceneQueryHelper.AnalyzedTerm(fieldName, nValue, GetTermType(nValue), analyzer);
            }

            var occur = Occur.SHOULD;
            if (expression.Arguments.Count == 3)
            {
                var fieldExpression = (FieldExpression)expression.Arguments[2];
                if (fieldExpression.Compound.Count != 1)
                    ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);

                var op = fieldExpression.Compound[0];
                if (string.Equals("AND", op, StringComparison.OrdinalIgnoreCase))
                    occur = Occur.MUST;
                else if (string.Equals("OR", op, StringComparison.OrdinalIgnoreCase))
                    occur = Occur.SHOULD;
                else
                    ThrowInvalidOperatorInSearch(metadata, parameters, fieldExpression);
            }

            var q = new BooleanQuery();
            foreach (var v in values)
                q.Add(LuceneQueryHelper.AnalyzedTerm(fieldName, v, GetTermType(v), analyzer), occur);

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
                fieldName = spatialExpression.GetText(null);
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
            var methodType = QueryMethod.GetMethodType(methodName);

            Shape shape = null;
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

        private static Shape HandleWkt(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField)
        {
            var wktValue = GetValue(query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            AssertValueIsString(fieldName, wktValue.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 2)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[1] as ValueExpression, metadata, parameters, fieldName);

            return spatialField.ReadShape(GetValueAsString(wktValue.Value), spatialUnits);
        }

        private static Shape HandleCircle(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
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
            return ToLuceneQuery(serverContext, context, query, expression.Arguments[0], metadata, indexDef: null, parameters, analyzer, factories, exact: true);
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
                    var valueAsLong = ParseInt64WithSeparators(value.Token);
                    return (valueAsLong, ValueTokenType.Long);
                case ValueTokenType.Double:
                    var valueAsDouble = double.Parse(value.Token, CultureInfo.InvariantCulture);
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
