using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Queries;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Utils;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Sparrow.Json;
using Spatial4n.Core.Shapes;
using Query = Raven.Server.Documents.Queries.AST.Query;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        public static Lucene.Net.Search.Query BuildQuery(JsonOperationContext context, QueryMetadata metadata, QueryExpression whereExpression,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var luceneQuery = ToLuceneQuery(context, metadata.Query, whereExpression, metadata, parameters, analyzer, factories);

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        private static Lucene.Net.Search.Query ToLuceneQuery(JsonOperationContext context, Query query, QueryExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact = false)
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
                            var fieldName = ExtractIndexFieldName(query, parameters, where.Left, metadata);
                            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, where.Right);

                            var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueType);

                            switch (fieldType)
                            {
                                case LuceneFieldType.String:
                                    var valueAsString = GetValueAsString(value);


                                    if (exact && metadata.IsDynamic)
                                        luceneFieldName = AutoIndexField.GetExactAutoIndexFieldName(luceneFieldName);

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
                    case OperatorType.AndNot:
                        var andPrefix = where.Operator == OperatorType.AndNot ? LucenePrefixOperator.Minus : LucenePrefixOperator.None;
                        return LuceneQueryHelper.And(
                            ToLuceneQuery(context, query, where.Left, metadata, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None,
                            ToLuceneQuery(context, query, where.Right, metadata, parameters, analyzer, factories, exact),
                            andPrefix);
                    case OperatorType.Or:
                    case OperatorType.OrNot:
                        var orPrefix = where.Operator == OperatorType.OrNot ? LucenePrefixOperator.Minus : LucenePrefixOperator.None;
                        return LuceneQueryHelper.Or(
                            ToLuceneQuery(context, query, where.Left, metadata, parameters, analyzer, factories, exact),
                            LucenePrefixOperator.None,
                            ToLuceneQuery(context, query, where.Right, metadata, parameters, analyzer, factories, exact),
                            orPrefix);
                }
            }
            if (expression is BetweenExpression be)
            {
                var fieldName = ExtractIndexFieldName(query, parameters, be.Source, metadata);
                var (valueFirst, valueFirstType) = GetValue(fieldName, query, metadata, parameters, be.Min);
                var (valueSecond, _) = GetValue(fieldName, query, metadata, parameters, be.Max);

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
                    foreach (var value in GetValuesForIn(context, query, ie, metadata, parameters, fieldName))
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
                foreach (var tuple in GetValuesForIn(context, query, ie, metadata, parameters, fieldName))
                {
                    if (hasGotTheRealType == false)
                    {
                        // we assume that the type of all the parameters is the same
                        termType = GetLuceneField(fieldName, tuple.Type).LuceneTermType;
                        hasGotTheRealType = true;
                    }
                    matches.Add(LuceneQueryHelper.GetTermValue(tuple.Value, termType, exact));
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
                        return HandleSearch(query, me, metadata, parameters, analyzer);
                    case MethodType.Boost:
                        return HandleBoost(context, query, me, metadata, parameters, analyzer, factories, exact);
                    case MethodType.Regex:
                        return HandleRegex(query, me, metadata, parameters, factories);
                    case MethodType.StartsWith:
                        return HandleStartsWith(query, me, metadata, parameters);
                    case MethodType.EndsWith:
                        return HandleEndsWith(query, me, metadata, parameters);
                    case MethodType.Lucene:
                        return HandleLucene(query, me, metadata, parameters, analyzer);
                    case MethodType.Exists:
                        return HandleExists(query, parameters, me, metadata);
                    case MethodType.Exact:
                        return HandleExact(context, query, me, metadata, parameters, analyzer, factories);
                    case MethodType.Within:
                    case MethodType.Contains:
                    case MethodType.Disjoint:
                    case MethodType.Intersects:
                        return HandleSpatial(query, me, metadata, parameters, methodType, factories.GetSpatialFieldFactory);
                    default:
                        QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, parameters);
                        return null; // never hit
                }
            }

            throw new InvalidQueryException("Unable to understand query", query.QueryText, parameters);
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
            JsonOperationContext context,
            Query query,
            InExpression expression,
            QueryMetadata metadata,
            BlittableJsonReaderObject parameters,
            string fieldName)
        {
            foreach (var val in expression.Values)
            {
                var valueToken = val as ValueExpression;
                if (valueToken == null)
                    ThrowInvalidInValue(query, parameters, val);

                foreach (var (value, type) in GetValues(fieldName, query, metadata, parameters, valueToken))
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
            Analyzer analyzer)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("lucene", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            if (metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

            var parser = new Lucene.Net.QueryParsers.QueryParser(Version.LUCENE_29, fieldName, analyzer);
            return parser.Parse(GetValueAsString(value));
        }

        private static Lucene.Net.Search.Query HandleStartsWith(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("startsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = GetValueAsString(value);
            if (string.IsNullOrEmpty(valueAsString))
                valueAsString = LuceneQueryHelper.Asterisk;
            else
                valueAsString += LuceneQueryHelper.Asterisk;

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.Prefix);
        }

        private static Lucene.Net.Search.Query HandleEndsWith(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            var fieldName = ExtractIndexFieldName(query, parameters, expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("endsWith", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            var valueAsString = GetValueAsString(value);
            valueAsString = string.IsNullOrEmpty(valueAsString)
                ? LuceneQueryHelper.Asterisk
                : valueAsString.Insert(0, LuceneQueryHelper.Asterisk);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.WildCard);
        }

        private static Lucene.Net.Search.Query HandleBoost(JsonOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories, bool exact)
        {
            var boost = float.Parse(((ValueExpression)expression.Arguments[1]).Token.Value);

            var q = ToLuceneQuery(context, query, expression.Arguments[0], metadata, parameters, analyzer, factories, exact);
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
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
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
            Analyzer analyzer)
        {
            QueryFieldName fieldName;
            if (expression.Arguments[0] is FieldExpression ft)
                fieldName = ExtractIndexFieldName(query, parameters, ft, metadata);
            else if (expression.Arguments[0] is ValueExpression vt)
                fieldName = ExtractIndexFieldName(vt, metadata, parameters);
            else
                throw new InvalidOperationException("search() method can only be called with an identifier or string, but was called with " + expression.Arguments[0]);

            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                ThrowMethodExpectsArgumentOfTheFollowingType("search", ValueTokenType.String, valueType, metadata.QueryText, parameters);

            Debug.Assert(metadata.IsDynamic == false || metadata.WhereFields[fieldName].IsFullTextSearch);

            var valueAsString = GetValueAsString(value);
            var values = valueAsString.Split(' ');

            if (metadata.IsDynamic)
                fieldName = new QueryFieldName(AutoIndexField.GetSearchAutoIndexFieldName(fieldName.Value), fieldName.IsQuoted);

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
                fieldName = spatialExpression.GetText();
            }

            var shapeExpression = (MethodExpression)expression.Arguments[1];

            var distanceErrorPct = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
            if (expression.Arguments.Count == 3)
            {
                var distanceErrorPctValue = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
                AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

                distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
            }

            var spatialField = getSpatialField(fieldName);

            var methodName = shapeExpression.Name;
            var methodType = QueryMethod.GetMethodType(methodName);

            Shape shape = null;
            switch (methodType)
            {
                case MethodType.Circle:
                    shape = HandleCircle(query, shapeExpression, metadata, parameters, fieldName, spatialField);
                    break;
                case MethodType.Wkt:
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
                case MethodType.Within:
                    operation = SpatialOperation.IsWithin;
                    break;
                case MethodType.Contains:
                    operation = SpatialOperation.Contains;
                    break;
                case MethodType.Disjoint:
                    operation = SpatialOperation.IsDisjointTo;
                    break;
                case MethodType.Intersects:
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
            var wktValue = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            AssertValueIsString(fieldName, wktValue.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 2)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[3] as ValueExpression, metadata, parameters, fieldName);

            return spatialField.ReadShape((string)wktValue.Value, spatialUnits);
        }

        private static Shape HandleCircle(Query query, MethodExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName,
            SpatialField spatialField)
        {
            var radius = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[0]);
            AssertValueIsNumber(fieldName, radius.Type);

            var latitute = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[1]);
            AssertValueIsNumber(fieldName, latitute.Type);

            var longitude = GetValue(fieldName, query, metadata, parameters, (ValueExpression)expression.Arguments[2]);
            AssertValueIsNumber(fieldName, longitude.Type);

            SpatialUnits? spatialUnits = null;
            if (expression.Arguments.Count == 4)
                spatialUnits = GetSpatialUnits(query, expression.Arguments[3] as ValueExpression, metadata, parameters, fieldName);

            return spatialField.ReadCircle(Convert.ToDouble(radius.Value), Convert.ToDouble(latitute.Value), Convert.ToDouble(longitude.Value), spatialUnits);
        }

        private static SpatialUnits? GetSpatialUnits(Query query, ValueExpression value, QueryMetadata metadata, BlittableJsonReaderObject parameters, string fieldName)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var spatialUnitsValue = GetValue(fieldName, query, metadata, parameters, value);
            AssertValueIsString(fieldName, spatialUnitsValue.Type);

            var spatialUnitsValueAsString = spatialUnitsValue.Value.ToString();
            if (Enum.TryParse(typeof(SpatialUnits), spatialUnitsValueAsString, true, out var su) == false)
                throw new InvalidOperationException(
                    $"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{spatialUnitsValueAsString}'.");

            return (SpatialUnits)su;
        }

        private static Lucene.Net.Search.Query HandleExact(JsonOperationContext context, Query query, MethodExpression expression, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, Analyzer analyzer, QueryBuilderFactories factories)
        {
            return ToLuceneQuery(context, query, expression.Arguments[0], metadata, parameters, analyzer, factories, exact: true);
        }

        public static IEnumerable<(object Value, ValueTokenType Type)> GetValues(string fieldName, Query query, QueryMetadata metadata,
            BlittableJsonReaderObject parameters, ValueExpression value)
        {
            if (value.Value == ValueTokenType.Parameter)
            {
                var parameterName = value.Token.Value;

                if (parameters == null)
                    ThrowParametersWereNotProvided(metadata.QueryText);

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    ThrowParameterValueWasNotProvided(parameterName, metadata.QueryText, parameters);

                var array = parameterValue as BlittableJsonReaderArray;
                if (array != null)
                {
                    ValueTokenType? expectedValueType = null;
                    foreach (var item in UnwrapArray(array, metadata.QueryText, parameters))
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
                    var valueAsLong = long.Parse(value.Token.Value);
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

        public static (object Value, ValueTokenType Type) GetValue(string fieldName, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters,
            QueryExpression expr)
        {
            var value = expr as ValueExpression;
            if (value == null)
                throw new InvalidQueryException("Expected value, but got: " + expr, query.QueryText, parameters);

            if (value.Value == ValueTokenType.Parameter)
            {
                var parameterName = value.Token.Value;

                if (parameters == null)
                    ThrowParametersWereNotProvided(metadata.QueryText);

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    ThrowParameterValueWasNotProvided(parameterName, metadata.QueryText, parameters);

                var parameterValueType = GetValueTokenType(parameterValue, metadata.QueryText, parameters);

                return (UnwrapParameter(parameterValue, parameterValueType), parameterValueType);
            }

            switch (value.Value)
            {
                case ValueTokenType.String:
                    return (value.Token, ValueTokenType.String);
                case ValueTokenType.Long:
                    var valueAsLong = long.Parse(value.Token);
                    return (valueAsLong, ValueTokenType.Long);
                case ValueTokenType.Double:
                    var valueAsDouble = double.Parse(value.Token, CultureInfo.InvariantCulture);
                    return (valueAsDouble, ValueTokenType.Double);
                case ValueTokenType.True:
                    return (LuceneDocumentConverterBase.TrueString, ValueTokenType.String);
                case ValueTokenType.False:
                    return (LuceneDocumentConverterBase.FalseString, ValueTokenType.String);
                case ValueTokenType.Null:
                    return (null, ValueTokenType.String);
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

                    var lcsv = parameterValue as LazyCompressedStringValue;
                    if (lcsv != null)
                        return lcsv.ToString();

                    return parameterValue.ToString();
                case ValueTokenType.True:
                    return LuceneDocumentConverterBase.TrueString;
                case ValueTokenType.False:
                    return LuceneDocumentConverterBase.FalseString;
                case ValueTokenType.Null:
                    return null;
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

        public static string Unescape(string term)
        {
            // method doesn't allocate a StringBuilder unless the string requires unescaping
            // also this copies chunks of the original string into the StringBuilder which
            // is far more efficient than copying character by character because StringBuilder
            // can access the underlying string data directly

            if (string.IsNullOrEmpty(term))
            {
                return term;
            }

            var isPhrase = term.StartsWith("\"") && term.EndsWith("\"");
            var start = 0;
            var length = term.Length;
            StringBuilder buffer = null;
            var prev = '\0';
            for (var i = start; i < length; i++)
            {
                var ch = term[i];
                if (prev != '\\')
                {
                    prev = ch;
                    continue;
                }
                prev = '\0'; // reset
                switch (ch)
                {
                    case '*':
                    case '?':
                    case '+':
                    case '-':
                    case '&':
                    case '|':
                    case '!':
                    case '(':
                    case ')':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '^':
                    case '"':
                    case '~':
                    case ':':
                    case '\\':
                        {
                            if (buffer == null)
                            {
                                // allocate builder with headroom
                                buffer = new StringBuilder(length * 2);
                            }
                            // append any leading substring
                            buffer.Append(term, start, i - start - 1);
                            buffer.Append(ch);
                            start = i + 1;
                            break;
                        }
                }
            }

            if (buffer == null)
            {
                if (isPhrase)
                    return term.Substring(1, term.Length - 2);
                // no changes required
                return term;
            }

            if (length > start)
            {
                // append any trailing substring
                buffer.Append(term, start, length - start);
            }

            return buffer.ToString();
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

            if (parameterValue is bool)
                return (bool)parameterValue ? ValueTokenType.True : ValueTokenType.False;

            if (unwrapArrays)
            {
                var array = parameterValue as BlittableJsonReaderArray;
                if (array != null)
                {
                    if (array.Length == 0)
                        return ValueTokenType.Null;

                    return GetValueTokenType(array[0], queryText, parameters, unwrapArrays: true);
                }
            }

            ThrowUnexpectedParameterValue(parameterValue, queryText, parameters);

            return default(ValueTokenType);
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
            throw new InvalidQueryException("The query is parametrized but the actual values of parameters were not provided", queryText, (BlittableJsonReaderObject)null);
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
    }
}
