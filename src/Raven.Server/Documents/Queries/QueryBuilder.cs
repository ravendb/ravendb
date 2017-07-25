using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Client;
using Raven.Server.Utils;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Sparrow.Json;
using Query = Raven.Server.Documents.Queries.Parser.Query;
using Version = Lucene.Net.Util.Version;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        public static Lucene.Net.Search.Query BuildQuery(QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var luceneQuery = ToLuceneQuery(metadata.Query, metadata.Query.Where, metadata, parameters, analyzer);

                // The parser already throws parse exception if there is a syntax error.
                // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                return luceneQuery ?? new BooleanQuery();
            }
        }

        private static Lucene.Net.Search.Query ToLuceneQuery(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            if (expression == null)
                return new MatchAllDocsQuery();

            switch (expression.Type)
            {
                case OperatorType.Equal:
                case OperatorType.GreaterThan:
                case OperatorType.LessThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    {
                        var fieldName = ExtractIndexFieldName(query.QueryText, expression.Field, metadata);
                        var (value, valueType) = GetValue(fieldName, query, metadata, parameters, expression.Value);

                        var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueType);

                        switch (fieldType)
                        {
                            case LuceneFieldType.String:
                                var valueAsString = value as string;

                                switch (expression.Type)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsString);
                                    case OperatorType.LessThan:
                                        return LuceneQueryHelper.LessThan(luceneFieldName, termType, valueAsString);
                                    case OperatorType.GreaterThan:
                                        return LuceneQueryHelper.GreaterThan(luceneFieldName, termType, valueAsString);
                                    case OperatorType.LessThanEqual:
                                        return LuceneQueryHelper.LessThanOrEqual(luceneFieldName, termType, valueAsString);
                                    case OperatorType.GreaterThanEqual:
                                        return LuceneQueryHelper.GreaterThanOrEqual(luceneFieldName, termType, valueAsString);
                                }
                                break;
                            case LuceneFieldType.Long:
                                var valueAsLong = (long)value;

                                switch (expression.Type)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsLong);
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

                                switch (expression.Type)
                                {
                                    case OperatorType.Equal:
                                        return LuceneQueryHelper.Equal(luceneFieldName, termType, valueAsDouble);
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
                                throw new ArgumentOutOfRangeException();
                        }

                        throw new NotSupportedException("Should not happen!");
                    }
                case OperatorType.Between:
                    {
                        var fieldName = ExtractIndexFieldName(query.QueryText, expression.Field, metadata);
                        var (valueFirst, valueFirstType) = GetValue(fieldName, query, metadata, parameters, expression.First);
                        var (valueSecond, _) = GetValue(fieldName, query, metadata, parameters, expression.Second);

                        var (luceneFieldName, fieldType, termType) = GetLuceneField(fieldName, valueFirstType);

                        switch (fieldType)
                        {
                            case LuceneFieldType.String:
                                var valueFirstAsString = valueFirst as string;
                                var valueSecondAsString = valueSecond as string;
                                return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsString, valueSecondAsString);
                            case LuceneFieldType.Long:
                                var valueFirstAsLong = (long)valueFirst;
                                var valueSecondAsLong = (long)valueSecond;
                                return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsLong, valueSecondAsLong);
                            case LuceneFieldType.Double:
                                var valueFirstAsDouble = (double)valueFirst;
                                var valueSecondAsDouble = (double)valueSecond;
                                return LuceneQueryHelper.Between(luceneFieldName, termType, valueFirstAsDouble, valueSecondAsDouble);
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                case OperatorType.In:
                    {
                        var fieldName = ExtractIndexFieldName(query.QueryText, expression.Field, metadata);
                        var (luceneFieldName, _, termType) = GetLuceneField(fieldName, metadata.WhereFields[fieldName]);

                        var matches = new List<string>(expression.Values.Count);
                        foreach (var valueToken in expression.Values)
                        {
                            foreach (var (value, _) in GetValues(fieldName, query, metadata, parameters, valueToken))
                                matches.Add(LuceneQueryHelper.GetTermValue(value, termType));
                        }

                        return new TermsMatchQuery(luceneFieldName, matches);
                    }
                case OperatorType.And:
                case OperatorType.AndNot:
                    var andPrefix = expression.Type == OperatorType.AndNot ? LucenePrefixOperator.Minus : LucenePrefixOperator.None;
                    return LuceneQueryHelper.And(
                        ToLuceneQuery(query, expression.Left, metadata, parameters, analyzer),
                        LucenePrefixOperator.None,
                        ToLuceneQuery(query, expression.Right, metadata, parameters, analyzer),
                        andPrefix);
                case OperatorType.Or:
                case OperatorType.OrNot:
                    var orPrefix = expression.Type == OperatorType.OrNot ? LucenePrefixOperator.Minus : LucenePrefixOperator.None;
                    return LuceneQueryHelper.Or(
                        ToLuceneQuery(query, expression.Left, metadata, parameters, analyzer),
                        LucenePrefixOperator.None,
                        ToLuceneQuery(query, expression.Right, metadata, parameters, analyzer),
                        orPrefix);
                case OperatorType.Method:
                    var methodName = QueryExpression.Extract(query.QueryText, expression.Field);
                    var methodType = GetMethodType(methodName);

                    switch (methodType)
                    {
                        case MethodType.Search:
                            return HandleSearch(query, expression, metadata, parameters, analyzer);
                        case MethodType.Boost:
                            return HandleBoost(query, expression, metadata, parameters, analyzer);
                        case MethodType.StartsWith:
                            return HandleStartsWith(query, expression, metadata, parameters);
                        case MethodType.EndsWith:
                            return HandleEndsWith(query, expression, metadata, parameters);
                        case MethodType.Lucene:
                            return HandleLucene(query, expression, metadata, parameters, analyzer);
                        case MethodType.Exists:
                            return HandleExists(query, expression, metadata);
                        default:
                            ThrowMethodNotSupported(methodType);
                            break;
                    }

                    break;
                default:
                    ThrowUnhandledExpressionOperatorType(expression.Type);
                    break;
            }

            Debug.Assert(false, "should never happen");

            return null;
        }

        private static string ExtractIndexFieldName(string queryText, FieldToken field, QueryMetadata metadata)
        {
            return metadata.GetIndexFieldName(QueryExpression.Extract(queryText, field));
        }

        private static Lucene.Net.Search.Query HandleExists(Query query, QueryExpression expression, QueryMetadata metadata)
        {
            var fieldName = ExtractIndexFieldName(query.QueryText, (FieldToken)expression.Arguments[0], metadata);

            return LuceneQueryHelper.Term(fieldName, LuceneQueryHelper.Asterisk, LuceneTermType.WildCard);
        }

        private static Lucene.Net.Search.Query HandleLucene(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            var fieldName = ExtractIndexFieldName(query.QueryText, (FieldToken)expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                throw new InvalidOperationException();

            var parser = new Lucene.Net.QueryParsers.QueryParser(Version.LUCENE_29, fieldName, analyzer);
            return parser.Parse(value as string);
        }

        private static Lucene.Net.Search.Query HandleStartsWith(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            var fieldName = ExtractIndexFieldName(query.QueryText, (FieldToken)expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                throw new InvalidOperationException();

            var valueAsString = value as string;
            if (string.IsNullOrEmpty(valueAsString))
                valueAsString = LuceneQueryHelper.Asterisk;
            else
                valueAsString += LuceneQueryHelper.Asterisk;

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.Prefix);
        }

        private static Lucene.Net.Search.Query HandleEndsWith(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters)
        {
            var fieldName = ExtractIndexFieldName(query.QueryText, (FieldToken)expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                throw new InvalidOperationException();

            var valueAsString = value as string;
            valueAsString = string.IsNullOrEmpty(valueAsString)
                ? LuceneQueryHelper.Asterisk
                : valueAsString.Insert(0, LuceneQueryHelper.Asterisk);

            return LuceneQueryHelper.Term(fieldName, valueAsString, LuceneTermType.WildCard);
        }

        private static Lucene.Net.Search.Query HandleBoost(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            var boost = float.Parse(QueryExpression.Extract(query.QueryText, (ValueToken)expression.Arguments[1]));
            expression = (QueryExpression)expression.Arguments[0];

            var q = ToLuceneQuery(query, expression, metadata, parameters, analyzer);
            q.Boost = boost;

            return q;
        }

        private static Lucene.Net.Search.Query HandleSearch(Query query, QueryExpression expression, QueryMetadata metadata, BlittableJsonReaderObject parameters, Analyzer analyzer)
        {
            var fieldName = ExtractIndexFieldName(query.QueryText, (FieldToken)expression.Arguments[0], metadata);
            var (value, valueType) = GetValue(fieldName, query, metadata, parameters, (ValueToken)expression.Arguments[1]);

            if (valueType != ValueTokenType.String)
                throw new InvalidOperationException();

            var valueAsString = (string)value;
            var values = valueAsString.Split(' ');

            if (values.Length == 1)
            {
                var nValue = values[0];
                return LuceneQueryHelper.AnalyzedTerm(fieldName, nValue, GetTermType(nValue), analyzer);
            }

            var q = new BooleanQuery();
            foreach (var v in values)
                q.Add(LuceneQueryHelper.AnalyzedTerm(fieldName, v, GetTermType(v), analyzer), Occur.SHOULD);

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

        public static IEnumerable<(string Value, ValueTokenType Type)> GetValues(string fieldName, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, ValueToken value)
        {
            if (value.Type == ValueTokenType.Parameter)
            {
                var parameterName = QueryExpression.Extract(query.QueryText, value);

                var expectedValueType = metadata.WhereFields[fieldName];

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                var array = parameterValue as BlittableJsonReaderArray;
                if (array != null)
                {
                    foreach (var item in UnwrapArray(array))
                    {
                        if (expectedValueType != item.Type)
                            ThrowInvalidParameterType(expectedValueType, item);

                        yield return item;
                    }

                    yield break;
                }

                var parameterValueType = GetValueTokenType(parameterValue);
                if (expectedValueType != parameterValueType)
                    ThrowInvalidParameterType(expectedValueType, parameterValue, parameterValueType);

                yield return (parameterValue.ToString(), parameterValueType);
            }

            yield return (QueryExpression.Extract(query.QueryText, value.TokenStart + 1, value.TokenLength - 2, value.EscapeChars), value.Type);
        }

        public static (object Value, ValueTokenType Type) GetValue(string fieldName, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, ValueToken value)
        {
            if (value.Type == ValueTokenType.Parameter)
            {
                var parameterName = QueryExpression.Extract(query.QueryText, value);

                var expectedValueType = metadata.WhereFields[fieldName];

                if (parameters == null)
                    throw new InvalidOperationException();

                if (parameters.TryGetMember(parameterName, out var parameterValue) == false)
                    throw new InvalidOperationException();

                var parameterValueType = GetValueTokenType(parameterValue);

                if (expectedValueType != parameterValueType)
                    throw new InvalidOperationException();

                return (UnwrapParameter(parameterValue, parameterValueType), parameterValueType);
            }

            switch (value.Type)
            {
                case ValueTokenType.String:
                    var valueAsString = QueryExpression.Extract(query.QueryText, value.TokenStart + 1, value.TokenLength - 2, value.EscapeChars);
                    return (valueAsString, ValueTokenType.String);
                case ValueTokenType.Long:
                    var valueAsLong = long.Parse(QueryExpression.Extract(query.QueryText, value));
                    return (valueAsLong, ValueTokenType.Long);
                case ValueTokenType.Double:
                    var valueAsDouble = double.Parse(QueryExpression.Extract(query.QueryText, value));
                    return (valueAsDouble, ValueTokenType.Double);
                case ValueTokenType.True:
                    return (LuceneDocumentConverterBase.TrueString, ValueTokenType.String);
                case ValueTokenType.False:
                    return (LuceneDocumentConverterBase.FalseString, ValueTokenType.String);
                case ValueTokenType.Null:
                    return (Constants.Documents.Indexing.Fields.NullValue, ValueTokenType.String);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static (string LuceneFieldName, LuceneFieldType LuceneFieldType, LuceneTermType LuceneTermType) GetLuceneField(string fieldName, ValueTokenType valueType)
        {
            switch (valueType)
            {
                case ValueTokenType.String:
                    return (fieldName, LuceneFieldType.String, LuceneTermType.String);
                case ValueTokenType.Double:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble, LuceneFieldType.Double, LuceneTermType.Double); // TODO arek - avoid +
                case ValueTokenType.Long:
                    return (fieldName + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong, LuceneFieldType.Long, LuceneTermType.Long); // TODO arek - avoid +
                case ValueTokenType.True:
                case ValueTokenType.False:
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
                    return Constants.Documents.Indexing.Fields.NullValue;
                default:
                    throw new ArgumentOutOfRangeException(nameof(parameterType), parameterType, null);
            }
        }

        private static IEnumerable<(string Value, ValueTokenType Type)> UnwrapArray(BlittableJsonReaderArray array)
        {
            foreach (var item in array)
            {
                var innerArray = item as BlittableJsonReaderArray;
                if (innerArray != null)
                {
                    foreach (var innerItem in UnwrapArray(innerArray))
                        yield return innerItem;

                    continue;
                }

                yield return (item.ToString(), GetValueTokenType(item));
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

        public static ValueTokenType GetValueTokenType(object parameterValue, bool unwrapArrays = false)
        {
            if (parameterValue == null)
                return ValueTokenType.String;

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
                    if (array.Length == 0) // TODO [ppekrol]
                        throw new InvalidOperationException();

                    return GetValueTokenType(array[0], unwrapArrays: true);
                }
            }

            throw new NotImplementedException();
        }

        private static MethodType GetMethodType(string methodName)
        {
            if (string.Equals(methodName, "search", StringComparison.OrdinalIgnoreCase))
                return MethodType.Search;

            if (string.Equals(methodName, "boost", StringComparison.OrdinalIgnoreCase))
                return MethodType.Boost;

            if (string.Equals(methodName, "startsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.StartsWith;

            if (string.Equals(methodName, "endsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.EndsWith;

            if (string.Equals(methodName, "lucene", StringComparison.OrdinalIgnoreCase))
                return MethodType.Lucene;

            if (string.Equals(methodName, "exists", StringComparison.OrdinalIgnoreCase))
                return MethodType.Exists;

            throw new NotSupportedException($"Method '{methodName}' is not supported.");
        }

        private static void ThrowUnhandledValueTokenType(ValueTokenType type)
        {
            throw new NotSupportedException($"Unhandled token type: {type}");
        }

        private static void ThrowInvalidParameterType(ValueTokenType expectedValueType, object parameterValue, ValueTokenType parameterValueType)
        {
            throw new InvalidOperationException("Expected parameter to be " + expectedValueType + " but was " + parameterValueType + ": " + parameterValue);
        }

        private static void ThrowInvalidParameterType(ValueTokenType expectedValueType, (string Value, ValueTokenType Type) item)
        {
            throw new InvalidOperationException("Expected query parameter to be " + expectedValueType + " but was " + item.Type + ": " + item.Value);
        }

        private static void ThrowMethodNotSupported(MethodType methodType)
        {
            throw new NotSupportedException($"Method '{methodType}' is not supported.");
        }

        private static void ThrowUnhandledExpressionOperatorType(OperatorType type)
        {
            throw new ArgumentOutOfRangeException($"Unhandled expression operator type: {type}");
        }

        private enum MethodType
        {
            Search,
            Boost,
            StartsWith,
            EndsWith,
            Lucene,
            Exists
        }
    }
}