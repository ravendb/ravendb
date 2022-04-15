using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Constants = Raven.Client.Constants;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Queries;

public static class QueryBuilderHelper
{
    internal const int ScoreId = -1;

    internal static IEnumerable<(object Value, ValueTokenType Type)> GetValues(Query query, QueryMetadata metadata,
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

    internal static void ThrowInvalidInt64(string token)
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

    public static (object Value, ValueTokenType Type) GetValue(Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, QueryExpression expression,
        bool allowObjectsInParameters = false)
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

    internal static (string IndexFieldName, IndexFieldType LuceneFieldType, LuceneTermType LuceneTermType) GetCoraxField(string fieldName,
        ValueTokenType valueType)
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
                ThrowUnhandledValueTokenType(valueType);
                break;
        }

        Debug.Assert(false);

        return (null, IndexFieldType.String, LuceneTermType.String);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static object UnwrapParameter(object parameterValue, ValueTokenType parameterType)
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

    internal static IEnumerable<(object Value, ValueTokenType Type)> UnwrapArray(BlittableJsonReaderArray array, string queryText,
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

    internal static void AssertValueIsString(string fieldName, ValueTokenType fieldType)
    {
        if (fieldType != ValueTokenType.String)
            ThrowValueTypeMismatch(fieldName, fieldType, ValueTokenType.String);
    }

    internal static void AssertValueIsNumber(string fieldName, ValueTokenType fieldType)
    {
        if (fieldType != ValueTokenType.Double && fieldType != ValueTokenType.Long)
            ThrowValueTypeMismatch(fieldName, fieldType, ValueTokenType.Double);
    }

    internal static void ThrowQueryTooComplexException(QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Query is too complex", metadata.QueryText, parameters);
    }

    internal static void ThrowUnhandledValueTokenType(ValueTokenType type)
    {
        throw new NotSupportedException($"Unhandled token type: {type}");
    }

    internal static void ThrowInvalidOperatorInSearch(QueryMetadata metadata, BlittableJsonReaderObject parameters, FieldExpression fieldExpression)
    {
        throw new InvalidQueryException($"Supported operators in search() method are 'OR' or 'AND' but was '{fieldExpression.FieldValue}'", metadata.QueryText,
            parameters);
    }

    internal static void ThrowInvalidParameterType(ValueTokenType expectedValueType, (object Value, ValueTokenType Type) item, string queryText,
        BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException("Expected query parameter to be " + expectedValueType + " but was " + item.Type + ": " + item.Value, queryText, parameters);
    }

    internal static void ThrowMethodExpectsArgumentOfTheFollowingType(string methodName, ValueTokenType expectedType, ValueTokenType gotType, string queryText,
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

    internal static void ThrowUnexpectedParameterValue(object parameter, string queryText, BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Parameter value '{parameter}' of type {parameter.GetType().FullName} is not supported", queryText, parameters);
    }

    internal static void ThrowValueTypeMismatch(string fieldName, ValueTokenType fieldType, ValueTokenType expectedType)
    {
        throw new InvalidOperationException($"Field '{fieldName}' should be a '{expectedType}' but was '{fieldType}'.");
    }

    internal static void ThrowValueTypeMismatch(ValueTokenType fieldType, ValueTokenType expectedType)
    {
        throw new InvalidOperationException($"Value should be a '{expectedType}' but was '{fieldType}'.");
    }

    internal static UnaryMatchOperation TranslateUnaryMatchOperation(OperatorType current) => current switch
    {
        OperatorType.Equal => UnaryMatchOperation.Equals,
        OperatorType.NotEqual => UnaryMatchOperation.NotEquals,
        OperatorType.LessThan => UnaryMatchOperation.LessThan,
        OperatorType.GreaterThan => UnaryMatchOperation.GreaterThan,
        OperatorType.LessThanEqual => UnaryMatchOperation.LessThanOrEqual,
        OperatorType.GreaterThanEqual => UnaryMatchOperation.GreaterThanOrEqual,
        _ => throw new ArgumentOutOfRangeException(nameof(current), current, null)
    };


    internal static IEnumerable<(string Value, ValueTokenType Type)> GetValuesForIn(
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

    internal static void ThrowInvalidInValue(Query query, BlittableJsonReaderObject parameters, QueryExpression val)
    {
        throw new InvalidQueryException("Expected in argument to be value, but was: " + val, query.QueryText, parameters);
    }

    internal static QueryFieldName ExtractIndexFieldName(Query query, BlittableJsonReaderObject parameters, QueryExpression field, QueryMetadata metadata)
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

    internal static int GetFieldId(string fieldName, Index index, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null, bool isForQuery = true)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        if (fieldName.Equals(Client.Constants.Documents.Indexing.Fields.DocumentIdMethodName, StringComparison.OrdinalIgnoreCase))
            return 0;

        if (isForQuery == false)
        {
            if (fieldName is "score" or "score()")
                return ScoreId;
        }

        if (queryMapping is not null)
        {
            if (queryMapping.IndexFields.TryGetValue(fieldName, out var indexField) == false)
                ThrowNotFoundInIndex();

            if (indexField!.Indexing is FieldIndexing.No)
                ThrowFieldIsNotIndexed();
                
            return indexField.Id;
        }

        if (indexMapping is not null)
        {
            if (indexMapping.TryGetByFieldName(fieldName, out var binding) == false)
                ThrowNotFoundInIndex();

            if (binding.FieldIndexingMode is FieldIndexingMode.No)
                ThrowFieldIsNotIndexed();

            return binding.FieldId;
        }

        throw new InvalidQueryException($"{nameof(IndexFieldBinding)} or {nameof(IndexFieldsMapping)} not found in {nameof(CoraxQueryBuilder)}.");
        
        void ThrowFieldIsNotIndexed() => throw new InvalidQueryException($"Field {fieldName} is not indexed in Index {index.Name}. You can index it by changing `Indexing` option from `No`.");
        
        void ThrowNotFoundInIndex() => throw new InvalidQueryException($"Field {fieldName} not found in Index '{index.Name}'.");
    }
    
    internal static QueryFieldName ExtractIndexFieldName(ValueExpression field, QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        return metadata.GetIndexFieldName(new QueryFieldName(field.Token.Value, field.Value == ValueTokenType.String), parameters);
    }

    internal static bool IsExact(Index index, bool exact, QueryFieldName fieldName)
    {
        if (exact)
            return true;

        if (index?.Definition?.IndexFields != null && index.Definition.IndexFields.TryGetValue(fieldName, out var indexingOptions))
        {
            return indexingOptions.Indexing == FieldIndexing.Exact;
        }
        
        return false;
    }

    internal static QueryExpression EvaluateMethod(Query query, QueryMetadata metadata, TransactionOperationContext serverContext,
        DocumentsOperationContext documentsContext, MethodExpression method, ref BlittableJsonReaderObject parameters)
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

    internal static string GetValueAsString(object value)
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
    
    internal static MatchCompareFieldType TranslateOrderByForCorax(OrderByFieldType original) =>
        original switch
        {
            OrderByFieldType.Double => MatchCompareFieldType.Floating,
            OrderByFieldType.Long => MatchCompareFieldType.Integer,
            OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Sequence,
            _ => MatchCompareFieldType.Sequence
        };
    
    internal static ComparerType GetComparerType(bool ascending, OrderByFieldType original, int fieldId) => (ascending, original, fieldId) switch
    {
        (true, OrderByFieldType.AlphaNumeric, _) => ComparerType.AscendingAlphanumeric,
        (false, OrderByFieldType.AlphaNumeric, _) => ComparerType.DescendingAlphanumeric,
        (_, _, ScoreId) => ComparerType.Boosting,
        (true, _, _) => ComparerType.Ascending,
        (false, _, _) => ComparerType.Descending,
    };
    
    internal enum ComparerType
    {
        Ascending,
        Descending,
        Boosting,
        AscendingAlphanumeric,
        DescendingAlphanumeric
    }
}
