using System;
using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server;
using Spatial4n.Shapes;
using Constants = Raven.Client.Constants;
using Index = Raven.Server.Documents.Indexes.Index;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;

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

    [DoesNotReturn]
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
        bool allowObjectsInParameters = false, bool allowArraysInParameters = false)
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

            if (allowArraysInParameters && parameterValue is BlittableJsonReaderArray)
                return (parameterValue, ValueTokenType.Parameter);
            
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

    [DoesNotReturn]
    internal static void ThrowQueryTooComplexException(QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Query is too complex", metadata.QueryText, parameters);
    }

    [DoesNotReturn]
    internal static void ThrowUnhandledValueTokenType(ValueTokenType type)
    {
        throw new NotSupportedException($"Unhandled token type: {type}");
    }

    [DoesNotReturn]
    internal static void ThrowInvalidOperatorInSearch(QueryMetadata metadata, BlittableJsonReaderObject parameters, FieldExpression fieldExpression)
    {
        throw new InvalidQueryException($"Supported operators in search() method are 'OR' or 'AND' but was '{fieldExpression.FieldValue}'", metadata.QueryText,
            parameters);
    }

    [DoesNotReturn]
    internal static void ThrowInvalidParameterType(ValueTokenType expectedValueType, (object Value, ValueTokenType Type) item, string queryText,
        BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException("Expected query parameter to be " + expectedValueType + " but was " + item.Type + ": " + item.Value, queryText, parameters);
    }

    [DoesNotReturn]
    internal static void ThrowMethodExpectsArgumentOfTheFollowingType(string methodName, ValueTokenType expectedType, ValueTokenType gotType, string queryText,
        BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Method {methodName}() expects to get an argument of type {expectedType} while it got {gotType}", queryText, parameters);
    }

    [DoesNotReturn]
    public static void ThrowParametersWereNotProvided(string queryText)
    {
        throw new InvalidQueryException("The query is parametrized but the actual values of parameters were not provided", queryText, null);
    }

    [DoesNotReturn]
    public static void ThrowParameterValueWasNotProvided(string parameterName, string queryText, BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Value of parameter '{parameterName}' was not provided", queryText, parameters);
    }

    [DoesNotReturn]
    internal static void ThrowUnexpectedParameterValue(object parameter, string queryText, BlittableJsonReaderObject parameters)
    {
        throw new InvalidQueryException($"Parameter value '{parameter}' of type {parameter.GetType().FullName} is not supported", queryText, parameters);
    }

    [DoesNotReturn]
    internal static void ThrowValueTypeMismatch(string fieldName, ValueTokenType fieldType, ValueTokenType expectedType)
    {
        throw new InvalidOperationException($"Field '{fieldName}' should be a '{expectedType}' but was '{fieldType}'.");
    }

    [DoesNotReturn]
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
                yield return (FormatAsInValueString(value, type), type);
            }
        }
    }

    internal static unsafe bool TryGetTime(Index index, object value, out long ticks)
    {
        ticks = -1;
        DateTime dt = default;
        DateTimeOffset dto = default;
        DateOnly @do = default;
        TimeOnly to = default;
        LazyStringParser.Result result = LazyStringParser.Result.Failed;

        switch (value)
        {
            case LazyStringValue lsv:
                result = LazyStringParser.TryParseTimeForQuery(lsv.Buffer, lsv.Size, out dt, out dto, out @do, out to,
                    index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.ProperlyParseThreeDigitsMillisecondsDates);
                break;
            case string valueAsString:
                fixed (char* buffer = valueAsString)
                {
                    result = LazyStringParser.TryParseTimeForQuery(buffer, valueAsString.Length, out dt, out dto, out @do, out to,
                        index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.ProperlyParseThreeDigitsMillisecondsDates);
                }

                break;
            default:
                var otherAsString = value.ToString();
                fixed (char* buffer = otherAsString)
                {
                    result = LazyStringParser.TryParseTimeForQuery(buffer, otherAsString.Length, out dt, out dto, out @do, out to,
                        index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.ProperlyParseThreeDigitsMillisecondsDates);
                }

                break;
        }

        switch (result)
        {
            case LazyStringParser.Result.Failed:
                return false;
            case LazyStringParser.Result.DateTime:
                ticks = dt.Ticks;
                return true;
            case LazyStringParser.Result.DateTimeOffset:
                ticks = dto.UtcDateTime.Ticks;
                return true;
            case LazyStringParser.Result.TimeOnly:
                ticks = to.Ticks;
                return true;
            case LazyStringParser.Result.DateOnly:
                ticks = @do.DayNumber * TimeSpan.TicksPerDay;
                return true;
            default:
                throw new InvalidOperationException("Should not happen!");
        }
    }

    [DoesNotReturn]
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

    internal static FieldMetadata GetFieldIdForOrderBy(ByteStringContext allocator, string fieldName, Index index, bool hasDynamics, Lazy<List<string>> dynamicFields, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null,
        bool isForQuery = true)
    {
        if (fieldName is "score()")
            return FieldMetadata.Build(allocator, fieldName, -1, FieldIndexingMode.Normal, null);



        return GetFieldMetadata(allocator, fieldName, index, indexMapping, queryMapping, hasDynamics, dynamicFields, isForQuery: isForQuery, isSorting: true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static FieldMetadata GetFieldMetadata(in CoraxQueryBuilder.Parameters parameters, string fieldName, bool isForQuery = true,
        bool exact = false, bool isSorting = false, bool hasBoost = false, bool handleSearch = false)
    {
        return GetFieldMetadata(parameters.Allocator, fieldName, parameters.Index, parameters.IndexFieldsMapping, parameters.FieldsToFetch, parameters.HasDynamics, parameters.DynamicFields, isForQuery, exact, isSorting, hasBoost, handleSearch);
    }
    
    internal static FieldMetadata GetFieldMetadata(ByteStringContext allocator, string fieldName, Index index, IndexFieldsMapping indexMapping,
        FieldsToFetch queryMapping, bool hasDynamics, Lazy<List<string>> dynamicFields, bool isForQuery = true,
        bool exact = false, bool isSorting = false, bool hasBoost = false, bool handleSearch = false, bool forceDefaultSearchAnalyzer = false)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        FieldMetadata metadata;

        //Sometimes index can contains Id property and its different than real document ID. We've to 
        if ((fieldName.Equals(Constants.Documents.Indexing.Fields.DocumentIdMethodName, StringComparison.OrdinalIgnoreCase) && indexMapping.ContainsField(fieldName)) == false &&
            fieldName is Constants.Documents.Indexing.Fields.DocumentIdFieldName)
        {
            metadata = indexMapping.GetByFieldId(0).Metadata;
            return exact
                ? metadata.ChangeAnalyzer(FieldIndexingMode.Exact, null).ChangeScoringMode(hasBoost)
                : metadata.ChangeScoringMode(hasBoost);
        }

        if (isForQuery == false)
        {
            if (fieldName is "score" or "score()")
                return default;
        }
        
        if (indexMapping.TryGetByFieldName(allocator, fieldName, out var indexFinding))
        {
            if (exact) //When field has exact let's change the analyzer to do nothing
                metadata = indexFinding.Metadata.ChangeAnalyzer(FieldIndexingMode.Exact);
            else if (indexFinding.FieldIndexingMode is FieldIndexingMode.Search) // in case of search
                metadata = handleSearch
                    ? indexFinding.Metadata //when we want mapping for search lets use 'search` analyzer 
                    : indexFinding.Metadata.ChangeAnalyzer(FieldIndexingMode.Normal, indexMapping.DefaultAnalyzer); //but when we do a TermMatch we want to have just default analyzer, even on full-text search field
            else
                metadata = indexFinding.Metadata;

            metadata = metadata.ChangeScoringMode(hasBoost);
        }
        else
        {
            if (hasDynamics == false)
                ThrowNotFoundInIndex();

            var mode = exact
                ? FieldIndexingMode.Exact
                : FieldIndexingMode.Normal;
            //Context: dynamic field without explicit configuration. For search query we might want to use the default search analyzer:
            var analyzer = handleSearch && forceDefaultSearchAnalyzer 
                ? indexMapping.SearchAnalyzer(fieldName) 
                : indexMapping.DefaultAnalyzer;
            
            metadata = FieldMetadata.Build(allocator, fieldName, Corax.Constants.IndexWriter.DynamicField, mode, analyzer, hasBoost: hasBoost);
        }

        return metadata;
        void ThrowNotFoundInIndex() => throw new InvalidQueryException($"Field {fieldName} not found in Index '{index.Name}'.");
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

    internal static QueryExpression EvaluateMethod(Query query, QueryMetadata metadata, TransactionOperationContext serverContext, AbstractCompareExchangeStorage compareExchangeStorage, MethodExpression method, BlittableJsonReaderObject parameters, QueryTimeScope queryTime = null)
    {
        var methodType = QueryMethod.GetMethodType(method.Name.Value);

        switch (methodType)
        {
            case MethodType.CompareExchange:
                var v = GetValue(query, metadata, parameters, method.Arguments[0]);
                if (v.Type != ValueTokenType.String)
                    throw new InvalidQueryException("Expected value of type string, but got: " + v.Type, query.QueryText, parameters);

                object value = null;
                compareExchangeStorage.GetCompareExchangeValue(serverContext, v.Value.ToString()).Value?.TryGetMember(Constants.CompareExchange.ObjectFieldName, out value);

                if (value == null)
                    return new ValueExpression(string.Empty, ValueTokenType.Null);

                return new ValueExpression(value.ToString(), ValueTokenType.String);

            case MethodType.Now:
                if (method.Arguments is { Count: > 1 })
                    throw new InvalidQueryException("Method now() expects zero or one argument.", query.QueryText, parameters);
                return new ValueExpression(
                    ResolveTimeFunction(query, metadata, parameters, method, queryTime.Now).GetDefaultRavenFormat(isUtc: true),
                    ValueTokenType.String);

            case MethodType.Today:
                if (method.Arguments is { Count: > 0 })
                    throw new InvalidQueryException("Method today() does not accept arguments. Use now() with an offset instead (e.g., now('+1d')).", query.QueryText, parameters);
                return new ValueExpression(
                    queryTime.Today.GetDefaultRavenFormat(isUtc: true),
                    ValueTokenType.String);
        }

        throw new ArgumentException($"Unknown method {method.Name}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static DateTime ResolveTimeFunction(Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters, MethodExpression method, DateTime baseTime)
    {
        if (method.Arguments is not { Count: 1 })
            return baseTime;

        var (value, _) = GetValue(query, metadata, parameters, method.Arguments[0]);
        var offsetString = value?.ToString();

        if (string.IsNullOrEmpty(offsetString))
            throw new InvalidQueryException($"Method {method.Name.Value}() offset argument must be a non-empty string.", query.QueryText, parameters);

        if (TimeFunctionOffset.TryParse(offsetString.AsSpan(), out var offset) == false)
            throw new InvalidQueryException(
                $"Invalid offset format '{offsetString}' for {method.Name.Value}(). " +
                "Expected format: [+|-]N(y|year|years)[N(mo|month|months)][N(d|day|days)][N(h|hour|hours)][N(m|min|minute|minutes)][N(s|sec|second|seconds)]. " +
                    "Units must appear in descending order. Spaces between components are allowed. " +
                    "Examples: '+1y6mo', '-2hours30minutes', '1 year 6 months', '15d'.",
                query.QueryText, parameters);

        return offset.Apply(baseTime);
    }

    internal static string CoraxGetValueAsString(object value) => value switch
    {
        StringSegment s => s.Value,
        string { Length: 0 } => global::Corax.Constants.EmptyString,
        string s => s,
        long l => l.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString(CultureInfo.InvariantCulture),
        _ => value?.ToString()
    };

    internal static ComparerType GetComparerType(bool ascending, OrderMetadata order) => (ascending, order.FieldType, order.Field.FieldId) switch
    {
        (true, MatchCompareFieldType.Spatial, _) => ComparerType.AscendingSpatial,
        (false, MatchCompareFieldType.Spatial, _) => ComparerType.DescendingSpatial,
        (_, MatchCompareFieldType.Score, _) => ComparerType.Boosting,
        (true, MatchCompareFieldType.Alphanumeric, _) => ComparerType.AscendingAlphanumeric,
        (false, MatchCompareFieldType.Alphanumeric, _) => ComparerType.DescendingAlphanumeric,
        (true, _, _) => ComparerType.Ascending,
        (false, _, _) => ComparerType.Descending,
    };

    internal enum ComparerType
    {
        Ascending,
        Descending,
        Boosting,
        AscendingAlphanumeric,
        DescendingAlphanumeric,
        AscendingSpatial,
        DescendingSpatial
    }

    internal static IShape HandleWkt(CoraxQueryBuilder.Parameters builderParameters, string fieldName, MethodExpression expression,
        SpatialField spatialField, out SpatialUnits units)
    {
        var wktValue = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters, (ValueExpression)expression.Arguments[0]);
        QueryBuilderHelper.AssertValueIsString(fieldName, wktValue.Type);

        SpatialUnits? spatialUnits = null;
        if (expression.Arguments.Count == 2)
            spatialUnits = GetSpatialUnits(builderParameters.Metadata.Query, expression.Arguments[1] as ValueExpression, builderParameters.Metadata, builderParameters.QueryParameters, fieldName);

        units = spatialUnits ?? spatialField.Units;

        var wkt = CoraxGetValueAsString(wktValue.Value);

        try
        {
            return spatialField.ReadShape(wkt, spatialUnits);
        }
        catch (Exception e)
        {
            throw new InvalidQueryException($"Value '{wkt}' is not a valid WKT value.", builderParameters.Metadata.QueryText, builderParameters.QueryParameters, e);
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

        var spatialUnitsValueAsString = CoraxGetValueAsString(spatialUnitsValue.Value);
        if (Enum.TryParse(typeof(SpatialUnits), spatialUnitsValueAsString, true, out var su) == false)
            throw new InvalidOperationException(
                $"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{spatialUnitsValueAsString}'.");

        return (SpatialUnits)su;
    }

    internal static bool TryUseTime(Index index, string fieldName, object valueFirst, object valueSecond, bool exact, out long ticksFirst, out long ticksSecond)
    {
        ticksFirst = -1;
        ticksSecond = -1;

        if (exact || index == null || valueFirst == null || valueSecond == null || index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.TimeTicks)
            return false;

        if (index.IndexFieldsPersistence.HasTimeValues(fieldName) && TryGetTime(index, valueFirst, out ticksFirst) && TryGetTime(index, valueSecond, out ticksSecond))
            return true;

        return false;
    }

    internal static bool TryUseTime(Index index, string fieldName, object value, bool exact, out long ticks)
    {
        ticks = -1;

        if (exact || index == null || value == null || index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.TimeTicks)
            return false;

        if (index.IndexFieldsPersistence.HasTimeValues(fieldName) && TryGetTime(index, value, out ticks))
            return true;

        return false;
    }

    internal static MethodExpression FindMoreLikeThisExpression(QueryExpression expression)
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

    internal static VectorValue GetVectorValueFromBlittableJsonVectorReader(ByteStringContext allocator, in VectorOptions options, BlittableJsonReaderVector value)
    {
        var bytesRequired = value.Length * (options.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : sizeof(byte));
        var scope = allocator.Allocate(bytesRequired, out Memory<byte> memory);
        
        
        switch (options.SourceEmbeddingType)
        {
            case VectorEmbeddingType.Single when value.TryReadArray(out ReadOnlySpan<float> asFloats):
                asFloats.CopyTo(MemoryMarshal.Cast<byte, float>(memory.Span));
                break;
            case VectorEmbeddingType.Single:
            {
                var floats = MemoryMarshal.Cast<byte, float>(memory.Span);
                int it = 0;
                foreach (var v in value.ReadAs<float>())
                    floats[it++] = v;
                break;
            }
            case VectorEmbeddingType.Int8 when value.TryReadArray(out ReadOnlySpan<sbyte> asSbyte):
            {
                asSbyte.CopyTo(MemoryMarshal.Cast<byte, sbyte>(memory.Span));
                break;
            }
            case VectorEmbeddingType.Int8:
            {
                var sbytes = MemoryMarshal.Cast<byte, sbyte>(memory.Span);
                int it = 0;
                foreach (var v in value.ReadAs<sbyte>())
                    sbytes[it++] = v;
                break;
            }
            case VectorEmbeddingType.Binary when value.TryReadArray(out ReadOnlySpan<byte> asSbyte):
                asSbyte.CopyTo(memory.Span);
                break;
            case VectorEmbeddingType.Binary:
            {
                int it = 0;
                foreach (var v in value.ReadAs<byte>())
                    memory.Span[it++] = v;
                break;
            }
        }

        return GenerateEmbeddings.FromArray(allocator, scope, memory, options, bytesRequired);
    }
    
    public static bool EvaluateConstantExpressionForWhenQuery(QueryExpression expression, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (expression is TrueExpression)
            return true;

        if (expression is NegatedExpression negated)
            return EvaluateConstantExpressionForWhenQuery(negated.Expression, query, metadata, parameters) == false;

        if (expression is InExpression inExpression)
            return EvaluateInExpressionForWhenQuery(inExpression, query, metadata, parameters);

        if (expression is not BinaryExpression constantExpression)
            throw new InvalidOperationException($"Expected binary or in expression, but got: '{expression}' of type '{expression.Type}'.");

        if (constantExpression.Operator is OperatorType.And or OperatorType.Or)
        {
            var leftResult = EvaluateConstantExpressionForWhenQuery(constantExpression.Left, query, metadata, parameters);
            var rightResult = EvaluateConstantExpressionForWhenQuery(constantExpression.Right, query, metadata, parameters);
            return (constantExpression.Operator) switch
            {
                OperatorType.And => leftResult && rightResult,
                OperatorType.Or => leftResult || rightResult,
                _ => throw new InvalidOperationException("Should not happen!")
            };
        }

        if (constantExpression.Left is not FieldExpression leftField)
            throw new InvalidOperationException($"Expected parameter field (e.g. $p0), but got: '{constantExpression.Left}'.");
        
        bool paramIsMissing = false;
        PortableExceptions.ThrowIfNot<InvalidOperationException>(leftField.FieldValue.StartsWith('$'), "Expected parameter field (e.g. $p0), but got: '{constantExpression.Left}'.");
        
        if (parameters == null || parameters.TryGet(new StringSegment(leftField.FieldValue, 1, leftField.FieldValue.Length - 1), out object paramValue) == false)
        {
            paramValue = null;
            paramIsMissing = true;
        }

        var rightExpression = (ValueExpression)constantExpression.Right;
        if ((paramIsMissing || paramValue is null) && rightExpression.Value is not ValueTokenType.Null)
        {
            //The left side is null or does not exist in parameter JSON, the right side is a non-null value. E.g.:
            // $p0 != 1.0 // true
            return constantExpression.Operator switch
            {
                OperatorType.NotEqual => true,
                _ => false
            };
        }

        switch (rightExpression.Value)
        {
            case ValueTokenType.Null:
            {
                bool paramIsNull = paramIsMissing || paramValue is null;
                return constantExpression.Operator switch
                {
                    OperatorType.Equal => paramIsNull,
                    OperatorType.NotEqual => paramIsNull == false,
                    _ => false
                };
            }
            case ValueTokenType.True:
            case ValueTokenType.False:
            {
                if (paramValue is not bool paramValueAsBool)
                    throw new InvalidOperationException($"Expected boolean value, but got: '{paramValue}' of type '{paramValue?.GetType()}'.");
            
                var leftSideAsBool = rightExpression.Value == ValueTokenType.True;

                return constantExpression.Operator switch
                {
                    OperatorType.Equal => paramValueAsBool == leftSideAsBool,
                    OperatorType.NotEqual => paramValueAsBool != leftSideAsBool,
                    _ => throw new InvalidOperationException($"Cannot execute {constantExpression.Operator} on boolean values.")
                };
            }
            case ValueTokenType.Long:
            {
                long leftSideValue = paramValue switch
                {
                    byte b => b,
                    sbyte sb => sb,
                    ushort us => us,
                    short s => s,
                    uint ui => ui,
                    int i => i,
                    ulong ul => checked((long)ul),
                    long l => l,
                    double d => checked((long)d),   
                    float f => checked((long)f),
                    decimal dc => checked((long)dc),
                    LazyStringValue lsv => lsv.ToInt64(CultureInfo.InvariantCulture),
                    LazyNumberValue lnv => lnv.ToInt64(CultureInfo.InvariantCulture),
                    _ => throw new InvalidOperationException($"Cannot convert {paramValue!.GetType()} to long")
                };

                if (long.TryParse(rightExpression.Token.Value, out var rightSideValue) == false)
                {
                    var rightSideTokenValue = rightExpression.Token.Value.Replace("_", string.Empty);
                    if (long.TryParse(rightSideTokenValue, out rightSideValue) == false)
                        throw new InvalidOperationException($"Cannot convert {rightExpression.Token.Value} to long");
                }
            
                return constantExpression.Operator switch
                {
                    OperatorType.Equal => leftSideValue == rightSideValue,
                    OperatorType.NotEqual => leftSideValue != rightSideValue,
                    OperatorType.GreaterThan => leftSideValue > rightSideValue,
                    OperatorType.GreaterThanEqual => leftSideValue >= rightSideValue,
                    OperatorType.LessThan => leftSideValue < rightSideValue,
                    OperatorType.LessThanEqual => leftSideValue <= rightSideValue,
                    _ => false
                };
            }
            case ValueTokenType.Double:
            {
                double leftSideValue = paramValue switch
                {
                    byte b => b,
                    sbyte sb => sb,
                    ushort us => us,
                    short s => s,
                    uint ui => ui,
                    int i => i,
                    ulong ul => checked((long)ul),
                    long l => l,
                    double d => d,
                    float f => f,
                    decimal dc => checked((double)dc),
                    LazyStringValue lsv => lsv.ToDouble(CultureInfo.InvariantCulture),
                    LazyNumberValue lnv => lnv.ToDouble(CultureInfo.InvariantCulture),
                    _ => throw new InvalidOperationException($"Cannot convert {paramValue!.GetType()} to double")
                };
            
                if (double.TryParse(rightExpression.Token.Value, out var rightSideValue) == false)
                    throw new InvalidOperationException($"Cannot convert {rightExpression.Token.Value} to double");
            
                return constantExpression.Operator switch
                {
                    OperatorType.Equal => leftSideValue.AlmostEquals(rightSideValue),
                    OperatorType.NotEqual => leftSideValue.AlmostEquals(rightSideValue) == false,
                    OperatorType.GreaterThan => leftSideValue > rightSideValue,
                    OperatorType.GreaterThanEqual => leftSideValue >= rightSideValue,
                    OperatorType.LessThan => leftSideValue < rightSideValue,
                    OperatorType.LessThanEqual => leftSideValue <= rightSideValue,
                    _ => false
                };
            }
            case ValueTokenType.String:
            {
                var paramIsString = paramValue is string or StringSegment or LazyStringValue or LazyCompressedStringValue;
                if (paramIsString == false)
                    throw new InvalidOperationException($"Cannot compare string with non-string value. Parameter type is: {paramValue!.GetType().FullName}");
                
                var leftValueAsString = GetValueAsString(paramValue);
                var rightValueAsString = rightExpression.Token.Value;

                var comparisonResult = string.Compare(leftValueAsString, rightValueAsString, StringComparison.OrdinalIgnoreCase);

                return constantExpression.Operator switch
                {
                    OperatorType.Equal => comparisonResult == 0,
                    OperatorType.NotEqual => comparisonResult != 0,
                    OperatorType.GreaterThan => comparisonResult > 0,
                    OperatorType.GreaterThanEqual => comparisonResult >= 0,
                    OperatorType.LessThan => comparisonResult < 0,
                    OperatorType.LessThanEqual => comparisonResult <= 0,
                    _ => false
                };
            }
            default:
                return false;
        }
    }

    private static bool EvaluateInExpressionForWhenQuery(InExpression inExpression, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        if (inExpression.Source is not FieldExpression sourceField)
            throw new InvalidOperationException($"Expected parameter field (e.g. $p0), but got: '{inExpression.Source}'.");

        PortableExceptions.ThrowIfNot<InvalidOperationException>(sourceField.FieldValue.StartsWith('$'),
            $"Expected parameter field (e.g. $p0), but got: '{sourceField.FieldValue}'.");

        object paramValue = null;
        parameters?.TryGet(new StringSegment(sourceField.FieldValue, 1, sourceField.FieldValue.Length - 1), out paramValue);

        if (paramValue is BlittableJsonReaderArray array)
            return WhenInArrayToArrayExpressionEvaluator(array, inExpression, query, metadata, parameters);

        var paramType = GetValueTokenType(paramValue, metadata.QueryText, parameters);
        var paramValueUnwrapped = UnwrapParameter(paramValue, paramType);
        return WhenInScalarExpressionEvaluator(FormatAsInValueString(paramValueUnwrapped, paramType), inExpression, query, metadata, parameters);
    }

    private static bool WhenInScalarExpressionEvaluator(string paramValueAsString, InExpression inExpression, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        int matches = 0;
        int count = 0;
        paramValueAsString ??= Corax.Constants.NullValue;
        foreach (var (valueAsString, _) in GetValuesForIn(query, inExpression, metadata, parameters))
        {
            count++;
            var inValue = valueAsString ?? Corax.Constants.NullValue;
            if (string.Equals(paramValueAsString, inValue, StringComparison.OrdinalIgnoreCase))
            {
                if (inExpression.All == false)
                    return true;

                matches++;
            }
        }

        return inExpression.All && matches == count;
    }

    private static bool WhenInArrayToArrayExpressionEvaluator(BlittableJsonReaderArray array, InExpression inExpression, Query query, QueryMetadata metadata, BlittableJsonReaderObject parameters)
    {
        var valuesSet = new HashSet<string>(inExpression.Values.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (valueAsString, _) in GetValuesForIn(query, inExpression, metadata, parameters))
        {
            valuesSet.Add(valueAsString ?? Corax.Constants.NullValue);
        }

        if (inExpression.All)
        {
            HashSet<string> parameterValues = new(array.Length, StringComparer.OrdinalIgnoreCase);
            foreach (var (elementValue, elementType) in UnwrapArray(array, metadata.QueryText, parameters))
            {
                parameterValues.Add(FormatAsInValueString(elementValue, elementType) ?? Corax.Constants.NullValue);
            }

            return parameterValues.IsSubsetOf(valuesSet);
        }

        foreach (var (elementValue, elementType) in UnwrapArray(array, metadata.QueryText, parameters))
        {
            var elementAsString = FormatAsInValueString(elementValue, elementType) ?? Corax.Constants.NullValue;
            if (valuesSet.Contains(elementAsString))
                return true;
        }

        return false;
    }

    private static string FormatAsInValueString(object value, ValueTokenType type)
    {
        return type switch
        {
            ValueTokenType.Long => ((long)value).ToString(CultureInfo.InvariantCulture),
            ValueTokenType.Double => ((double)value).ToString("G", CultureInfo.InvariantCulture),
            ValueTokenType.True => LuceneDocumentConverterBase.TrueString,
            ValueTokenType.False => LuceneDocumentConverterBase.FalseString,
            ValueTokenType.Null => null,
            _ => GetValueAsString(value),
        };
    }
}
