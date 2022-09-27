using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Mono.Unix.Native;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Spatial4n.Shapes;
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

    internal unsafe static bool TryGetTime(Index index, object value, out long ticks)
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

    internal static void ThrowInvalidInValue(Query query, BlittableJsonReaderObject parameters, QueryExpression val)
    {
        throw new InvalidQueryException("Expected in argument to be value, but was: " + val, query.QueryText, parameters);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static QueryFieldName ExtractIndexFieldNameForOrderBy(Query query, BlittableJsonReaderObject parameters, QueryExpression field, QueryMetadata metadata)
    {
        if (field is MethodExpression me)
        {
            if (me.Name.Value is "score")
                return new QueryFieldName("score()", false);

            return new QueryFieldName("score()", false);
        }

        return ExtractIndexFieldName(query, parameters, field, metadata);
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

    internal static int GetFieldIdForOrderBy(string fieldName, Index index, bool hasDynamics, Lazy<List<string>> dynamicFields, IndexFieldsMapping indexMapping = null, FieldsToFetch queryMapping = null,
        bool isForQuery = true)
    {
        if (fieldName is "score()")
            return ScoreId;

        return GetFieldId(fieldName, index, indexMapping, queryMapping, hasDynamics, dynamicFields, isForQuery);
    }

    internal static int GetFieldId(string fieldName, Index index, IndexFieldsMapping indexMapping, FieldsToFetch queryMapping, bool hasDynamics, Lazy<List<string>> dynamicFields, bool isForQuery = true,
        bool exact = false)
    {
        if (exact)
            return Corax.Constants.IndexSearcher.NonAnalyzer;

        RuntimeHelpers.EnsureSufficientExecutionStack();
        if (fieldName.Equals(Client.Constants.Documents.Indexing.Fields.DocumentIdMethodName, StringComparison.OrdinalIgnoreCase) ||
            fieldName is Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            return 0;

        if (isForQuery == false)
        {
            if (fieldName is "score" or "score()")
                return ScoreId;
        }

        IndexField indexField = null;
        IndexFieldBinding binding = null;
        if (queryMapping?.IndexFields.TryGetValue(fieldName, out indexField) is null or false &&
            indexMapping?.TryGetByFieldName(fieldName, out binding) is null or false)
        {
            if (hasDynamics && dynamicFields.Value.Contains(fieldName))
                return Corax.Constants.IndexWriter.DynamicField;
            
            ThrowNotFoundInIndex();
        }

        if (indexField?.Indexing == FieldIndexing.No || binding?.FieldIndexingMode == FieldIndexingMode.No)
        {
            ThrowFieldIsNotIndexed();
        }

        return indexField?.Id ?? binding?.FieldId ??
            throw new InvalidQueryException($"{nameof(IndexFieldBinding)} or {nameof(IndexFieldsMapping)} not found in {nameof(CoraxQueryBuilder)}.");


        void ThrowFieldIsNotIndexed() =>
            throw new InvalidQueryException($"Field {fieldName} is not indexed in Index {index.Name}. You can index it by changing `Indexing` option from `No`.");

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
        DocumentsOperationContext documentsContext, MethodExpression method, BlittableJsonReaderObject parameters)
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

    internal static string CoraxGetValueAsString(object value) => value switch
    {
        StringSegment s => s.Value,
        string {Length: 0} => Constants.Documents.Indexing.Fields.EmptyString,
        string s => s,
        long l => l.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString(CultureInfo.InvariantCulture),
        null => Corax.Constants.NullValue,
        _ => value?.ToString()
    };

    internal static ComparerType GetComparerType(bool ascending, MatchCompareFieldType original, int fieldId) => (ascending, original, fieldId) switch
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
}
