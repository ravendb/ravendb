using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Amazon.SimpleNotificationService.Model;
using Corax;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Constants = Raven.Client.Constants;
using Encoding = System.Text.Encoding;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxDocumentConverterBase : ConverterBase
{
    protected const int DocumentBufferSize = 1024 * 10;
    private static readonly Memory<byte> _trueLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("true"));
    private static readonly Memory<byte> _falseLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("false"));
    private static readonly StandardFormat _standardFormat = new StandardFormat('g');
    private static readonly StandardFormat _timeSpanFormat = new StandardFormat('c');
    
    private ConversionScope Scope;
    protected readonly IndexFieldsMapping _knownFields;
    protected readonly ByteStringContext _allocator;

    private const int InitialSizeOfEnumerableBuffer = 128;
    private bool EnumerableDataStructExist => StringsListForEnumerableScope is not null && LongsListForEnumerableScope is not null && DoublesListForEnumerableScope is not null;

    public List<Memory<byte>> StringsListForEnumerableScope;
    public List<long> LongsListForEnumerableScope;
    public List<double> DoublesListForEnumerableScope;

    public abstract Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id);

    protected CoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
        _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        _knownFields = GetKnownFields();
        Scope = new();
    }

    public static IndexFieldsMapping GetKnownFields(ByteStringContext allocator, Index index)
    {
        var knownFields = new IndexFieldsMapping(allocator);
        //todo maciej: will optimize it while doing static indexes.
        Slice.From(allocator, index.Type.IsMapReduce()
            ? Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
            : Constants.Documents.Indexing.Fields.DocumentIdFieldName, ByteStringType.Immutable, out var value);

        knownFields = knownFields.AddBinding(0, value, null);
        foreach (var field in index.Definition.IndexFields.Values)
        {
            Slice.From(allocator, field.Name, ByteStringType.Immutable, out value);
            knownFields = knownFields.AddBinding(field.Id, value, null, hasSuggestion: field.HasSuggestions);
        }

        if (index.Type.IsMapReduce())
        {
            Slice.From(allocator, Constants.Documents.Indexing.Fields.AllStoredFields, ByteStringType.Immutable, out var storedKey);
            knownFields = knownFields.AddBinding(knownFields.Count, storedKey, null, true);
        }

        return knownFields;
    }

    public IndexFieldsMapping GetKnownFields()
    {
        return _knownFields ?? GetKnownFields(_allocator, _index);
    }

    protected void InsertRegularField(IndexField field, object value, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter,
        IWriterScope scope, bool nestedArray = false)
    {
        var path = field.Name;
        var valueType = GetValueType(value);
        shouldSkip = false;
        long @long;
        double @double;

        switch (valueType)
        {
            case ValueType.Double:
                if (value is LazyNumberValue ldv)
                {
                    if (TryToTrimTrailingZeros(ldv, indexContext, out var doubleAsString) == false)
                        doubleAsString = ldv.Inner;
                    @long = (long)ldv;
                    @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                    scope.Write(field.Id, doubleAsString.AsSpan(), @long, @double, ref entryWriter);
                    break;
                }
                else
                {
                    using (_allocator.Allocate(16, out var buffer))
                    {
                        var length = 0;
                        switch (value)
                        {
                            case double d:
                                if (Utf8Formatter.TryFormat(d, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                                break;

                            case decimal dm:
                                if (Utf8Formatter.TryFormat(dm, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as decimal into bytes.");
                                break;

                            case float f:
                                if (Utf8Formatter.TryFormat(f, buffer.ToSpan(), out length, _standardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as float into bytes.");
                                break;
                        }

                        @long = Convert.ToInt64(value);
                        @double = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                        buffer.Truncate(length);
                        scope.Write(field.Id, buffer.ToSpan(), @long, @double, ref entryWriter);
                        return;
                    }
                }

            case ValueType.Numeric:
                var lazyNumber = value as LazyNumberValue;
                if (lazyNumber == null)
                {
                    scope.Write(field.Id, lazyNumber.Inner.AsSpan(), (long)value, Convert.ToDouble(value), ref entryWriter);
                    return;
                }

                @long = (long)lazyNumber;
                @double = lazyNumber.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(field.Id, lazyNumber.Inner.AsSpan(), @long, @double, ref entryWriter);
                return;

            case ValueType.String:
                scope.Write(field.Id, (string)value, ref entryWriter);
                return;

            case ValueType.LazyCompressedString:
            case ValueType.LazyString:
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;
                scope.Write(field.Id, lazyStringValue.AsSpan(), ref entryWriter);
                return;

            case ValueType.Enum:
                scope.Write(field.Id, value.ToString(), ref entryWriter);
                return;

            case ValueType.Boolean:
                scope.Write(field.Id, ((bool)value ? _trueLiteral : _falseLiteral).Span, ref entryWriter);
                return;

            case ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                scope.Write(field.Id, dateAsBytes, ref entryWriter);
                return;

            case ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                scope.Write(field.Id, dateTimeOffsetBytes, ref entryWriter);
                return;

            case ValueType.TimeSpan:
                var timeSpan = (TimeSpan)value;
                using (_allocator.Allocate(256, out var buffer))
                {
                    if (Utf8Formatter.TryFormat(timeSpan, buffer.ToSpan(), out var bytesWritten, _timeSpanFormat) == false)
                        throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                    buffer.Truncate(bytesWritten);
                    scope.Write(field.Id, buffer.ToSpan(), ref entryWriter);
                }

                return;

            case ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                @long = iConvertible.ToInt64(CultureInfo.InvariantCulture);
                @double = iConvertible.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(field.Id, iConvertible.ToString(CultureInfo.InvariantCulture), @long, @double, ref entryWriter);
                return;

            case ValueType.Enumerable:
                var iterator = (IEnumerable)value;
                if (EnumerableDataStructExist == false)
                {
                    StringsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    LongsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                    DoublesListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
                }
                var enumerableScope = new EnumerableWriterScope(StringsListForEnumerableScope, LongsListForEnumerableScope, DoublesListForEnumerableScope, _allocator);
                foreach (var item in iterator)
                {
                    InsertRegularField(field, item, indexContext, out _, ref entryWriter, enumerableScope);
                }

                enumerableScope.Finish(field.Id, ref entryWriter);
                return;

            case ValueType.DynamicJsonObject:
                HandleObject((BlittableJsonReaderObject)value, field, indexContext, out _, ref entryWriter, scope);
                return;

            case ValueType.ConvertToJson:
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (val is not DynamicJsonValue json)
                {
                    InsertRegularField(field, val, indexContext, out _, ref entryWriter, scope, nestedArray);
                    return;
                }

                var jsonScope = Scope.CreateJson(json, indexContext);
                InsertRegularField(field, HandleStreamsAndConversionIntoJson(null, jsonScope), indexContext, out _, ref entryWriter, scope);
                return;

            case ValueType.BlittableJsonObject:
                HandleObject((BlittableJsonReaderObject)value, field, indexContext, out _, ref entryWriter, scope);
                return;

            case ValueType.DynamicNull:
            case ValueType.Null:
                scope.Write(field.Id, Encoding.UTF8.GetBytes(Constants.Documents.Indexing.Fields.NullValue), ref entryWriter);
                return;
            case ValueType.BoostedValue:
                throw new InvalidParameterException("Boosting in index is not supported by Corax. You can do it during querying or change index type into Lucene.");
            case ValueType.Stream:

            default:
                throw new NotImplementedException();
        }

        shouldSkip = true;
    }
    
    object HandleStreamsAndConversionIntoJson(Stream streamValue, BlittableJsonReaderObject blittableValue)
    {
        if (blittableValue is not null)
        {
            //TODO PERF 
            //TODO: Notice this is basic implementation. We've to create additional flag `Json` inside Corax for patching.
            var blittableReader = Scope.GetBlittableReader();
            var reader = blittableReader.GetTextReaderFor(blittableValue);
            return reader.ReadToEnd();
        }
        if (streamValue is not null)
        {
            return ToArray(Scope, streamValue, out var streamLength);
        }

        throw new InvalidParameterException($"Got no data at {nameof(HandleStreamsAndConversionIntoJson)}");
    }
    void HandleArray(IEnumerable itemsToIndex, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter,
        IWriterScope scope, bool nestedArray = false)
    {
        //todo maciej https://github.com/ravendb/ravendb/pull/12689/files/d7ab5b0c9db82dff655b647a7cc97a8cdaf8a6fe#r701808845
        shouldSkip = false;
        if (nestedArray)
        {
            return;
        }

        foreach (var itemToIndex in itemsToIndex)
        {
            InsertRegularField(field, itemToIndex, indexContext, out _, ref entryWriter, scope);
        }
    }

    //todo maciej Discuss how we gonna handle nestedArrays. Now I skip them.
    void HandleObject(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter,
        IWriterScope scope, bool nestedArray = false)
    {
        if (val.TryGetMember(Constants.Json.Fields.Values, out var values) &&
            IsArrayOfTypeValueObject(val))
        {
            HandleArray((IEnumerable)values, field, indexContext, out _, ref entryWriter, scope, true);
        }

        shouldSkip = false;
    }

    public override void Dispose()
    {
        Scope?.Dispose();
    }
}
