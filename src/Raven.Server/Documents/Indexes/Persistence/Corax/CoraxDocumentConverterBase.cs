using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using RavenConstants = Raven.Client.Constants;
using CoraxConstants = Corax.Constants;
using Encoding = System.Text.Encoding;
using System.Diagnostics;
using Corax.Mappings;
using Raven.Client.Exceptions.Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxDocumentConverterBase : ConverterBase
{
    private readonly bool _canContainSourceDocumentId;
    private static readonly Memory<byte> _trueLiteral = new(Encoding.UTF8.GetBytes("true"));
    private static ReadOnlySpan<byte> TrueLiteral => _trueLiteral.Span;

    private static readonly Memory<byte> _falseLiteral = new(Encoding.UTF8.GetBytes("false"));
    private static ReadOnlySpan<byte> FalseLiteral => _falseLiteral.Span;


    private static readonly StandardFormat StandardFormat = new('g');
    private static readonly StandardFormat TimeSpanFormat = new('c');

    private readonly ConversionScope Scope;
    private readonly Lazy<IndexFieldsMapping> _knownFieldsForReaders;
    protected IndexFieldsMapping KnownFieldsForWriter;
    protected readonly ByteStringContext Allocator;
    
    private const int InitialSizeOfEnumerableBuffer = 128;
    
    private bool EnumerableDataStructExist =>
        StringsListForEnumerableScope is not null && LongsListForEnumerableScope is not null && DoublesListForEnumerableScope is not null;

    public List<ByteString> StringsListForEnumerableScope;
    public List<long> LongsListForEnumerableScope;
    public List<double> DoublesListForEnumerableScope;
    public List<BlittableJsonReaderObject> BlittableJsonReaderObjectsListForEnumerableScope;
    public List<CoraxSpatialPointEntry> CoraxSpatialPointEntryListForEnumerableScope;
    private HashSet<IndexField> _complexFields;

    private IndexEntryWriter _indexEntryWriter;
    private bool _indexEntryWriterInitialized;
    
    public abstract ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext, object sourceDocument, out LazyStringValue id,
        out ByteString output, out float? documentBoost, out int fields);

    public ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocument(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext, out LazyStringValue id,
        out ByteString output, out float? documentBoost, out bool shouldSkip)
    {
        var currentIndexingScope = CurrentIndexingScope.Current;
        var scope = SetDocumentFields(key, sourceDocumentId, doc, indexContext, currentIndexingScope?.Source, out id, out output, out documentBoost, out var fields);
        if (_fields.Count > 0)
        {
            shouldSkip = _indexEmptyEntries == false && fields <= _numberOfBaseFields; // there is always a key field, but we want to filter-out empty documents, some indexes (e.g. TS indexes contain more than 1 field by default)
        }
        else
        {
            shouldSkip = fields <= 0; // if we have no entries, we might have an index on the id only, so retain it
        }
        return scope;
    }
    
    protected CoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName, string storeValueFieldName, bool canContainSourceDocumentId, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
        _canContainSourceDocumentId = canContainSourceDocumentId;
        Allocator = new ByteStringContext(SharedMultipleUseFlag.None);       
        
        Scope = new();
        _knownFieldsForReaders = new(() =>
        {
            try
            {
                return CoraxIndexingHelpers.CreateMappingWithAnalyzers(Allocator, _index, _index.Definition, _keyFieldName, storeValue, storeValueFieldName, true, _canContainSourceDocumentId);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }
        });
    }
    
    public IndexFieldsMapping GetKnownFieldsForQuerying() => _knownFieldsForReaders.Value;

    private IndexFieldsMapping CreateKnownFieldsForWriter()
    {
        if (KnownFieldsForWriter == null)
        {
            try
            {
                KnownFieldsForWriter = CoraxIndexingHelpers.CreateMappingWithAnalyzers(Allocator, _index, _index.Definition, _keyFieldName, _storeValue, _storeValueFieldName, false, _canContainSourceDocumentId);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }
        }

        return KnownFieldsForWriter;
    }

    protected ref IndexEntryWriter GetEntriesWriter()
    {
        if (_indexEntryWriterInitialized == false)
        {
            _indexEntryWriter = new IndexEntryWriter(Allocator, GetKnownFieldsForWriter());
            _indexEntryWriterInitialized = true;
        }
        
        return ref _indexEntryWriter;
    }

    protected void ResetEntriesWriter()
    {
        if (_indexEntryWriterInitialized)
            _indexEntryWriter.Dispose();
        _indexEntryWriterInitialized = false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IndexFieldsMapping GetKnownFieldsForWriter()
    {
        return KnownFieldsForWriter ?? CreateKnownFieldsForWriter();
    }
    
    protected void InsertRegularField(IndexField field, object value, JsonOperationContext indexContext, ref IndexEntryWriter entryWriter, object sourceDocument,
        IWriterScope scope, out bool shouldSkip, bool nestedArray = false)
    {
        if (_index.Type.IsMapReduce() == false && field.Indexing == FieldIndexing.No && field.Storage == FieldStorage.No && (_complexFields is null || _complexFields.Contains(field) == false))
            ThrowFieldIsNoIndexedAndStored(field);
        
        shouldSkip = false;
        var valueType = GetValueType(value);
        var path = field.Name;
        var fieldId = field.Id;
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
                    scope.Write(path, fieldId, doubleAsString.AsSpan(), @long, @double, ref entryWriter);
                    break;
                }
                else
                {
                    using (Allocator.Allocate(128, out var buffer))
                    {
                        var length = 0;
                        switch (value)
                        {
                            case double d:
                                if (Utf8Formatter.TryFormat(d, buffer.ToSpan(), out length, StandardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                                break;

                            case decimal dm:
                                if (Utf8Formatter.TryFormat(dm, buffer.ToSpan(), out length, StandardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as decimal into bytes.");
                                break;

                            case float f:
                                if (Utf8Formatter.TryFormat(f, buffer.ToSpan(), out length, StandardFormat) == false)
                                    throw new Exception($"Cannot convert {field.Name} as float into bytes.");
                                break;
                        }

                        @long = Convert.ToInt64(value);
                        @double = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                        buffer.Truncate(length);
                        scope.Write(path, fieldId, buffer.ToSpan(), @long, @double, ref entryWriter);
                        break;
                    }
                }

            case ValueType.Numeric:
                var lazyNumber = value as LazyNumberValue;
                if (lazyNumber == null)
                {
                    scope.Write(path, fieldId, lazyNumber.Inner.AsSpan(), (long)value, Convert.ToDouble(value), ref entryWriter);
                    break;
                }

                @long = (long)lazyNumber;
                @double = lazyNumber.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(path, fieldId, lazyNumber.Inner.AsSpan(), @long, @double, ref entryWriter);
                break;

            case ValueType.String:
                scope.Write(path, fieldId, value.ToString(), ref entryWriter);
                break;

            case ValueType.LazyCompressedString:
            case ValueType.LazyString:
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;
                scope.Write(path, fieldId, lazyStringValue.AsSpan(), ref entryWriter);
                break;

            case ValueType.Enum:
                scope.Write(path, fieldId, value.ToString(), ref entryWriter);
                break;

            case ValueType.Boolean:
                scope.Write(path, fieldId, (bool)value ? TrueLiteral : FalseLiteral, ref entryWriter);
                break;

            case ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                scope.Write(path, fieldId, dateAsBytes, dateTime.Ticks, dateTime.Ticks, ref entryWriter);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;

            case ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                scope.Write(path, fieldId, dateTimeOffsetBytes, dateTimeOffset.UtcDateTime.Ticks, dateTimeOffset.UtcDateTime.Ticks, ref entryWriter);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;

            case ValueType.TimeSpan:
                var timeSpan = (TimeSpan)value;
                using (Allocator.Allocate(256, out var buffer))
                {
                    if (Utf8Formatter.TryFormat(timeSpan, buffer.ToSpan(), out var bytesWritten, TimeSpanFormat) == false)
                        throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                    buffer.Truncate(bytesWritten);
                    scope.Write(path, fieldId, buffer.ToSpan(), timeSpan.Ticks, timeSpan.Ticks, ref entryWriter);
                    _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                }

                break;
            
            case ValueType.DateOnly:
                var dateOnly = ((DateOnly)value);
                var dateOnlyTextual = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                var ticks = dateOnly.DayNumber * TimeSpan.TicksPerDay;
                
                scope.Write(path, fieldId, dateOnlyTextual, ticks, ticks, ref entryWriter);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;
            
            case ValueType.TimeOnly:
                var timeOnly = ((TimeOnly)value);
                var timeOnlyTextual = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                scope.Write(path, fieldId, timeOnlyTextual, timeOnly.Ticks, timeOnly.Ticks, ref entryWriter);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);

                break;
            
            case ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                @long = iConvertible.ToInt64(CultureInfo.InvariantCulture);
                @double = iConvertible.ToDouble(CultureInfo.InvariantCulture);

                scope.Write(path, fieldId, iConvertible.ToString(CultureInfo.InvariantCulture), @long, @double, ref entryWriter);
                break;

            case ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                
                var iterator = (IEnumerable)value;

                var canFinishEnumerableWriting = false;
                
                if (scope is not EnumerableWriterScope enumerableWriterScope)
                {
                    canFinishEnumerableWriting = true;
                    enumerableWriterScope = CreateEnumerableWriterScope();
                }
               
                foreach (var item in iterator)
                {
                    InsertRegularField(field, item, indexContext, ref entryWriter, sourceDocument, enumerableWriterScope, out var _, nestedArray);
                }
                
                if (canFinishEnumerableWriting)
                {
                    enumerableWriterScope.Finish(path, fieldId, ref entryWriter);
                }
                
                break;

            case ValueType.DynamicJsonObject:
                if (field.Indexing is not FieldIndexing.No) 
                    AssertOrAdjustIndexingOptionForComplexObject(field);

                if (_index.SourceDocumentIncludedInOutput == false && sourceDocument == value)
                {
                    _index.SourceDocumentIncludedInOutput = true;
                }
                
                var dynamicJson = (DynamicBlittableJson)value;
                scope.Write(path, fieldId, dynamicJson.BlittableJson, ref entryWriter);
                break;

            case ValueType.ConvertToJson:
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (val is not DynamicJsonValue json)
                {
                    InsertRegularField(field, val, indexContext, ref entryWriter, sourceDocument, scope, out shouldSkip, nestedArray);
                    return;
                }

                if (field.Indexing is not FieldIndexing.No)
                {
                    AssertOrAdjustIndexingOptionForComplexObject(field);
                    break;
                }

                var jsonScope = Scope.CreateJson(json, indexContext);
                scope.Write(path, fieldId, jsonScope, ref entryWriter);
                break;

            case ValueType.BlittableJsonObject:
                HandleObject((BlittableJsonReaderObject)value, field, indexContext, ref entryWriter, sourceDocument, scope, out shouldSkip, nestedArray);
                return;

            case ValueType.DynamicNull:       
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull || _indexImplicitNull)
                    scope.WriteNull(path, fieldId, ref entryWriter);
                else
                    shouldSkip = true;
                
                break;

            case ValueType.Null:
                scope.WriteNull(path, fieldId, ref entryWriter);
                break;
            case ValueType.BoostedValue:
                throw new NotSupportedException("Boosting in index is not supported by Corax. You can do it during querying or change index type into Lucene.");
            case ValueType.EmptyString:
                scope.Write(path, fieldId, ReadOnlySpan<byte>.Empty, ref entryWriter);
                break;
            case ValueType.CoraxSpatialPointEntry:
                scope.Write(path, fieldId, (CoraxSpatialPointEntry)value, ref entryWriter);
                break;
            case ValueType.CoraxDynamicItem:
                var cdi = value as CoraxDynamicItem;
                (scope as EnumerableWriterScope)?.SetAsDynamic();
            //we want to unpack item here.
                InsertRegularField(cdi!.Field, cdi.Value, indexContext, ref entryWriter, sourceDocument, scope, out shouldSkip, nestedArray);
                break;
            case ValueType.Stream:
                throw new NotImplementedInCoraxException($"Streams are not implemented in Corax yet");
            case ValueType.Lucene:
                throw new NotSupportedException("The Lucene value type is not supported by Corax. You can change index type into Lucene.");
            default:
                throw new NotImplementedException();
        }
    }

    private static void ThrowFieldIsNoIndexedAndStored(IndexField field)
    {
        throw new InvalidOperationException($"A field `{field.Name}` that is neither indexed nor stored is useless because it cannot be searched or retrieved.");
    }

    private void AssertOrAdjustIndexingOptionForComplexObject(IndexField field)
    {
        Debug.Assert(field.Indexing != FieldIndexing.No, "field.Indexing != FieldIndexing.No");

        if (_index.GetIndexDefinition().Fields.TryGetValue(field.Name, out var fieldFromDefinition) &&
            fieldFromDefinition.Indexing != FieldIndexing.No)
        {
            // We need to disable the complex object handling after we check and then throw. 
            DisableIndexingForComplexObject(field);
            ThrowIndexingComplexObjectNotSupported(field, _index.Type);
        }

        DisableIndexingForComplexObject(field);

    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HandleObject(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, ref IndexEntryWriter entryWriter, object sourceDocument,
        IWriterScope scope, out bool shouldSkip, bool nestedArray = false)
    {
        if (val.TryGetMember(RavenConstants.Json.Fields.Values, out var values) &&
            IsArrayOfTypeValueObject(val))
        {
            InsertRegularField(field, (IEnumerable)values, indexContext, ref entryWriter, sourceDocument, scope, out shouldSkip, nestedArray);
            return;
        }
        
        if (field.Indexing is not FieldIndexing.No) 
            AssertOrAdjustIndexingOptionForComplexObject(field);
        if (GetKnownFieldsForWriter().TryGetByFieldId(field.Id, out var binding))
            binding.SetAnalyzer(null);
        
        scope.Write(field.Name, field.Id, val, ref entryWriter);
        shouldSkip = false;
    }

    private void DisableIndexingForComplexObject(IndexField field)
    {
        field.Indexing = FieldIndexing.No;
        _complexFields ??= new();
        _complexFields.Add(field);
        
        if (GetKnownFieldsForWriter().TryGetByFieldId(field.Id, out var binding))
        {
            binding.OverrideFieldIndexingMode(FieldIndexingMode.No);
        }
    }

    internal static void ThrowIndexingComplexObjectNotSupported(IndexField field, IndexType indexType)
    {
        var fieldName = field.OriginalName ?? field.Name;

        string exceptionMessage;

        if (indexType.IsStatic())
        {
            exceptionMessage =
                $"The value of '{fieldName}' field is a complex object. Indexing it as a text isn't supported and it's supposed to have \\\"Indexing\\\" option set to \\\"No\\\". " +
                $"Note that you can still store it and use it in projections.{Environment.NewLine}" +
                "If you need to use it for searching purposes, you have to call ToString() on the field value in the index definition.";
        }
        else
        {
            exceptionMessage =
                $"The value of '{fieldName}' field is a complex object. Indexing it as a text isn't supported. You should consider querying on individual fields of that object.";
        }
        throw new NotSupportedException(exceptionMessage);
    }

    protected EnumerableWriterScope CreateEnumerableWriterScope()
    {
        if (EnumerableDataStructExist == false)
        {
            StringsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
            LongsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
            DoublesListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
            BlittableJsonReaderObjectsListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
            CoraxSpatialPointEntryListForEnumerableScope = new(InitialSizeOfEnumerableBuffer);
        }
        
        return new EnumerableWriterScope(StringsListForEnumerableScope, LongsListForEnumerableScope, DoublesListForEnumerableScope, CoraxSpatialPointEntryListForEnumerableScope, BlittableJsonReaderObjectsListForEnumerableScope, Allocator);
    }
    
    public override void Dispose()
    {
        _indexEntryWriter.Dispose();
        if (_knownFieldsForReaders.IsValueCreated)
            _knownFieldsForReaders.Value?.Dispose();
        KnownFieldsForWriter?.Dispose();
        Scope?.Dispose();
    }
}
