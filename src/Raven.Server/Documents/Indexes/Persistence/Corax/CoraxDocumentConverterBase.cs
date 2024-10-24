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
using System.Diagnostics;
using Corax.Mappings;
using Raven.Client.Exceptions.Corax;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Corax.Indexing;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Binary;
using static Raven.Server.Config.Categories.IndexingConfiguration;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxDocumentConverterBase : ConverterBase
{
    protected byte[] _compoundFieldsBuffer;

    private readonly bool _canContainSourceDocumentId;
    private readonly bool _legacyHandlingOfComplexFields;
    private static ReadOnlySpan<byte> TrueLiteral => "true"u8;
    private static ReadOnlySpan<byte> FalseLiteral => "false"u8;

    private static readonly StandardFormat StandardFormat = new('g');
    private static readonly StandardFormat TimeSpanFormat = new('c');

    private readonly ConversionScope Scope;
    private readonly Lazy<IndexFieldsMapping> _knownFieldsForReaders;
    protected IndexFieldsMapping KnownFieldsForWriter;
    protected readonly ByteStringContext Allocator;
    
    public List<ByteString> StringsListForEnumerableScope;
    public List<long> LongsListForEnumerableScope;
    public List<double> DoublesListForEnumerableScope;
    public List<BlittableJsonReaderObject> BlittableJsonReaderObjectsListForEnumerableScope;
    public bool IgnoreComplexObjectsDuringIndex;
    public List<string[]> CompoundFields;
    protected HashSet<string> _nonExistingFieldsOfDocument;

    protected abstract bool SetDocumentFields<TBuilder>(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext,
        TBuilder builder,
        object sourceDocument)
        where TBuilder : IIndexEntryBuilder;

    [SkipLocalsInit]
    public bool SetDocument<TBuilder>(
        LazyStringValue key, LazyStringValue sourceDocumentId,
        object doc, JsonOperationContext indexContext,
        TBuilder builder)
        where TBuilder : IIndexEntryBuilder
    {
        using var _ = Scope; // ensure that we release all the resources generated in SetDocumentFields
        var currentIndexingScope = CurrentIndexingScope.Current;

        return SetDocumentFields(key, sourceDocumentId, doc, indexContext, builder, currentIndexingScope?.Source);
    }
    
    protected CoraxDocumentConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName, string storeValueFieldName, bool canContainSourceDocumentId, ICollection<IndexField> fields = null) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
        keyFieldName, storeValueFieldName, fields)
    {
        _canContainSourceDocumentId = canContainSourceDocumentId;
        _legacyHandlingOfComplexFields = _index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.CoraxComplexFieldIndexingBehavior;

        Allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        
        Scope = new();
        _knownFieldsForReaders = new(() =>
        {
            try
            {
                return CoraxIndexingHelpers.CreateMappingWithAnalyzers(_index, _index.Definition, _keyFieldName, storeValue, storeValueFieldName, true, _canContainSourceDocumentId);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }
        });
        
        CompoundFields = index.GetIndexDefinition().CompoundFields;
        if (CompoundFields != null)
        {
            foreach (string[] compoundField in CompoundFields)
            {
                if (compoundField.Length != 2)
                {
                    throw new NotSupportedInCoraxException("CompoundField must have exactly 2 elements, but got: " + string.Join(",", compoundField));
                }
            }
        }

        _nonExistingFieldsOfDocument = new HashSet<string>();
    }
    
    public IndexFieldsMapping GetKnownFieldsForQuerying() => _knownFieldsForReaders.Value;

    private IndexFieldsMapping CreateKnownFieldsForWriter()
    {
        if (KnownFieldsForWriter == null)
        {
            try
            {
                KnownFieldsForWriter = CoraxIndexingHelpers.CreateMappingWithAnalyzers(_index, _index.Definition, _keyFieldName, _storeValue, _storeValueFieldName, false, _canContainSourceDocumentId);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }
        }

        return KnownFieldsForWriter;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IndexFieldsMapping GetKnownFieldsForWriter()
    {
        return KnownFieldsForWriter ?? CreateKnownFieldsForWriter();
    }

    [SkipLocalsInit]
    protected void InsertRegularField<TBuilder>(IndexField field, object value, JsonOperationContext indexContext, TBuilder builder, object sourceDocument,
        out bool shouldSkip)
        where TBuilder : IIndexEntryBuilder
    {
        if (_index.Type.IsMapReduce() == false && field.Indexing == FieldIndexing.No && field.Storage == FieldStorage.No && (_index.ComplexFieldsNotIndexedByCorax is null || _index.ComplexFieldsNotIndexedByCorax.Contains(field) == false))
            ThrowFieldIsNoIndexedAndStored(field);
        
        shouldSkip = false;
       
        var path = field.Name;
        var fieldId = field.Id;

        long @long;
        double @double;
        ValueType valueType = GetValueType(value);
        switch (valueType)
        {
            case ValueType.Double:
                if (value is LazyNumberValue ldv)
                {
                    if (TryToTrimTrailingZeros(ldv, indexContext, out var doubleAsString) == false)
                        doubleAsString = ldv.Inner;
                    @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                    @long = (long)@double;
                    builder.Write( fieldId, path,doubleAsString.AsSpan(), @long, @double);
                    break;
                }
                else
                {
                    // PERF: Stackalloc when used with SkipLocalsInit would use a `lea` instruction instead of `push 0` repeatedly.
                    // For big stack allocations the usage of SkipLocalsInit is a must unless there is a hard requirement for zeroing the memory. 
                    Span<byte> buffer = stackalloc byte[128];

                    var length = 0;
                    switch (value)
                    {
                        case double d:
                            if (Utf8Formatter.TryFormat(d, buffer, out length, StandardFormat) == false)
                                throw new Exception($"Cannot convert {field.Name} as double into bytes.");
                            @double = d;
                            @long = (long)d;
                            break;
                        case decimal dm:
                            if (Utf8Formatter.TryFormat(dm, buffer, out length, StandardFormat) == false)
                                throw new Exception($"Cannot convert {field.Name} as decimal into bytes.");
                            @double = (double)dm;
                            @long = (long)@double;
                            break;
                        case float f:
                            if (Utf8Formatter.TryFormat(f, buffer, out length, StandardFormat) == false)
                                throw new Exception($"Cannot convert {field.Name} as float into bytes.");
                            @double = f;
                            @long = (long)@double;
                            break;
                        default:
                            @double = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                            @long = (long)@double;
                            break;
                    }
                    builder.Write(fieldId, path, buffer.Slice(0, length), @long, @double);
                    break;
                }

            case ValueType.Numeric:
                switch (value)
                {
                    case long l: @long = l; break;
                    case ulong ul: @long = (long)ul; break;
                    case int i: @long = i; break;
                    case uint ui: @long = ui; break;
                    case short s: @long = s; break;
                    case ushort us: @long = us; break;
                    case byte b: @long = b; break;
                    case sbyte sb: @long = sb; break;
                    default: throw new InvalidOperationException("There shouldn't be other numeric type here.");
                }
                builder.Write(fieldId, path, value.ToString(), @long, @long);
                break;

            case ValueType.Char:
                unsafe
                {
                    char item = (char)value;
                    var span = new ReadOnlySpan<byte>(&item, sizeof(char));
                    builder.Write(fieldId, path, span.Slice(0, span[1] == 0 ? 1 : 2));
                }

                break;

            case ValueType.String:
                builder.Write(fieldId, path, (string)value);
                break;

            case ValueType.LazyCompressedString:
            case ValueType.LazyString:
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;
                ReadOnlySpan<byte> value1 = lazyStringValue.AsSpan();
                builder.Write(fieldId, path, value1);
                break;

            case ValueType.Enum:
                builder.Write( fieldId,path, value.ToString());
                break;

            case ValueType.Boolean:
                ReadOnlySpan<byte> value2 = (bool)value ? TrueLiteral : FalseLiteral;
                builder.Write(fieldId, path, value2);
                break;

            case ValueType.DateTime:
                var dateTime = (DateTime)value;
                var dateAsBytes = dateTime.GetDefaultRavenFormat();
                builder.Write( fieldId,path, dateAsBytes, dateTime.Ticks, dateTime.Ticks);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;

            case ValueType.DateTimeOffset:
                var dateTimeOffset = (DateTimeOffset)value;
                var dateTimeOffsetBytes = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                builder.Write( fieldId,path, dateTimeOffsetBytes, dateTimeOffset.UtcDateTime.Ticks, dateTimeOffset.UtcDateTime.Ticks);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;

            case ValueType.TimeSpan:
            {
                var timeSpan = (TimeSpan)value;

                Span<byte> buffer = stackalloc byte[256];

                if (Utf8Formatter.TryFormat(timeSpan, buffer, out var bytesWritten, TimeSpanFormat) == false)
                    throw new Exception($"Cannot convert {field.Name} as double into bytes.");

                builder.Write(fieldId, path, buffer.Slice(0, bytesWritten), timeSpan.Ticks, timeSpan.Ticks);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);

                break;
            }
            case ValueType.DateOnly:
                var dateOnly = ((DateOnly)value);
                var dateOnlyTextual = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                var ticks = dateOnly.DayNumber * TimeSpan.TicksPerDay;
                
                builder.Write(fieldId, path,  dateOnlyTextual, ticks, ticks);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                break;
            
            case ValueType.TimeOnly:
                var timeOnly = ((TimeOnly)value);
                var timeOnlyTextual = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                builder.Write(fieldId, path,  timeOnlyTextual, timeOnly.Ticks, timeOnly.Ticks);
                _index.IndexFieldsPersistence.MarkHasTimeValue(path);

                break;
            
            case ValueType.Convertible:
                var iConvertible = (IConvertible)value;
                @long = iConvertible.ToInt64(CultureInfo.InvariantCulture);
                @double = iConvertible.ToDouble(CultureInfo.InvariantCulture);

                builder.Write(fieldId, path,  iConvertible.ToString(CultureInfo.InvariantCulture), @long, @double);
                break;
            
            case ValueType.Vector:
                using (var vectorField = (VectorValue)value)
                {
                    switch (field.Vector.IndexingStrategy)
                    {
                        case VectorIndexingStrategy.Exact:
                            builder.WriteExactVector(fieldId, path, vectorField.Embedding.Span);
                            break;
                        case VectorIndexingStrategy.HNSW:
                            throw new NotImplementedException("HNSW is not yet implemented.");
                        default:
                            throw new InvalidDataException($"Unknown vector indexing strategy: '{field.Vector.IndexingStrategy}'.");
                    }
                }
                break;

            case ValueType.Enumerable:
                RuntimeHelpers.EnsureSufficientExecutionStack();
                var iterator = (IEnumerable)value;
                builder.IncrementList();
                bool hasValues = false;
                foreach (var item in iterator)
                {
                    hasValues = true;
                    InsertRegularField(field, item, indexContext, builder, sourceDocument, out _);
                }

                if (hasValues == false && field.Storage == FieldStorage.Yes)
                {
                    builder.RegisterEmptyOrNull(field.Id, field.Name, StoredFieldType.Empty | StoredFieldType.Raw | StoredFieldType.List);
                }
                builder.DecrementList();

                break;

            case ValueType.DynamicJsonObject:
                if (_index.Type.IsAuto())
                {
                    InsertRegularField(field, CoraxConstants.JsonValue, indexContext, builder, sourceDocument, out shouldSkip);
                    _index.SetFieldIsIndexedAsJsonViaCoraxAutoIndex(field);

                    return;
                }

                if (field.Indexing is not FieldIndexing.No)
                {
                    if (_legacyHandlingOfComplexFields)
                        ComplexObjectInStaticIndexLegacyHandling(field);
                    else
                        AssertIndexingBehaviorForComplexObjectInStaticIndex(field);
                }
                
                if (_index.SourceDocumentIncludedInOutput == false && sourceDocument == value)
                {
                    _index.SourceDocumentIncludedInOutput = true;
                }

                if (field.Storage is FieldStorage.Yes || _legacyHandlingOfComplexFields)
                {
                    var dynamicJson = (DynamicBlittableJson)value;
                    builder.Store(fieldId, path, dynamicJson.BlittableJson);
                }

                break;

            case ValueType.Dictionary:
            case ValueType.ConvertToJson:
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (val is not DynamicJsonValue json)
                {
                    InsertRegularField(field, val, indexContext, builder, sourceDocument, out shouldSkip);
                    return;
                }

                var jsonScope = Scope.CreateJson(json, indexContext);
                if (_index.Type.IsAuto())
                {
                    InsertRegularField(field, CoraxConstants.JsonValue, indexContext, builder, sourceDocument, out shouldSkip);
                    _index.SetFieldIsIndexedAsJsonViaCoraxAutoIndex(field);
                    return;
                }
                
                if (field.Indexing is not FieldIndexing.No)
                {
                    if (_legacyHandlingOfComplexFields)
                        ComplexObjectInStaticIndexLegacyHandling(field);
                    else
                        AssertIndexingBehaviorForComplexObjectInStaticIndex(field);
                }

                if (field.Storage is FieldStorage.Yes || _legacyHandlingOfComplexFields) 
                    builder.Store(fieldId, path, jsonScope);

                break;

            case ValueType.BlittableJsonObject:
                HandleObject((BlittableJsonReaderObject)value, field, indexContext, builder, sourceDocument,out shouldSkip);
                return;

            case ValueType.DynamicNull:       
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull || _indexImplicitNull)
                    builder.WriteNull(fieldId, path);
                else
                    shouldSkip = true;
                break;
            case ValueType.Null:
                builder.WriteNull(fieldId, path);
                break;
            case ValueType.BoostedValue:
                throw new NotSupportedInCoraxException("Boosting in index is not supported by Corax. You can do it during querying or change index type into Lucene.");
            case ValueType.EmptyString:
                builder.Write(fieldId, path, ReadOnlySpan<byte>.Empty);
                break;
            case ValueType.CoraxSpatialPointEntry:
                builder.WriteSpatial(fieldId, path,  (CoraxSpatialPointEntry)value);
                break;
            case ValueType.CoraxDynamicItem:
                var old = builder.ResetList(); // For lists of CreatedField(), we ignoring the list
                if (value is CoraxDynamicItem standardCdi)
                    InsertRegularField(standardCdi.Field, standardCdi.Value, indexContext, builder, sourceDocument, out shouldSkip);
                else
                    throw new NotSupportedInCoraxException($"Unknown path for `CoraxDynamicItem`. Got type: {value.GetType().FullName}");
                
                //we want to unpack item here.
                builder.RestoreList(old);
                break;
            case ValueType.Stream:
                throw new NotImplementedInCoraxException($"Streams are not implemented in Corax yet");
            case ValueType.Lucene:
                throw new NotSupportedInCoraxException("The Lucene value type is not supported by Corax. You can change index type into Lucene.");
            default:
                throw new NotSupportedException(valueType + " is not a supported type for indexing");
        }
    }

    protected void RegisterMissingFieldFor(IndexField field)
    {
        if (field.Id == CoraxConstants.IndexWriter.DynamicField || 
            IndexDefinitionBaseServerSide.IndexVersion.IsNonExistingPostingListSupported(_index.Definition.Version) == false)
            return;
        
        _nonExistingFieldsOfDocument.Add(field.Name);
    }

    protected void WriteNonExistingMarkerForMissingFields<TBuilder>(TBuilder builder) where TBuilder : IIndexEntryBuilder
    {
        if (IndexDefinitionBaseServerSide.IndexVersion.IsNonExistingPostingListSupported(_index.Definition.Version) == false) 
            return;

        foreach (var fieldName in _nonExistingFieldsOfDocument)
        {
            var path = _fields[fieldName].Name;
            var fieldId= _fields[fieldName].Id;
        
            builder.WriteNonExistingMarker(fieldId, path);
        }
            
        _nonExistingFieldsOfDocument.Clear();
    }
    
    [DoesNotReturn]
    private static void ThrowFieldIsNoIndexedAndStored(IndexField field)
    {
        throw new InvalidOperationException($"A field `{field.Name}` that is neither indexed nor stored is useless because it cannot be searched or retrieved.");
    }

    private void AssertIndexingBehaviorForComplexObjectInStaticIndex(IndexField field)
    {
        Debug.Assert(_index.Type.IsStatic(), $"It is is supposed to be called for static indexes only while we got {_index.Type} index");

        if (IgnoreComplexObjectsDuringIndex)
            return;

        switch (_index.CoraxComplexFieldIndexingBehavior)
        {
            case CoraxComplexFieldIndexingBehavior.Throw:
                ThrowIndexingComplexObjectNotSupportedInStaticIndex(field, _index.Definition.Version);
                break;
            case CoraxComplexFieldIndexingBehavior.Skip:
                // static Corax indexes don't support indexing of complex objects, so we're not going to index it anyway
                break;
            default:
                throw new NotSupportedException(
                    $"Unknown {nameof(CoraxComplexFieldIndexingBehavior)} option: {_index.CoraxComplexFieldIndexingBehavior}");
        }
    }

    private void ComplexObjectInStaticIndexLegacyHandling(IndexField field)
    {
        // Backward compatibility for older indexes
        // Previously, we silently changed the definition not to throw when encountering a complex field without any particular configuration.

        if (IgnoreComplexObjectsDuringIndex) 
            return;

        Debug.Assert(_index.Type.IsStatic(), "Legacy complex field handling is supposed to be called for static indexes");
        Debug.Assert(field.Indexing != FieldIndexing.No, "field.Indexing != FieldIndexing.No");
        Debug.Assert(_index.Definition.Version < IndexDefinitionBaseServerSide.IndexVersion.CoraxComplexFieldIndexingBehavior, "Legacy complex field handling is supposed to be called only for old indexes");

        // Skip indexing of this complex field for current and next entries by overwriting field.Indexing option by FieldIndexing.No

        DisableIndexingForComplexObjectLegacyHandling(field);

        if (_index.GetIndexDefinition().Fields.TryGetValue(field.Name, out var fieldFromDefinition) &&
            fieldFromDefinition.Indexing is { } and not FieldIndexing.No)
        {
            // Indexing option was set explicitly in the definition. We'll throw in that case but only for the first encountered entry. 
            // The next ones will not index this field at all because we just disabled indexing for that field
            
            ThrowIndexingComplexObjectNotSupportedInStaticIndex(field, _index.Definition.Version);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HandleObject<TBuilder>(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, TBuilder builder, object sourceDocument, out bool shouldSkip)
        where TBuilder : IIndexEntryBuilder
    {
        if (val.TryGetMember(RavenConstants.Json.Fields.Values, out var values) &&
            IsArrayOfTypeValueObject(val))
        {
            InsertRegularField(field, (IEnumerable)values, indexContext, builder, sourceDocument, out shouldSkip);
            return;
        }

        if (_index.Type.IsAuto())
        {
            _index.SetFieldIsIndexedAsJsonViaCoraxAutoIndex(field);
            InsertRegularField(field, CoraxConstants.JsonValue, indexContext, builder, sourceDocument, out shouldSkip);
            return;
        }

        if (field.Indexing is not FieldIndexing.No)
        {
            if (_legacyHandlingOfComplexFields)
                ComplexObjectInStaticIndexLegacyHandling(field);
            else
                AssertIndexingBehaviorForComplexObjectInStaticIndex(field);
        }
        
        if (GetKnownFieldsForWriter().TryGetByFieldId(field.Id, out var binding))
            binding.SetAnalyzer(null);

        if (field.Storage is FieldStorage.Yes || _legacyHandlingOfComplexFields)
        {
            if (val.HasParent)
            {
                using var clonedBlittable = val.CloneOnTheSameContext();
                builder.Store(field.Id, field.Name, clonedBlittable);
            }
            else
            {
                builder.Store(field.Id, field.Name, val);
            }
        }

        shouldSkip = false;
    }

    private void DisableIndexingForComplexObjectLegacyHandling(IndexField field)
    {
        field.Indexing = FieldIndexing.No;
       _index.SetComplexFieldNotIndexedByCoraxStaticIndex(field);
        
        if (GetKnownFieldsForWriter().TryGetByFieldId(field.Id, out var binding))
        {
            binding.OverrideFieldIndexingMode(FieldIndexingMode.No);
        }
    }

    [DoesNotReturn]
    internal static void ThrowIndexingComplexObjectNotSupportedInStaticIndex(IndexField field, long indexVersion)
    {
        var fieldName = field.OriginalName ?? field.Name;

        StringBuilder exceptionMessage = new();

        exceptionMessage.AppendLine(
            $"The value of '{fieldName}' field is a complex object. Typically a complex field is not intended to be indexed as as whole hence indexing it as a text isn't supported in Corax. " +
            $"The field is supposed to have 'Indexing' option set to 'No' (note that you can still store it and use it in projections). ");

        if (indexVersion >= IndexDefinitionBaseServerSide.IndexVersion.CoraxComplexFieldIndexingBehavior)
        {
            exceptionMessage.AppendLine(
                $"Alternatively you can switch '{RavenConfiguration.GetKey(x => x.Indexing.CoraxStaticIndexComplexFieldIndexingBehavior)}' configuration option from " +
                $"'{CoraxComplexFieldIndexingBehavior.Throw}' to '{CoraxComplexFieldIndexingBehavior.Skip}' to disable the indexing of all complex fields in the index or globally for all indexes (index reset is required). ");
        }

        exceptionMessage.Append(
            "If you really need to use this field for searching purposes, you have to call ToString() on the field value in the index definition. Although it's recommended to index individual fields of this complex object. " +
            "Read more at: https://ravendb.net/l/OB9XW4/6.2");

        throw new NotSupportedInCoraxException(exceptionMessage.ToString());
    }
    
    protected int AppendFieldValue<TBuilder>(string field, object v, int index, TBuilder builder)         
    where TBuilder : IIndexEntryBuilder
    {        

        var indexFieldId = _index.Definition.IndexFields[field].Id;

        var initialIndex = index;
        ValueType valueType = GetValueType(v);
        switch (valueType)
        {
            case ValueType.EmptyString:
            case ValueType.DynamicNull:
            case ValueType.Null:
                // for nulls, we put no value and it will sort at the beginning
                break;
            case ValueType.Enum:
            case ValueType.String:
            {
                string s = v.ToString();
                var buffer = EnsureHasSpace(Encoding.UTF8.GetMaxByteCount(s.Length));
                var bytes = Encoding.UTF8.GetBytes(s, buffer[index..]);
                index += AppendAnalyzedTerm(buffer.Slice(index, bytes));
                break;
            }
            case ValueType.LazyString:
                var lazyStringValue = ((LazyStringValue)v);
                EnsureHasSpace(lazyStringValue.Length);
                index += AppendAnalyzedTerm(lazyStringValue.AsSpan());
                break;
            case ValueType.LazyCompressedString:
                v = ((LazyCompressedStringValue)v).ToLazyStringValue();
                goto case ValueType.LazyString;
            case ValueType.DateTime:
                AppendLong(((DateTime)v).Ticks);
                break;
            case ValueType.DateTimeOffset:
                AppendLong(((DateTimeOffset)v).Ticks);
                break;
            case ValueType.DateOnly:
                AppendLong(((DateOnly)v).ToDateTime(TimeOnly.MinValue).Ticks);
                break;
            case ValueType.TimeOnly:
                AppendLong(((TimeOnly)v).Ticks);
                break;
            case ValueType.TimeSpan:
                AppendLong(((TimeSpan)v).Ticks);
                break;
            case ValueType.Convertible:
                v = Convert.ToDouble(v);
                goto case ValueType.Double;
            case ValueType.Boolean:
                EnsureHasSpace(1);
                _compoundFieldsBuffer[index++] = (bool)v ? (byte)1 : (byte)0;
                break;
            case ValueType.Numeric:
                var l = v switch
                {
                    long ll => ll,
                    ulong ul => (long)ul,
                    int ii => ii,
                    short ss => ss,
                    ushort us => us,
                    byte b => b,
                    sbyte sb => sb,
                    _ => Convert.ToInt64(v)
                };
                AppendLong(l);
                break;
            case ValueType.Char:
                unsafe
                {
                    char value = (char)v;
                    var span = new ReadOnlySpan<byte>((byte*)&value, sizeof(char));
                    span = span[1] == 0 ? span : span.Slice(0, 1);
                    EnsureHasSpace(span.Length);
                    span.CopyTo(_compoundFieldsBuffer.AsSpan(index));
                    index += span.Length;
                }
                break;
            case ValueType.Double:
                var d = v switch
                {
                    LazyNumberValue ldv => ldv.ToDouble(CultureInfo.InvariantCulture),
                    double dd => dd,
                    float f => f,
                    decimal m => (double)m,
                    _ => Convert.ToDouble(v)
                };
                AppendLong(Bits.DoubleToSortableLong(d));
                break;
            default:
                throw new NotSupportedException(
                    $"Unable to create compound index with value of type: {valueType} ({v}) for compound field: {field}");
        }

        int termLen = index - initialIndex;
        if(termLen > byte.MaxValue)
            throw new ArgumentOutOfRangeException($"Unable to create compound index with value of type: {valueType} ({v}) for compound field: {field} because it exceeded the 256 max size for a compound index value (was {termLen}).");

        EnsureHasSpace(1); // for the len
        return index;


        Span<byte> EnsureHasSpace(int additionalSize)
        {
            if (additionalSize + index >= _compoundFieldsBuffer.Length)
            {
                var newSize = Bits.PowerOf2(additionalSize + index + 1);
                Array.Resize(ref _compoundFieldsBuffer, newSize);
            }

            return _compoundFieldsBuffer;
        }
        

        int AppendAnalyzedTerm(Span<byte> term)
        {
            var analyzedTerm = builder.AnalyzeSingleTerm(indexFieldId, term);
            var buffer = EnsureHasSpace(analyzedTerm.Length - term.Length); // just in case the analyze is _larger_
            analyzedTerm.CopyTo(buffer[index..]);
            return analyzedTerm.Length;
        }

        void AppendLong(long l)
        {
            EnsureHasSpace(sizeof(long));
            BitConverter.TryWriteBytes(_compoundFieldsBuffer.AsSpan()[index..], Bits.SwapBytes(l));
            index += sizeof(long);
        }
    }

    [DoesNotReturn]
    protected void ThrowFieldInCompoundFieldNotFound(string field)
    {
        throw new InvalidDataException($"Field '{field}' not found in map. Cannot create compound field.");
    }
    
    public override void Dispose()
    {
        if (_knownFieldsForReaders.IsValueCreated)
            _knownFieldsForReaders.Value?.Dispose();
        KnownFieldsForWriter?.Dispose();
        Scope?.Dispose();
    }
}
