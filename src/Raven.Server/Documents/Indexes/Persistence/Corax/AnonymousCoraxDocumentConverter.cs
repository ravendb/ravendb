using System;
using System.Globalization;
using Amazon.SimpleNotificationService.Model;
using Corax;
using Parquet.Meta;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server.Utils;
using ZstdSharp.Unsafe;
using Constants = Raven.Client.Constants;
using Encoding = System.Text.Encoding;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class AnonymousCoraxDocumentConverter : AnonymousCoraxDocumentConverterBase
{
    public AnonymousCoraxDocumentConverter(Index index, bool storeValue = false) : base(index, numberOfBaseFields: 1, storeValue: storeValue)
    {
    }
}

public abstract class AnonymousCoraxDocumentConverterBase : CoraxDocumentConverterBase
{
    private readonly bool _isMultiMap;
    private IPropertyAccessor _propertyAccessor;
    private byte[] _compoundFieldsBuffer;

    public AnonymousCoraxDocumentConverterBase(Index index, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, bool canContainSourceDocumentId = false) : base(index, storeValue, indexImplicitNull: index.Configuration.IndexMissingFieldsAsNull, index.Configuration.IndexEmptyEntries, 1, keyFieldName, Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, canContainSourceDocumentId)
    {
        _isMultiMap = index.IsMultiMap;
    }

    protected override bool SetDocumentFields<TBuilder>(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, TBuilder builder,
        object sourceDocument)
    {
        var boostedValue = doc as BoostedValue;
        var documentToProcess = boostedValue == null ? doc : boostedValue.Value;
        
        // It is important to note that as soon as an accessor is created this instance is tied to the underlying property type.
        // This optimization is not able to handle differences in types for the same property. Therefore, this instances cannot
        // be reused for Map and Reduce documents at the same time. You need a new instance to do so. 
        IPropertyAccessor accessor;
        if (_isMultiMap == false)
            accessor = _propertyAccessor ??= PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess);
        else
            accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

        var storedValue = _storeValue ? new DynamicJsonValue() : null;

        var knownFields = GetKnownFieldsForWriter();

        // We prepare for the next entry.
        if (boostedValue != null)
            builder.Boost(boostedValue.Boost);

        if (CompoundFields != null)
            HandleCompoundFields();

        bool hasFields = false;
        foreach (var property in accessor.GetProperties(documentToProcess))
        {
            var value = property.Value;

            if (_fields.TryGetValue(property.Key, out var field) == false)
                throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.");

                
            InsertRegularField(field, value, indexContext, builder, sourceDocument, out var innerShouldSkip);
            hasFields |= innerShouldSkip == false;
                
            if (storedValue is not null && innerShouldSkip == false)
            {
                //Notice: we are always saving values inside Corax index. This method is explicitly for MapReduce because we have to have JSON as the last item.
                var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                if (returnType != TypeConverter.BlittableSupportedReturnType.Ignored)
                    storedValue[property.Key] = blittableValue;
            }
        }

        if (hasFields is false && _indexEmptyEntries is false)
            return false;

        if (storedValue is not null)
        {
            using var bjo = indexContext.ReadObject(storedValue, "corax field as json");
            builder.Store(bjo);
        }
            
        var id = key ?? throw new InvalidParameterException("Cannot find any identifier of the document.");
        if (sourceDocumentId != null && knownFields.TryGetByFieldName(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, out var documentSourceField))
            builder.Write(documentSourceField.FieldId, string.Empty, sourceDocumentId.AsSpan());

        builder.Write(0, string.Empty, id.AsSpan());
        
        return true;
        
        
        void HandleCompoundFields()
        {
            _compoundFieldsBuffer ??= new byte[128];
            var baseLine = _index.Definition.IndexFields.Count;
            for (int i = 0; i < CompoundFields.Count; i++)
            {
                int index = 0;
                var fields = CompoundFields[i];
                for (int j = 0; j < fields.Length; j++)
                {
                    object v = accessor.GetValue(fields[j], doc);
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
                            string s = v.ToString();
                            EnsureHasSpace(Encoding.UTF8.GetMaxByteCount(s.Length));
                            index += Encoding.UTF8.GetBytes(s, _compoundFieldsBuffer.AsSpan()[index..]);
                            break;
                        case ValueType.LazyString:
                            var lazyStringValue = ((LazyStringValue)v);
                            EnsureHasSpace(lazyStringValue.Length);
                            lazyStringValue.AsSpan().CopyTo(_compoundFieldsBuffer.AsSpan()[index..]);
                            index += lazyStringValue.Length;
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
                            EnsureHasSpace(sizeof(char));
                            BitConverter.TryWriteBytes(_compoundFieldsBuffer.AsSpan()[index..], (char)v);
                            index += sizeof(char);
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
                            AppendLong(DoubleToSortableLong(d));
                            break;
                        default:
                            throw new NotSupportedException(
                                $"Unable to create compound index with value of type: {valueType} ({v}) for compound field: {string.Join(",", CompoundFields[i])}");
                    }
                    _compoundFieldsBuffer[index++] = SpecialChars.RecordSeparator;
                }

                var fieldId = baseLine + i;
                builder.Write( fieldId, _compoundFieldsBuffer.AsSpan()[..index]);

                long DoubleToSortableLong(double val)
                {
                    long f = BitConverter.DoubleToInt64Bits(val);
                    if (f < 0)
                        f ^= 0x7fffffffffffffffL;
                    return f;
                }
                void EnsureHasSpace(int additionalSize)
                {
                    if (additionalSize + index < _compoundFieldsBuffer.Length)
                        return;

                    var newSize = Bits.PowerOf2(additionalSize + index + 1);
                    Array.Resize(ref _compoundFieldsBuffer, newSize);
                }

                void AppendLong(long l)
                {
                    EnsureHasSpace(sizeof(long));
                    BitConverter.TryWriteBytes(_compoundFieldsBuffer.AsSpan()[index..], Bits.SwapBytes(l));
                    index += sizeof(long);
                }
            }

            if (_compoundFieldsBuffer.Length > 64 * 1024)
            {
                // let's ensure that we aren't holding on to really big buffers forever
                _compoundFieldsBuffer = null;
            }
        }
        
    }
}
