using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.IO;
using Corax.Utils;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Enum = System.Enum;
using Field = Lucene.Net.Documents.Field;
using Type = System.Type;


namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class ConverterBase : IDisposable
    {
        public static readonly BlittableJsonTraverser FlatMapReduceResultsWithTimeOnlyDateOnlySupport = BlittableJsonTraverser.CreateInstanceForIndexing(BlittableJsonTraverser.DefaultFlatMapReduceSeparators, supportTimeOnlyDateOnly: true);
        public static readonly BlittableJsonTraverser DefaultWithTimeOnlyDateOnlySupport =  BlittableJsonTraverser.CreateInstanceForIndexing(supportTimeOnlyDateOnly: true);
        
        protected readonly BlittableJsonTraverser _blittableTraverser;
        protected readonly Index _index;
        protected readonly Dictionary<string, IndexField> _fields;
        protected readonly bool _indexImplicitNull;
        internal readonly bool _indexEmptyEntries;
        protected readonly string _keyFieldName;
        protected readonly bool _storeValue;
        protected readonly string _storeValueFieldName;
        protected readonly int _numberOfBaseFields;

        protected ConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
            string storeValueFieldName, ICollection<IndexField> fields = null)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
            _blittableTraverser = (storeValue, index.Definition.Version) switch
            {
                (storeValue: true, Version: >= IndexDefinitionBaseServerSide.IndexVersion.Base60Version) => FlatMapReduceResultsWithTimeOnlyDateOnlySupport,
                (storeValue: false, Version: >= IndexDefinitionBaseServerSide.IndexVersion.Base60Version) => DefaultWithTimeOnlyDateOnlySupport,
                (storeValue: true, Version: _) => BlittableJsonTraverser.FlatMapReduceResults,
                (storeValue: false, Version: _) => BlittableJsonTraverser.Default
            };
            
            _indexImplicitNull = indexImplicitNull;
            _indexEmptyEntries = indexEmptyEntries;
            _keyFieldName = keyFieldName ??
                            (storeValue ? Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName : Constants.Documents.Indexing.Fields.DocumentIdFieldName);
            _storeValueFieldName = storeValueFieldName;
            _storeValue = storeValue;
            _numberOfBaseFields = numberOfBaseFields;

            if (fields == null)
                fields = index.Definition.IndexFields.Values;

            var dictionary = new Dictionary<string, IndexField>(fields.Count);
            foreach (var field in fields)
                dictionary[field.Name] = field;
            _fields = dictionary;
        }

        protected static bool IsArrayOfTypeValueObject(BlittableJsonReaderObject val)
        {
            foreach (var propertyName in val.GetPropertyNames())
            {
                if (propertyName.Length == 0 || propertyName[0] != '$')
                {
                    return false;
                }
            }

            return true;
        }

        private static readonly TypeCache<ValueType> _valueTypeCache = new(32);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected ValueType GetValueType(object value)
        {
            ValueType GetValueTypeUnlikely(Type type)
            {
                ValueType valueType;
                switch (value)
                {
                    case DynamicNullObject:
                        valueType = ValueType.DynamicNull;
                        break;
                    case LazyStringValue:
                        valueType = ValueType.LazyString;
                        break;
                    case LazyCompressedStringValue:
                        valueType = ValueType.LazyCompressedString;
                        break;
                    case Enum:
                        valueType = ValueType.Enum;
                        break;
                    case BoostedValue:
                        valueType = ValueType.BoostedValue;
                        break;
                    case DynamicDictionary:
                        valueType = ValueType.ConvertToJson;
                        break;
                    case DynamicBlittableJson:
                        valueType = ValueType.DynamicJsonObject;
                        break;
                    case BlittableJsonReaderObject:
                        valueType = ValueType.BlittableJsonObject;
                        break;
                    case CoraxSpatialPointEntry:
                        valueType = ValueType.CoraxSpatialPointEntry;
                        break;
                    case CoraxDynamicItem:
                        valueType = ValueType.CoraxDynamicItem;
                        break;
                    case Stream:
                        valueType = ValueType.Stream;
                        break;
                    case AbstractField:
                        valueType = ValueType.Lucene;
                        break;

                    case bool:
                        valueType = ValueType.Boolean;
                        break;

                    case char:
                        valueType = ValueType.Char;
                        break;
                    
                    case string:
                        valueType = ValueType.String;
                        break;

                    case double:
                    case float:
                    case decimal:
                    case LazyNumberValue:
                        valueType = ValueType.Double;
                        break;

                    case long:
                    case int:
                    case byte:
                    case short:
                    case ushort:
                    case uint:
                    case sbyte:
                    case ulong:
                        valueType = ValueType.Numeric;
                        break;

                    case DateTime:
                        valueType = ValueType.DateTime;
                        break;
                    case DateTimeOffset:
                        valueType = ValueType.DateTimeOffset;
                        break;
                    case TimeSpan:
                        valueType = ValueType.TimeSpan;
                        break;
                    case DateOnly:
                        valueType = ValueType.DateOnly;
                        break;
                    case TimeOnly:
                        valueType = ValueType.TimeOnly;
                        break;
                    
                    case VectorValue:
                        valueType = ValueType.Vector;
                        break;

                    default:
                        if (value is IDictionary)
                            valueType = ValueType.Dictionary;
                        else if (value is IEnumerable)
                            valueType = ValueType.Enumerable;
                        else if (value is IConvertible)
                            valueType = ValueType.Convertible;
                        else
                            valueType = ValueType.ConvertToJson;
                        break;
                }

                _valueTypeCache.Put(type, valueType);

                // We call GetValueType again because by now we know which type it is and we will return immediately
                // after checking in the cache.
                return GetValueType(value);
            }

            if (value == null)
                return ValueType.Null;

            var type = value.GetType();
            if (_valueTypeCache.TryGet(type, out ValueType valueType))
            {
                switch (valueType)
                {
                    case ValueType.String:
                        return ((string)value).Length == 0 ? ValueType.EmptyString : ValueType.String;
                    case ValueType.LazyString:
                        return ((LazyStringValue)value).Length == 0 ? ValueType.EmptyString : ValueType.LazyString;
                    case ValueType.LazyCompressedString:
                        return ((LazyCompressedStringValue)value).UncompressedSize == 0 ? ValueType.EmptyString : ValueType.LazyCompressedString;
                    case ValueType.Dictionary:
                        return (_index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.ProperlyParseDictionaryToStoredField)
                            ? ValueType.Dictionary
                            : ValueType.Enumerable;
                }

                return valueType;
            }

            return GetValueTypeUnlikely(type);
        }

        protected static byte[] ToArray(ConversionScope scope, Stream stream, out int length)
        {
            length = (int)(stream.Length - stream.Position);

            var buffer = scope.RentArray(length);

            using (var ms = new MemoryStream(buffer))
            {
                stream.CopyTo(ms);

                return buffer;
            }
        }

        protected static bool IsNumber(object value)
        {
            return value is long
                   || value is decimal
                   || value is int
                   || value is byte
                   || value is short
                   || value is ushort
                   || value is uint
                   || value is sbyte
                   || value is ulong
                   || value is float
                   || value is double;
        }

        internal static unsafe bool TryToTrimTrailingZeros(LazyNumberValue ldv, JsonOperationContext context, out LazyStringValue dblAsString)
        {
            var dotIndex = ldv.Inner.LastIndexOf(".");
            if (dotIndex <= 0)
            {
                dblAsString = null;
                return false;
            }

            var index = ldv.Inner.Length - 1;
            var anyTrailingZeros = false;
            while (true)
            {
                var lastChar = ldv.Inner[index];
                if (lastChar != '0')
                {
                    if (lastChar == '.')
                        index = index - 1;

                    break;
                }

                anyTrailingZeros = true;
                index = index - 1;
            }

            if (anyTrailingZeros == false)
            {
                dblAsString = null;
                return false;
            }

            dblAsString = context.AllocateStringValue(null, ldv.Inner.Buffer, index + 1);
            return true;
        }

        protected enum ValueType
        {
            Null,

            DynamicNull,

            EmptyString,

            Char,

            String,

            LazyString,

            LazyCompressedString,

            Enumerable,

            Double,

            Convertible,

            Numeric,

            BoostedValue,

            DynamicJsonObject,

            BlittableJsonObject,

            Boolean,

            DateTime,

            DateTimeOffset,

            TimeSpan,

            Enum,

            Lucene,

            ConvertToJson,

            Stream,

            DateOnly,

            TimeOnly,
            
            Vector,
            
            CoraxSpatialPointEntry,
            
            CoraxDynamicItem,
            
            Dictionary
        }

        protected sealed class ConversionScope : IDisposable
        {
            private readonly LinkedList<IDisposable> _toDispose = new LinkedList<IDisposable>();
            private readonly LinkedList<BlittableObjectReader> _readers = new LinkedList<BlittableObjectReader>();
            private readonly LinkedList<byte[]> _arrays = new LinkedList<byte[]>();


            private static readonly byte[] EmptyBuffer = Array.Empty<byte>();

            private bool _storeField;
            private Field _storeValueField;

            public ConversionScope()
            {

            }

            public ConversionScope(bool storeField, Field storeValueField)
            {
                _storeField = storeField;
                _storeValueField = storeValueField;
            }

            public BlittableJsonReaderObject CreateJson(DynamicJsonValue djv, JsonOperationContext context)
            {
                var result = context.ReadObject(djv, "index field as json");

                _toDispose.AddFirst(result);

                return result;
            }

            public void AddToDispose(IDisposable toDispose)
            {
                if (toDispose == null)
                    return;

                _toDispose.AddFirst(toDispose);
            }

            public void Dispose()
            {
                if (_storeField)
                    _storeValueField.SetValue(EmptyBuffer);


                if (_toDispose.Count > 0)
                {
                    foreach (var toDispose in _toDispose)
                        toDispose.Dispose();

                    _toDispose.Clear();
                }

                if (_readers.Count > 0)
                {
                    foreach (var reader in _readers)
                        BlittableObjectReaderPool.Instance.Free(reader);

                    _readers.Clear();
                }

                if (_arrays.Count > 0)
                {
                    foreach (var array in _arrays)
                        ArrayPool<byte>.Shared.Return(array);

                    _arrays.Clear();
                }
            }

            public BlittableObjectReader GetBlittableReader()
            {
                var reader = BlittableObjectReaderPool.Instance.Allocate();
                _readers.AddFirst(reader);

                return reader;
            }

            public byte[] RentArray(int length)
            {
                var array = ArrayPool<byte>.Shared.Rent(length);
                _arrays.AddFirst(array);

                return array;
            }
        }

        public abstract void Dispose();
    }
}
