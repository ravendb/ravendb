using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class ConverterBase : IDisposable
    {
        protected readonly BlittableJsonTraverser _blittableTraverser;
        protected readonly Index _index;
        protected readonly Dictionary<string, IndexField> _fields;
        protected readonly bool _indexImplicitNull;
        protected readonly bool _indexEmptyEntries;
        protected readonly string _keyFieldName;
        protected readonly bool _storeValue;
        protected readonly string _storeValueFieldName;
        protected readonly int _numberOfBaseFields;

        public ConverterBase(Index index, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries, int numberOfBaseFields, string keyFieldName,
            string storeValueFieldName, ICollection<IndexField> fields = null)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
            _blittableTraverser = storeValue ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
            _indexImplicitNull = indexImplicitNull;
            _indexEmptyEntries = indexEmptyEntries;
            _keyFieldName = keyFieldName ??
                            (storeValue ? Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName : Constants.Documents.Indexing.Fields.DocumentIdFieldName);
            _storeValueFieldName = storeValueFieldName;
            _storeValue = storeValue;
            _numberOfBaseFields = numberOfBaseFields;

            if (fields == null)
                fields = index.Definition.IndexFields.Values;

            var dictionary = new Dictionary<string, IndexField>(fields.Count, OrdinalStringStructComparer.Instance);
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

        protected static ValueType GetValueType(object value)
        {
            if (value == null)
                return ValueType.Null;

            if (value is DynamicNullObject)
                return ValueType.DynamicNull;

            var lazyStringValue = value as LazyStringValue;
            if (lazyStringValue != null)
                return lazyStringValue.Size == 0 ? ValueType.EmptyString : ValueType.LazyString;

            var lazyCompressedStringValue = value as LazyCompressedStringValue;
            if (lazyCompressedStringValue != null)
                return lazyCompressedStringValue.UncompressedSize == 0 ? ValueType.EmptyString : ValueType.LazyCompressedString;

            var valueString = value as string;
            if (valueString != null)
                return valueString.Length == 0 ? ValueType.EmptyString : ValueType.String;

            if (value is Enum)
                return ValueType.Enum;

            if (value is bool)
                return ValueType.Boolean;

            if (value is DateTime)
                return ValueType.DateTime;

            if (value is DateTimeOffset)
                return ValueType.DateTimeOffset;

            if (value is TimeSpan)
                return ValueType.TimeSpan;

            if (value is BoostedValue)
                return ValueType.BoostedValue;

            if (value is DynamicBlittableJson)
                return ValueType.DynamicJsonObject;

            if (value is DynamicDictionary)
                return ValueType.ConvertToJson;

            if (value is IEnumerable)
                return ValueType.Enumerable;

            if (value is LazyNumberValue || value is double || value is decimal || value is float)
                return ValueType.Double;

            if (value is AbstractField)
                return ValueType.Lucene;

            if (value is char)
                return ValueType.String;

            if (value is IConvertible)
                return ValueType.Convertible;

            if (value is BlittableJsonReaderObject)
                return ValueType.BlittableJsonObject;

            if (IsNumber(value))
                return ValueType.Numeric;

            if (value is Stream)
                return ValueType.Stream;

            if (value is DateOnly)
                return ValueType.DateOnly;

            if (value is TimeOnly)
                return ValueType.TimeOnly;

            return ValueType.ConvertToJson;
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

            TimeOnly
        }

        protected class ConversionScope : IDisposable
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
