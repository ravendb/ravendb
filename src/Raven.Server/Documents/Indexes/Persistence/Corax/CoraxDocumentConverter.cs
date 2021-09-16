using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Corax;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Encoding = System.Text.Encoding;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed class CoraxDocumentConverter : ConverterBase
    {
        private readonly ByteStringContext _allocator;
        private readonly List<IDisposable> _slicesToDispose;
        private readonly Dictionary<Slice, int> _knownFields;
        private static readonly Memory<byte> _trueLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("true"));
        private static readonly Memory<byte> _falseLiteral = new Memory<byte>(Encoding.UTF8.GetBytes("false"));
        private static readonly StandardFormat _standardFormat = new StandardFormat('g');
        private static readonly StandardFormat _timeSpanFormat = new StandardFormat('c');
        public CoraxDocumentConverter(
                Index index, 
                bool indexImplicitNull = false, 
                bool indexEmptyEntries = true, 
                string keyFieldName = null,
                bool storeValue = false, 
                string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName) :
            base(index, storeValue, indexImplicitNull, indexEmptyEntries, 1, keyFieldName, storeValueFieldName)
        {
            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            _slicesToDispose = new();
            _knownFields = GetKnownFields();
        }

        public Dictionary<Slice, int> GetKnownFields()
        {
            if (_knownFields != null)
                return _knownFields;
            var knownFields = new Dictionary<Slice, int>();
            
            //todo maciej: will optimize it while doing static indexes.
            _slicesToDispose.Add(Slice.From(_allocator, _index.Type.IsMapReduce() ? Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName : Constants.Documents.Indexing.Fields.DocumentIdFieldName, ByteStringType.Immutable, out var value));
            knownFields.Add(value, 0);
            foreach (var field in _fields.Values)
            {
                _slicesToDispose.Add(Slice.From(_allocator, field.Name, ByteStringType.Immutable, out value));
                //offset for ID()
                knownFields.Add(value, field.Id);
            }

            return knownFields;
        }
        
        public Span<byte> InsertDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, out LazyStringValue id)
        {
            var document = (Document)doc;
            using ( _allocator.Allocate(document.Data.Size * 2, out ByteString buffer))
            {
                var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
                id = document.LowerId ?? key;
                bool shouldSkip;
                
                
                //TODO maciej - please look at this.
                // this is reference to list for EnumerableWritingScope due to making it persistence during indexing document. 
                // We want avoid allocation for every enumerable in doc, but if think it should be persistence during whole indexing process.
                // Where should we put this and when release it?
                List<int> stringsLength = new List<int>(128);
                var scope = new SingleEntryWriterScope(stringsLength, _allocator);
                scope.Write(0, id.AsSpan(), ref entryWriter);
                foreach (var indexField in _fields.Values)
                {
                    if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out var value))
                    {
                        InsertRegularField(indexField, value, indexContext, out shouldSkip, ref entryWriter, scope);
                    }
                }

                //todo maciej try to figure out how should I deal with shouldSkip
                entryWriter.Finish(out var output);
                return output;
            }
        }
        
        private void InsertRegularField(IndexField field, object value, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, IWriterScope scope, bool nestedArray = false)
        {
            var path = field.Name;
            var valueType = GetValueType(value);
            shouldSkip = false;
            long @long;
            double @double;
            
            switch (valueType)
            {
                case ValueType.Double:
                    var ldv = value as LazyNumberValue;
                    if (ldv != null)
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

                            @long = (long)ldv;
                            @double = ldv.ToDouble(CultureInfo.InvariantCulture);
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
                    using (_allocator.Allocate(256 ,out var buffer))
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
                    var enumerableScope = new EnumerableWriterScope(field.Id, ref entryWriter, scope.GetLengthList(), _allocator);
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
                    if (!(val is DynamicJsonValue json))
                        InsertRegularField(field, val, indexContext, out _, ref entryWriter, scope, nestedArray);
                    return;

                case ValueType.BlittableJsonObject:
                    HandleObject((BlittableJsonReaderObject)value, field, indexContext, out _, ref entryWriter, scope);
                    return; 

                case ValueType.BoostedValue:
                case ValueType.Stream:
                case ValueType.DynamicNull:
                case ValueType.Null:
                default:
                    throw new NotImplementedException();
            }

            shouldSkip = true;
        }

        void HandleArray(IEnumerable itemsToIndex, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, IWriterScope scope, bool nestedArray = false)
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
        void HandleObject(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, IWriterScope scope, bool nestedArray = false)
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
            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxDocumentConverter)} of {_index.Name}");
            foreach(var disposableItem in _slicesToDispose)
            {
                exceptionAggregator.Execute(() =>
                {
                    disposableItem?.Dispose();
                });
            }
            exceptionAggregator.Execute(() =>
            {
                _allocator?.Dispose();
            });
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
