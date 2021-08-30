using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Corax;
using Raven.Client;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed class CoraxDocumentConverter : ConverterBase, IDisposable
    {
        private readonly BlittableJsonTraverser _blittableTraverser;
        private readonly Dictionary<string, IndexField> _fields;
        private readonly Index _index;
        private readonly ByteStringContext _allocator;
        private readonly Dictionary<Slice, int> _knownFields;
        private static readonly string _trueLiteral = "true";
        private static readonly string _falseLiteral = "false";

        public CoraxDocumentConverter(
                Index index, 
                bool indexImplicitNull = false, 
                bool indexEmptyEntries = true, 
                string keyFieldName = null, 
                bool storeValue = false, 
                string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
            _index = index;
            var fields = index.Definition.IndexFields.Values;
            var dictionary = new Dictionary<string, IndexField>(fields.Count, default(OrdinalStringStructComparer));
            foreach (var field in fields)
                dictionary[field.Name] = field;
            _fields = dictionary;
            _blittableTraverser = storeValue ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
            _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            _knownFields = GetKnownFields();
        }

        public Dictionary<Slice, int> GetKnownFields()
        {
            if (_knownFields != null)
                return _knownFields;
            var knownFields = new Dictionary<Slice, int>();
            foreach (var field in _fields.Values)
            {
                if(field.Name == null) 
                    continue;
                
                Slice.From(_allocator, field.Name, ByteStringType.Immutable, out var value);
                knownFields.Add(value, field.Id);
            }

            return knownFields;
        }
        
        public Span<byte> InsertDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, out string id)
        {
            var document = (Document)doc;
            _allocator.Allocate(document.Data.Size + _fields.Count * 1024, out ByteString buffer);
            var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
            id = document.LowerId.ToLower();
            bool shouldSkip;
            foreach (var indexField in _fields.Values)
            {
                var scope = new CoraxWriterScope(_fields.Count);
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out var value) == true)
                {
                    InsertRegularField(indexField, value, indexContext, out shouldSkip, ref entryWriter, scope);
                }
            }

            //todo maciej try to figure out how should I deal with shouldSkip
            entryWriter.Finish(out var output);
            _allocator.Release(ref buffer);
            return output;
        }
        
        private void InsertRegularField(IndexField field, object value, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, CoraxWriterScope scope, bool nestedArray = false)
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
                        scope.Write(field.Id, doubleAsString.ToString(), @long, @double, ref entryWriter);
                        break;
                    }
                    else
                    {
                        string s = null;
                        switch (value)
                        {
                            case double d:
                                s = d.ToString("G");
                                break;

                            case decimal dm:
                                s = dm.ToString("G");
                                break;

                            case float f:
                                s = f.ToString("G");
                                break;
                        }
                        @long = (long)ldv;
                        @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                        scope.Write(field.Id, s, @long, @double, ref entryWriter);
                        return;
                    }
                    
                case ValueType.Numeric:
                    var lazyNumber = value as LazyNumberValue;
                    if (lazyNumber == null)
                    {
                        scope.Write(field.Id, value.ToString(), (long)value, Convert.ToDouble(value), ref entryWriter);
                        return;
                    }
                    @long = (long)lazyNumber;
                    @double = lazyNumber.ToDouble(CultureInfo.InvariantCulture);

                    scope.Write(field.Id, lazyNumber.ToString(), @long, @double, ref entryWriter);
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
                    scope.Write(field.Id, lazyStringValue.ToString(), ref entryWriter);
                    return;

                    case ValueType.Enum:
                    scope.Write(field.Id, value.ToString(), ref entryWriter);
                    return;

                case ValueType.Boolean:
                    scope.Write(field.Id, (bool)value ? _trueLiteral : _falseLiteral, ref entryWriter);
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
                    scope.Write(field.Id, timeSpan.ToString("c", CultureInfo.InvariantCulture), ref entryWriter);
                    return;

                case ValueType.Convertible:
                    scope.Write(field.Id, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), ref entryWriter);
                    return;

                case ValueType.Enumerable:
                    var iterator = (IEnumerable)value;
                    if (scope.Allocate(field.Id, false) == false)
                        return;
                    foreach (var item in iterator)
                    {
                        InsertRegularField(field, item, indexContext, out _, ref entryWriter, scope);
                    }
                    scope.WriteCollection(field.Id, ref entryWriter);
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

        void HandleArray(IEnumerable itemsToIndex, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, CoraxWriterScope scope, bool nestedArray = false)
        {
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
        void HandleObject(BlittableJsonReaderObject val, IndexField field, JsonOperationContext indexContext, out bool shouldSkip, ref IndexEntryWriter entryWriter, CoraxWriterScope scope, bool nestedArray = false)
        {
            if (val.TryGetMember("$values", out var values) &&
                IsArrayOfTypeValueObject(val))
            {
                HandleArray((IEnumerable)values, field, indexContext, out _, ref entryWriter, scope, true);
            }

            shouldSkip = false;
        }

        public void Dispose()
        {
            _allocator?.Dispose();
        }
    }
}
