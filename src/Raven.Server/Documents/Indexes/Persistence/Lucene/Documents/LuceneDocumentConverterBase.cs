using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using LuceneDocument = Lucene.Net.Documents.Document;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public interface ILuceneDocumentWrapper
    {
        void Add(AbstractField field);

        IList<IFieldable> GetFields();
    }

    public abstract class LuceneDocumentConverterBase : ConverterBase
    {
        protected const float LuceneDefaultBoost = 1f;
        
        public struct DefaultDocumentLuceneWrapper : ILuceneDocumentWrapper
        {
            private readonly LuceneDocument _doc;

            public DefaultDocumentLuceneWrapper(LuceneDocument doc)
            {
                _doc = doc;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(AbstractField field)
            {
                _doc.Add(field);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IList<IFieldable> GetFields()
            {
                return _doc.GetFields();
            }
        }

        internal const string IsArrayFieldSuffix = "_IsArray";

        internal const string ConvertToJsonSuffix = "_ConvertToJson";

        internal const string TrueString = "true";

        internal const string FalseString = "false";

        private readonly Field _storeValueField;

        protected readonly ConversionScope Scope;

        internal static readonly int MaximumNumberOfItemsInFieldsCacheForMultipleItemsSameField = PlatformDetails.Is32Bits == false ? 8 * 1024 : 2 * 1024;

        private int _numberOfItemsInFieldsCacheForMultipleItemsSameField;

        private int _numberOfItemsInNumericFieldsCacheForMultipleItemsSameField;

        private Dictionary<int, CachedFieldItem<Field>> _fieldsCache = new();

        private Dictionary<int, CachedFieldItem<NumericField>> _numericFieldsCache = new ();

        public readonly LuceneDocument Document = new LuceneDocument();

        private readonly List<int> _multipleItemsSameFieldCount = new List<int>();
        
        public void Clean()
        {
            if (_fieldsCache.Count > 256)
            {
                var fieldsCache = _fieldsCache;
                _fieldsCache = new Dictionary<int, CachedFieldItem<Field>>();
                _numberOfItemsInFieldsCacheForMultipleItemsSameField = 0;

                ClearFieldCache(fieldsCache);
            }

            if (_numericFieldsCache.Count > 256)
            {
                var fieldsCache = _numericFieldsCache;
                _numericFieldsCache = new Dictionary<int, CachedFieldItem<NumericField>>();
                _numberOfItemsInNumericFieldsCacheForMultipleItemsSameField = 0;

                ClearFieldCache(fieldsCache);
            }
        }

        protected LuceneDocumentConverterBase(
            Index index,
            bool indexEmptyEntries,
            int numberOfBaseFields,
            string keyFieldName = null,
            bool storeValue = false,
            string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : this(
                  index,
                  index.Definition.IndexFields.Values,
                  index.Configuration.IndexMissingFieldsAsNull,
                  indexEmptyEntries,
                  numberOfBaseFields,
                  keyFieldName,
                  storeValue,
                  storeValueFieldName)
        {
        }

        protected LuceneDocumentConverterBase(
             Index index,
             ICollection<IndexField> fields,
             bool indexImplicitNull,
             bool indexEmptyEntries,
             int numberOfBaseFields,
             string keyFieldName = null,
             bool storeValue = false,
             string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName) : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields, keyFieldName, storeValueFieldName, fields)
        {
            _storeValueField = new Field(storeValueFieldName, Array.Empty<byte>(), 0, 0, Field.Store.YES);

            foreach ((var name, var field) in _fields)
            {
                if (field.Vector)
                    throw new NotSupportedException("Vector Search is enabled only for Corax indexes, not Lucene. But was asked to index " + name + " as a vector");
            }
            
            Scope = new (storeValue, _storeValueField);
        }

        // returned document needs to be written do index right after conversion because the same cached instance is used here
        public IDisposable SetDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, JsonOperationContext indexContext, IWriteOperationBuffer writeBuffer, out bool shouldSkip)
        {
            Document.GetFields().Clear();

            var scope = CurrentIndexingScope.Current;
            if (scope != null)
                scope.IncrementDynamicFields();

            int numberOfFields = GetFields(new DefaultDocumentLuceneWrapper(Document), key, sourceDocumentId, document, indexContext, writeBuffer, scope?.Source);
            if (_fields.Count > 0)
            {
                shouldSkip = _indexEmptyEntries == false && numberOfFields <= _numberOfBaseFields; // there is always a key field, but we want to filter-out empty documents, some indexes (e.g. TS indexes contain more than 1 field by default)
            }
            else
            {
                shouldSkip = numberOfFields <= 0; // if we have no entries, we might have an index on the id only, so retain it
            }
            return Scope;
        }

        protected abstract int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object document, JsonOperationContext indexContext,
            IWriteOperationBuffer writeBuffer, object sourceDocument) where T : ILuceneDocumentWrapper;

        /// <summary>
        /// This method generate the fields for indexing documents in lucene from the values.
        /// Given a name and a value, it has the following behavior:
        /// * If the value is enumerable, index all the items in the enumerable under the same field name
        /// * If the value is null, create a single field with the supplied name with the unanalyzed value 'NULL_VALUE'
        /// * If the value is string or was set to not analyzed, create a single field with the supplied name
        /// * If the value is date, create a single field with millisecond precision with the supplied name
        /// * If the value is numeric (int, long, double, decimal, or float) will create two fields:
        ///		1. with the supplied name, containing the numeric value as an unanalyzed string - useful for direct queries
        ///		2. with the name: name +'_Range', containing the numeric value in a form that allows range queries
        /// </summary>
        public int GetRegularFields<T>(T instance, IndexField field, object value, JsonOperationContext indexContext, object sourceDocument, out bool shouldSkip,
            bool nestedArray = false) where T : ILuceneDocumentWrapper
        {
            var path = field.Name;

            var valueType = GetValueType(value);

            Field.Index defaultIndexing;
            switch (valueType)
            {
                case ValueType.LazyString:
                case ValueType.LazyCompressedString:
                case ValueType.Char:
                case ValueType.String:
                case ValueType.Enum:
                case ValueType.Stream:
                    defaultIndexing = Field.Index.ANALYZED;
                    break;

                case ValueType.Dictionary:
                    defaultIndexing = Field.Index.NOT_ANALYZED_NO_NORMS; // RavenDB-19560
                    break;

                case ValueType.DateOnly:
                case ValueType.TimeOnly:
                case ValueType.DateTime:
                case ValueType.DateTimeOffset:
                case ValueType.TimeSpan:
                case ValueType.Boolean:
                case ValueType.Double:
                case ValueType.Null:
                case ValueType.DynamicNull:
                case ValueType.EmptyString:
                case ValueType.Numeric:
                case ValueType.BlittableJsonObject:
                case ValueType.DynamicJsonObject:
                    defaultIndexing = Field.Index.NOT_ANALYZED_NO_NORMS;
                    break;

                default:
                    defaultIndexing = Field.Index.ANALYZED_NO_NORMS;
                    break;
            }

            var indexing = field.Indexing.GetLuceneValue(field.Analyzer, @default: defaultIndexing);
            var storage = field.Storage.GetLuceneValue();
            var termVector = field.TermVector.GetLuceneValue();

            shouldSkip = false;
            int newFields = 0;

            if (_storeValue && indexing == Field.Index.NO && storage == Field.Store.NO)
                return newFields;

            if (valueType == ValueType.Null)
            {
                instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.NullValue, null, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                newFields++;

                return newFields;
            }

            if (valueType == ValueType.DynamicNull)
            {
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull || _indexImplicitNull)
                {
                    instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.NullValue, null, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                    newFields++;
                }

                shouldSkip = newFields == 0;
                return newFields;
            }

            if (valueType == ValueType.EmptyString)
            {
                instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.EmptyString, null, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                newFields++;
                return newFields;
            }

            if (valueType is ValueType.String or ValueType.Char)
            {
                string stringValue = value as string ?? value.ToString();

                instance.Add(GetOrCreateField(path, stringValue, null, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.LazyString || valueType == ValueType.LazyCompressedString)
            {
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;

                instance.Add(GetOrCreateField(path, null, lazyStringValue, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.Enum)
            {
                instance.Add(GetOrCreateField(path, value.ToString(), null, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.Stream)
            {
                var stream = (Stream)value;
                instance.Add(GetOrCreateField(path, null, null, stream, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.Boolean)
            {
                instance.Add(GetOrCreateField(path, (bool)value ? TrueString : FalseString, null, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.DateTime)
            {
                var dateTime = (DateTime)value;
                var dateAsString = dateTime.GetDefaultRavenFormat();

                instance.Add(GetOrCreateField(path, dateAsString, null, null, null, storage, indexing, termVector));
                newFields++;

                instance.Add(GerOrCreateNumericLongField(path + Constants.Documents.Indexing.Fields.TimeFieldSuffix, dateTime.Ticks, Field.Store.NO));
                newFields++;

                _index.IndexFieldsPersistence.MarkHasTimeValue(path);

                return newFields;
            }

            if (valueType == ValueType.DateTimeOffset)
            {
                var dateTimeOffset = (DateTimeOffset)value;

                string dateAsString;
                if (field.Indexing != FieldIndexing.Default && (indexing == Field.Index.NOT_ANALYZED || indexing == Field.Index.NOT_ANALYZED_NO_NORMS))
                    dateAsString = dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
                else
                {
                    dateAsString = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);

                    instance.Add(GerOrCreateNumericLongField(path + Constants.Documents.Indexing.Fields.TimeFieldSuffix, dateTimeOffset.UtcDateTime.Ticks, Field.Store.NO));
                    newFields++;

                    _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                }

                instance.Add(GetOrCreateField(path, dateAsString, null, null, null, storage, indexing, termVector));
                newFields++;

                return newFields;
            }

            if (valueType == ValueType.TimeSpan)
            {
                var timeSpan = (TimeSpan)value;
                instance.Add(GetOrCreateField(path, timeSpan.ToString("c", CultureInfo.InvariantCulture), null, null, null, storage, indexing, termVector));

                foreach (var numericField in GetOrCreateNumericField(field, timeSpan.Ticks, storage, termVector))
                {
                    instance.Add(numericField);
                    newFields++;
                }

                return newFields;
            }

            if (valueType == ValueType.Vector)
            {
                throw new NotSupportedException("Vector operations are only supported in Corax indexes, not with Lucene indexes");
            }

            if (valueType == ValueType.DateOnly)
            {
                
                var dateOnly = (DateOnly)value;
                var asString = dateOnly.ToString(DefaultFormat.DateOnlyFormatToWrite, CultureInfo.InvariantCulture);
                var ticks = dateOnly.DayNumber * TimeSpan.TicksPerDay;

                instance.Add(GetOrCreateField(path, asString, null, null, null, storage, indexing, termVector));
                newFields++;

                instance.Add(GerOrCreateNumericLongField(path + Constants.Documents.Indexing.Fields.TimeFieldSuffix, ticks, Field.Store.NO));
                newFields++;

                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                return newFields;
            }

            if (valueType == ValueType.TimeOnly)
            {
                var timeOnly = (TimeOnly)value;
                var asString = timeOnly.ToString(DefaultFormat.TimeOnlyFormatToWrite, CultureInfo.InvariantCulture);
                instance.Add(GetOrCreateField(path, asString, null, null, null, storage, indexing, termVector));
                newFields++;

                instance.Add(GerOrCreateNumericLongField(path + Constants.Documents.Indexing.Fields.TimeFieldSuffix, timeOnly.Ticks, Field.Store.NO));
                newFields++;

                _index.IndexFieldsPersistence.MarkHasTimeValue(path);
                return newFields;

            }

            if (valueType == ValueType.BoostedValue)
            {
                var boostedValue = (BoostedValue)value;

                int boostedFields = GetRegularFields(instance, field, boostedValue.Value, indexContext, sourceDocument, out _);
                newFields += boostedFields;

                var fields = instance.GetFields();
                for (int idx = fields.Count - 1; boostedFields > 0; boostedFields--, idx--)
                {
                    var fieldFromCollection = fields[idx];
                    fieldFromCollection.Boost = boostedValue.Boost;
                    fieldFromCollection.OmitNorms = false;
                }

                return newFields;
            }

            int HandleArray(IEnumerable itemsToIndex)
            {
                int count = 1;

                if (nestedArray == false)
                {
                    instance.Add(GetOrCreateField(path + IsArrayFieldSuffix, TrueString, null, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                    newFields++;
                }

                foreach (var itemToIndex in itemsToIndex)
                {
                    if (CanCreateFieldsForNestedArray(itemToIndex, field.Indexing) == false)
                        continue;

                    using var i = NestedField(count++);

                    newFields += GetRegularFields(instance, field, itemToIndex, indexContext, sourceDocument, out _, nestedArray: true);
                }

                return newFields;
            }

            if (valueType == ValueType.Enumerable)
            {
                return HandleArray((IEnumerable)value);
            }

            int HandleObject(BlittableJsonReaderObject val)
            {
                if (val.TryGetMember("$values", out var values) &&
                    IsArrayOfTypeValueObject(val))
                {
                    return HandleArray((IEnumerable)values);
                }

                foreach (var complexObjectField in GetComplexObjectFields(path, val, storage, indexing, termVector))
                {
                    instance.Add(complexObjectField);
                    newFields++;
                }

                return newFields;
            }

            if (valueType == ValueType.DynamicJsonObject)
            {
                if (_index.SourceDocumentIncludedInOutput == false && sourceDocument == value)
                {
                    _index.SourceDocumentIncludedInOutput = true;
                }
                
                var dynamicJson = (DynamicBlittableJson)value;
                return HandleObject(dynamicJson.BlittableJson);
            }

            if (valueType == ValueType.BlittableJsonObject)
            {
                return HandleObject((BlittableJsonReaderObject)value);
            }

            if (valueType == ValueType.Lucene)
            {
                instance.Add((AbstractField)value);
                newFields++;

                return newFields;
            }

            if (valueType == ValueType.ConvertToJson || valueType == ValueType.Dictionary)
            {
                var val = TypeConverter.ToBlittableSupportedType(value);
                if (!(val is DynamicJsonValue json))
                {
                    return GetRegularFields(instance, field, val, indexContext, sourceDocument, out _, nestedArray: nestedArray);
                }

                foreach (var jsonField in GetComplexObjectFields(path, Scope.CreateJson(json, indexContext), storage, indexing, termVector))
                {
                    instance.Add(jsonField);
                    newFields++;
                }

                return newFields;
            }

            if (valueType == ValueType.Double)
            {
                var ldv = value as LazyNumberValue;
                if (ldv != null)
                {
                    if (TryToTrimTrailingZeros(ldv, indexContext, out var doubleAsString) == false)
                        doubleAsString = ldv.Inner;

                    instance.Add(GetOrCreateField(path, null, doubleAsString, null, null, storage, indexing, termVector));
                    newFields++;
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

                    instance.Add(GetOrCreateField(path, s, null, null, null, storage, indexing, termVector));
                    newFields++;
                }
            }
            else if (valueType is ValueType.Convertible or ValueType.Numeric) // we need this to store numbers in invariant format, so JSON could read them
            {
                instance.Add(GetOrCreateField(path, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), null, null, null, storage, indexing, termVector));
                newFields++;
            }

            foreach (var numericField in GetOrCreateNumericField(field, value, storage))
            {
                instance.Add(numericField);
                newFields++;
            }

            return newFields;
        }

        public ReduceMultipleValuesScope NestedField(int v)
        {
            _multipleItemsSameFieldCount.Add(v);
            return new ReduceMultipleValuesScope(this);
        }

        public readonly struct ReduceMultipleValuesScope : IDisposable
        {
            private readonly LuceneDocumentConverterBase _parent;

            public ReduceMultipleValuesScope(LuceneDocumentConverterBase parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent._multipleItemsSameFieldCount.RemoveAt(_parent._multipleItemsSameFieldCount.Count - 1);
            }
        }

        private IEnumerable<AbstractField> GetComplexObjectFields(string path, BlittableJsonReaderObject val, Field.Store storage, Field.Index indexing, Field.TermVector termVector)
        {
            if (_multipleItemsSameFieldCount.Count == 0 || _multipleItemsSameFieldCount[0] == 1)
                yield return GetOrCreateField(path + ConvertToJsonSuffix, TrueString, null, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

            yield return GetOrCreateField(path, null, null, null, val, storage, indexing, termVector);
        }

        protected Field GetOrCreateKeyField(LazyStringValue key)
        {
            return GetOrCreateField(_keyFieldName, null, key, null, null, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        }

        protected Field GetOrCreateSourceDocumentIdField(LazyStringValue key)
        {
            return GetOrCreateField(Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName, null, key, null, null, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        }

        protected Field GetOrCreateField(string name, string value, LazyStringValue lazyValue, Stream streamValue, BlittableJsonReaderObject blittableValue, Field.Store store, Field.Index index, Field.TermVector termVector)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            
            int cacheKey = FieldCacheKey.CalculateHashCode(name, index, store, termVector, _multipleItemsSameFieldCount);

            Field field;
            if (_fieldsCache.TryGetValue(cacheKey, out CachedFieldItem<Field> cached) == false ||
                !cached.Key.IsSame(name, index, store, termVector, _multipleItemsSameFieldCount))
            {
                LazyStringReader stringReader = null;
                BlittableObjectReader blittableReader = null;

                if (streamValue == null)
                {
                    if ((lazyValue != null || blittableValue != null) && store.IsStored() == false && index.IsIndexed() && index.IsAnalyzed())
                    {
                        TextReader reader;
                        if (lazyValue != null)
                        {
                            stringReader = new LazyStringReader();
                            reader = stringReader.GetTextReaderFor(lazyValue);
                        }
                        else
                        {
                            blittableReader = Scope.GetBlittableReader();
                            reader = blittableReader.GetTextReaderFor(blittableValue);
                        }

                        field = new Field(name, reader, termVector);
                    }
                    else
                    {
                        if (value == null && lazyValue == null)
                            blittableReader = Scope.GetBlittableReader();

                        field = new Field(name,
                            value ?? LazyStringReader.GetStringFor(lazyValue) ?? blittableReader.GetStringFor(blittableValue),
                            store, index, termVector);
                    }
                }
                else
                {
                    var streamBuffer = ToArray(Scope, streamValue, out var streamLength);

                    field = new Field(name, streamBuffer, 0, streamLength, store);
                }

                field.Boost = 1;
                field.OmitNorms = true;

                AddToFieldsCache(cacheKey, _multipleItemsSameFieldCount.Count > 0, cached, new CachedFieldItem<Field>
                {
                    Key = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray()),
                    Field = field,
                    LazyStringReader = stringReader
                });
            }
            else
            {
                BlittableObjectReader blittableReader = null;

                field = cached.Field;

                if (streamValue == null)
                {
                    if (lazyValue != null && cached.LazyStringReader == null)
                        cached.LazyStringReader = new LazyStringReader();
                    if (blittableValue != null)
                        blittableReader = Scope.GetBlittableReader();

                    if ((lazyValue != null || blittableValue != null) && store.IsStored() == false && index.IsIndexed() && index.IsAnalyzed())
                    {
                        field.SetValue(lazyValue != null
                            ? cached.LazyStringReader.GetTextReaderFor(lazyValue)
                            : blittableReader.GetTextReaderFor(blittableValue));
                    }
                    else
                    {
                        field.SetValue(value ?? LazyStringReader.GetStringFor(lazyValue) ?? blittableReader.GetStringFor(blittableValue));
                    }
                }
                else
                {
                    var streamBuffer = ToArray(Scope, streamValue, out var streamLength);

                    field.SetValue(streamBuffer, 0, streamLength);
                }
            }

            return field;

            
        }

        private IEnumerable<AbstractField> GetOrCreateNumericField(IndexField field, object value, Field.Store storage, Field.TermVector termVector = Field.TermVector.NO)
        {
            var fieldNameDouble = field.Name + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
            var fieldNameLong = field.Name + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;

            var numericFieldDouble = GetNumericFieldFromCache(fieldNameDouble, null, storage, termVector);
            var numericFieldLong = GetNumericFieldFromCache(fieldNameLong, null, storage, termVector);

            switch (BlittableNumber.Parse(value, out double doubleValue, out long longValue))
            {
                case NumberParseResult.Double:
                    yield return numericFieldDouble.SetDoubleValue(doubleValue);
                    yield return numericFieldLong.SetLongValue((long)doubleValue);
                    break;

                case NumberParseResult.Long:
                    yield return numericFieldDouble.SetDoubleValue(longValue);
                    yield return numericFieldLong.SetLongValue(longValue);
                    break;
            }
        }

        private NumericField GerOrCreateNumericLongField(string name, long value, Field.Store storage, Field.TermVector termVector = Field.TermVector.NO)
        {
            var numericFieldLong = GetNumericFieldFromCache(name, null, storage, termVector);
            return numericFieldLong.SetLongValue(value);
        }

        private NumericField GetNumericFieldFromCache(string name, Field.Index? index, Field.Store store, Field.TermVector termVector)
        {
            int cacheKey = FieldCacheKey.CalculateHashCode(name, index, store, termVector, _multipleItemsSameFieldCount);

            NumericField numericField;
            if (_numericFieldsCache.TryGetValue(cacheKey, out CachedFieldItem<NumericField> cached) == false ||
                !cached.Key.IsSame(name, index, store, termVector, _multipleItemsSameFieldCount))
            {
                AddToNumericFieldsCache(cacheKey, _multipleItemsSameFieldCount.Count > 0, cached, new CachedFieldItem<NumericField>
                {
                    Key = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray()),
                    Field = numericField = new NumericField(name, store, true)
                });
            }
            else
            {
                numericField = cached.Field;
            }

            return numericField;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CanCreateFieldsForNestedArray(object value, FieldIndexing fieldIndexing)
        {
            if (fieldIndexing == FieldIndexing.Search)
            {
                if (value == null || value is DynamicNullObject)
                    return false;
            }

            return true;
        }

        protected AbstractField GetStoredValueField(BlittableJsonReaderObject value, IWriteOperationBuffer writeBuffer)
        {
            _storeValueField.SetValue(GetStoredValue(value, writeBuffer), 0, value.Size);
            
            return _storeValueField;
        }

        private byte[] GetStoredValue(BlittableJsonReaderObject value, IWriteOperationBuffer writeBuffer)
        {
            var necessarySize = Bits.PowerOf2(value.Size);

            var storeValueBuffer = writeBuffer.GetBuffer(necessarySize);

            unsafe
            {
                fixed (byte* v = storeValueBuffer)
                    value.CopyTo(v);
            }

            return storeValueBuffer;
        }

        public override void Dispose()
        {
            ClearFieldCache(_fieldsCache);
            ClearFieldCache(_numericFieldsCache);

            _numberOfItemsInFieldsCacheForMultipleItemsSameField = 0;
            _numberOfItemsInNumericFieldsCacheForMultipleItemsSameField = 0;
        }

        private static void ClearFieldCache<T>(Dictionary<int, CachedFieldItem<T>> fieldCache)
            where T : AbstractField
        {
            if (fieldCache == null)
                return;

            foreach (var cachedFieldItem in fieldCache.Values)
                cachedFieldItem.Dispose();

            fieldCache.Clear();
        }

        private void AddToFieldsCache(int cacheKey, bool isMultipleItemsSameField, CachedFieldItem<Field> oldItem, CachedFieldItem<Field> newItem)
        {
            var addToCache = isMultipleItemsSameField == false || _numberOfItemsInFieldsCacheForMultipleItemsSameField < MaximumNumberOfItemsInFieldsCacheForMultipleItemsSameField;

            if (addToCache == false)
            {
                Scope.AddToDispose(newItem);
                return;
            }

            Scope.AddToDispose(oldItem);

            _fieldsCache[cacheKey] = newItem;

            if (isMultipleItemsSameField)
                _numberOfItemsInFieldsCacheForMultipleItemsSameField++;
        }

        private void AddToNumericFieldsCache(int cacheKey, bool isMultipleItemsSameField, CachedFieldItem<NumericField> oldItem, CachedFieldItem<NumericField> newItem)
        {
            var addToCache = isMultipleItemsSameField == false || _numberOfItemsInNumericFieldsCacheForMultipleItemsSameField < MaximumNumberOfItemsInFieldsCacheForMultipleItemsSameField;

            if (addToCache == false)
            {
                Scope.AddToDispose(newItem);
                return;
            }

            Scope.AddToDispose(oldItem);

            _numericFieldsCache[cacheKey] = newItem;

            if (isMultipleItemsSameField)
                _numberOfItemsInNumericFieldsCacheForMultipleItemsSameField++;
        }
    }
}
