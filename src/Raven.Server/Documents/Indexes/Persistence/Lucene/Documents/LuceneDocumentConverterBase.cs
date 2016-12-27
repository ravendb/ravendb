using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Json;
using Sparrow.Json;
using Raven.Abstractions.Extensions;
using Raven.Abstractions;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public abstract class LuceneDocumentConverterBase : IDisposable
    {
        internal const string IsArrayFieldSuffix = "_IsArray";

        internal const string ConvertToJsonSuffix = "_ConvertToJson";

        private const string TrueString = "true";

        private const string FalseString = "false";

        private static readonly FieldCacheKeyEqualityComparer Comparer = new FieldCacheKeyEqualityComparer();

        private readonly Field _reduceValueField = new Field(Constants.Indexing.Fields.ReduceValueFieldName, new byte[0], 0, 0, Field.Store.YES);

        protected readonly ConversionScope Scope = new ConversionScope();

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<Field>> _fieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<Field>>(Comparer);

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<NumericField>> _numericFieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<NumericField>>(Comparer);

        public readonly global::Lucene.Net.Documents.Document Document = new global::Lucene.Net.Documents.Document();

        private readonly List<int> _multipleItemsSameFieldCount = new List<int>();

        protected readonly Dictionary<string, IndexField> _fields;

        protected readonly bool _reduceOutput;

        private byte[] _reduceValueBuffer;

        public void Clean()
        {
            if (_fieldsCache.Count > 256)
            {
                _fieldsCache.Clear();
            }
        }

        protected LuceneDocumentConverterBase(ICollection<IndexField> fields, bool reduceOutput = false)
        {
            _fields = fields.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

            _reduceOutput = reduceOutput;

            if (reduceOutput)
                _reduceValueBuffer = new byte[0];
        }

        // returned document needs to be written do index right after conversion because the same cached instance is used here
        public IDisposable SetDocument(LazyStringValue key, object document, JsonOperationContext indexContext, out bool shouldSkip)
        {
            Document.GetFields().Clear();

            var numberOfFields = 0;
            foreach (var field in GetFields(key, document, indexContext))
            {
                Document.Add(field);
                numberOfFields++;
            }

            shouldSkip = numberOfFields <= 1; // there is always a key field, but we want to filter-out empty documents

            return Scope;
        }

        protected abstract IEnumerable<AbstractField> GetFields(LazyStringValue key, object document, JsonOperationContext indexContext);

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
        public IEnumerable<AbstractField> GetRegularFields(IndexField field, object value, JsonOperationContext indexContext, bool nestedArray = false)
        {
            var path = field.Name;

            var valueType = GetValueType(value);

            Field.Index defaultIndexing;
            switch (valueType)
            {
                case ValueType.LazyString:
                case ValueType.LazyCompressedString:
                case ValueType.String:
                case ValueType.Enum:
                    defaultIndexing = Field.Index.ANALYZED;
                    break;
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

            if (valueType == ValueType.Null)
            {
                yield return GetOrCreateField(path, Constants.NullValue, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                yield break;
            }

            if (valueType == ValueType.DynamicNull)
            {
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull)
                {
                    var sort = field.SortOption;
                    if (sort == null
                        || sort.Value == SortOptions.None
                        || sort.Value == SortOptions.String
                        || sort.Value == SortOptions.StringVal
                        //|| sort.Value == SortOptions.Custom // TODO arek
                        )
                    {
                        yield return GetOrCreateField(path, Constants.NullValue, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                    }

                    foreach (var numericField in GetOrCreateNumericField(field, GetNullValueForSorting(sort), storage))
                        yield return numericField;
                }

                yield break;
            }

            if (valueType == ValueType.EmptyString)
            {
                yield return GetOrCreateField(path, Constants.EmptyString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                yield break;
            }

            if (valueType == ValueType.String)
            {
                yield return GetOrCreateField(path, value.ToString(), null, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.LazyString || valueType == ValueType.LazyCompressedString)
            {
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.LazyCompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;

                yield return GetOrCreateField(path, null, lazyStringValue, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.Enum)
            {
                yield return GetOrCreateField(path, value.ToString(), null, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.Boolean)
            {
                yield return GetOrCreateField(path, (bool)value ? TrueString : FalseString, null, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.DateTime)
            {
                var dateTime = (DateTime)value;
                var dateAsString = dateTime.GetDefaultRavenFormat(isUtc: dateTime.Kind == DateTimeKind.Utc);
                yield return GetOrCreateField(path, dateAsString, null, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.DateTimeOffset)
            {
                var dateTimeOffset = (DateTimeOffset)value;

                string dateAsString;
                if (field.Indexing != FieldIndexing.Default && (indexing == Field.Index.NOT_ANALYZED || indexing == Field.Index.NOT_ANALYZED_NO_NORMS))
                    dateAsString = dateTimeOffset.ToString(Default.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
                else
                    dateAsString = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);

                yield return GetOrCreateField(path, dateAsString, null, null, storage, indexing, termVector);
                yield break;
            }

            if (valueType == ValueType.TimeSpan)
            {
                var timeSpan = (TimeSpan)value;
                yield return GetOrCreateField(path, timeSpan.ToString("c", CultureInfo.InvariantCulture), null, null, storage, indexing, termVector);

                foreach (var numericField in GetOrCreateNumericField(field, timeSpan.Ticks, storage, termVector))
                    yield return numericField;

                yield break;
            }

            if (valueType == ValueType.BoostedValue)
            {
                var boostedValue = (BoostedValue)value;
                foreach (var fieldFromCollection in GetRegularFields(field, boostedValue.Value, indexContext, nestedArray: false))
                {
                    fieldFromCollection.Boost = boostedValue.Boost;
                    fieldFromCollection.OmitNorms = false;
                    yield return fieldFromCollection;
                }

                yield break;
            }

            if (valueType == ValueType.Enumerable)
            {
                var itemsToIndex = (IEnumerable)value;
                int count = 1;

                if (nestedArray == false)
                    yield return GetOrCreateField(path + IsArrayFieldSuffix, TrueString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

                foreach (var itemToIndex in itemsToIndex)
                {
                    if (CanCreateFieldsForNestedArray(itemToIndex, indexing) == false)
                        continue;

                    _multipleItemsSameFieldCount.Add(count++);

                    foreach (var fieldFromCollection in GetRegularFields(field, itemToIndex, indexContext, nestedArray: true))
                        yield return fieldFromCollection;

                    _multipleItemsSameFieldCount.RemoveAt(_multipleItemsSameFieldCount.Count - 1);
                }

                yield break;
            }

            if (valueType == ValueType.DynamicJsonObject)
            {
                var dynamicJson = (DynamicBlittableJson)value;

                foreach (var complexObjectField in GetComplexObjectFields(path, dynamicJson.BlittableJson, storage, indexing, termVector))
                    yield return complexObjectField;

                yield break;
            }

            if (valueType == ValueType.BlittableJsonObject)
            {
                var val = (BlittableJsonReaderObject)value;

                foreach (var complexObjectField in GetComplexObjectFields(path, val, storage, indexing, termVector))
                    yield return complexObjectField;

                yield break;
            }

            if (valueType == ValueType.Lucene)
            {
                yield return (AbstractField)value;
                yield break;
            }

            if (valueType == ValueType.Double)
            {
                yield return GetOrCreateField(path, null, ((LazyDoubleValue)value).Inner, null, storage, indexing, termVector);
            }
            else if (valueType == ValueType.Convertible) // we need this to store numbers in invariant format, so JSON could read them
            {
                yield return GetOrCreateField(path, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), null, null, storage, indexing, termVector);
            }

            if (valueType == ValueType.ConvertToJson)
            {
                var json = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(value);

                foreach (var jsonField in GetComplexObjectFields(path, Scope.CreateJson(json, indexContext), storage, indexing, termVector))
                    yield return jsonField;

                yield break;
            }

            foreach (var numericField in GetOrCreateNumericField(field, value, storage))
                yield return numericField;
        }

        private IEnumerable<AbstractField> GetComplexObjectFields(string path, BlittableJsonReaderObject val, Field.Store storage, Field.Index indexing, Field.TermVector termVector)
        {
            if (_multipleItemsSameFieldCount.Count == 0 || _multipleItemsSameFieldCount[0] == 1)
                yield return GetOrCreateField(path + ConvertToJsonSuffix, TrueString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

            yield return GetOrCreateField(path, null, null, val, storage, indexing, termVector);
        }

        private static ValueType GetValueType(object value)
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

            if (value is Enum) return ValueType.Enum;

            if (value is bool) return ValueType.Boolean;

            if (value is DateTime) return ValueType.DateTime;

            if (value is DateTimeOffset) return ValueType.DateTimeOffset;

            if (value is TimeSpan) return ValueType.TimeSpan;

            if (value is BoostedValue) return ValueType.BoostedValue;

            if (value is DynamicBlittableJson) return ValueType.DynamicJsonObject;

            if (value is IEnumerable) return ValueType.Enumerable;

            if (value is LazyDoubleValue) return ValueType.Double;

            if (value is AbstractField) return ValueType.Lucene;

            if (value is char) return ValueType.String;

            if (value is IConvertible) return ValueType.Convertible;

            if (value is BlittableJsonReaderObject) return ValueType.BlittableJsonObject;

            if (IsNumber(value)) return ValueType.Numeric;


            return ValueType.ConvertToJson;
        }

        private static object GetNullValueForSorting(SortOptions? sortOptions)
        {
            switch (sortOptions)
            {
                case SortOptions.NumericDouble:
                    return double.MinValue;
                default:
                    return long.MinValue;
            }
        }

        protected Field GetOrCreateKeyField(LazyStringValue key)
        {
            if (_reduceOutput == false)
                return GetOrCreateField(Constants.Indexing.Fields.DocumentIdFieldName, null, key, null, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

            return GetOrCreateField(Constants.Indexing.Fields.ReduceKeyFieldName, null, key, null, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        }

        protected Field GetOrCreateField(string name, string value, LazyStringValue lazyValue, BlittableJsonReaderObject blittableValue, Field.Store store, Field.Index index, Field.TermVector termVector)
        {
            var cacheKey = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray());

            Field field;
            CachedFieldItem<Field> cached;

            if (_fieldsCache.TryGetValue(cacheKey, out cached) == false)
            {
                LazyStringReader stringReader = null;
                BlittableObjectReader blittableReader = null;

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
                        blittableReader = new BlittableObjectReader();
                        reader = blittableReader.GetTextReaderFor(blittableValue);
                    }

                    field = new Field(CreateFieldName(name), reader, termVector);
                }
                else
                {
                    if (value == null && blittableValue == null)
                        stringReader = new LazyStringReader();
                    else if (value == null && lazyValue == null)
                        blittableReader = new BlittableObjectReader();

                    field = new Field(CreateFieldName(name),
                        value ?? stringReader?.GetStringFor(lazyValue) ?? blittableReader.GetStringFor(blittableValue),
                        store, index, termVector);
                }

                field.Boost = 1;
                field.OmitNorms = true;

                _fieldsCache[cacheKey] = new CachedFieldItem<Field>
                {
                    Field = field,
                    LazyStringReader = stringReader,
                    BlittableObjectReader = blittableReader
                };
            }
            else
            {
                field = cached.Field;
                if (lazyValue != null && cached.LazyStringReader == null)
                    cached.LazyStringReader = new LazyStringReader();
                if (blittableValue != null && cached.BlittableObjectReader == null)
                    cached.BlittableObjectReader = new BlittableObjectReader();

                if ((lazyValue != null || blittableValue != null) && store.IsStored() == false && index.IsIndexed() && index.IsAnalyzed())
                {
                    field.SetValue(cached.LazyStringReader?.GetTextReaderFor(lazyValue) ?? cached.BlittableObjectReader.GetTextReaderFor(blittableValue));
                }
                else
                {
                    field.SetValue(value ?? cached.LazyStringReader?.GetStringFor(lazyValue) ?? cached.BlittableObjectReader.GetStringFor(blittableValue));
                }
            }

            return field;
        }

        private IEnumerable<AbstractField> GetOrCreateNumericField(IndexField field, object value, Field.Store storage, Field.TermVector termVector = Field.TermVector.NO)
        {
            var fieldName = field.Name + Constants.Indexing.Fields.RangeFieldSuffix;

            var cacheKey = new FieldCacheKey(field.Name, null, storage, termVector,
                _multipleItemsSameFieldCount.ToArray());

            NumericField numericField;
            CachedFieldItem<NumericField> cached;

            if (_numericFieldsCache.TryGetValue(cacheKey, out cached) == false)
            {
                _numericFieldsCache[cacheKey] = cached = new CachedFieldItem<NumericField>
                {
                    Field = numericField = new NumericField(CreateFieldName(fieldName), storage, true)
                };
            }
            else
            {
                numericField = cached.Field;
            }

            double doubleValue;
            long longValue;

            switch (BlittableNumber.Parse(value, out doubleValue, out longValue))
            {
                case NumberParseResult.Double:
                    if (field.SortOption == SortOptions.NumericLong)
                        yield return numericField.SetLongValue((long) doubleValue);
                    else
                        yield return numericField.SetDoubleValue(doubleValue);
                    break;
                case NumberParseResult.Long:
                    if (field.SortOption == SortOptions.NumericDouble)
                        yield return numericField.SetDoubleValue(longValue);
                    else
                        yield return numericField.SetLongValue(longValue);
                    break;
            }
        }

        private string CreateFieldName(string name)
        {
            var result = IndexField.ReplaceInvalidCharactersInFieldName(name);

            return result;
        }

        private static bool CanCreateFieldsForNestedArray(object value, Field.Index index)
        {
            if (index.IsAnalyzed() == false)
                return true;

            if (value == null || value is DynamicNullObject)
                return false;

            return true;
        }

        protected AbstractField GetReduceResultValueField(BlittableJsonReaderObject reduceResult)
        {
            _reduceValueField.SetValue(GetReduceResult(reduceResult), 0, reduceResult.Size);

            return _reduceValueField;
        }

        private byte[] GetReduceResult(BlittableJsonReaderObject reduceResult)
        {
            var necessarySize = Bits.NextPowerOf2(reduceResult.Size);

            if (_reduceValueBuffer.Length < necessarySize)
                _reduceValueBuffer = new byte[necessarySize];

            unsafe
            {
                fixed (byte* v = _reduceValueBuffer)
                    reduceResult.CopyTo(v);
            }

            return _reduceValueBuffer;
        }

        public void Dispose()
        {
            foreach (var cachedFieldItem in _fieldsCache.Values)
            {
                cachedFieldItem.Dispose();
            }
        }

        public static bool IsNumber(object value)
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

        private enum ValueType
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

            ConvertToJson
        }

        protected class ConversionScope : IDisposable
        {
            private readonly LinkedList<BlittableJsonReaderObject> _jsons = new LinkedList<BlittableJsonReaderObject>();

            public BlittableJsonReaderObject CreateJson(DynamicJsonValue djv, JsonOperationContext context)
            {
                var result = context.ReadObject(djv, "lucene field as json");

                _jsons.AddFirst(result);

                return result;
            }

            public void Dispose()
            {
                if (_jsons.Count == 0)
                    return;

                foreach (var json in _jsons)
                {
                    json.Dispose();
                }

                _jsons.Clear();
            }
        }
    }
}