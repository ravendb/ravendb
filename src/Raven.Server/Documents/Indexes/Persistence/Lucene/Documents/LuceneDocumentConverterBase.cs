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

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public abstract class LuceneDocumentConverterBase : IDisposable
    {
        internal const string IsArrayFieldSuffix = "_IsArray";

        internal const string ConvertToJsonSuffix = "_ConvertToJson";

        private const string TrueString = "true";

        private const string FalseString = "false";

        private static readonly FieldCacheKeyEqualityComparer Comparer = new FieldCacheKeyEqualityComparer();

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<Field>> _fieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<Field>>(Comparer);

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<NumericField>> _numericFieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<NumericField>>(Comparer);

        private readonly global::Lucene.Net.Documents.Document _document = new global::Lucene.Net.Documents.Document();

        private readonly List<int> _multipleItemsSameFieldCount = new List<int>();

        protected readonly Dictionary<string, IndexField> _fields;

        protected readonly bool _reduceOutput;

        protected LuceneDocumentConverterBase(ICollection<IndexField> fields, bool reduceOutput = false)
        {
            _fields = fields.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

            _reduceOutput = reduceOutput;
        }

        // returned document needs to be written do index right after conversion because the same cached instance is used here
        public global::Lucene.Net.Documents.Document ConvertToCachedDocument(LazyStringValue key, object document)
        {
            _document.GetFields().Clear();

            foreach (var field in GetFields(key, document))
            {
                _document.Add(field);
            }

            return _document;
        }

        protected abstract IEnumerable<AbstractField> GetFields(LazyStringValue key, object document);

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
        protected IEnumerable<AbstractField> GetRegularFields(IndexField field, object value, bool nestedArray = false)
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
                case ValueType.Boolean:
                case ValueType.Double:
                case ValueType.Null:
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

            if (valueType == ValueType.EmptyString)
            {
                yield return GetOrCreateField(path, Constants.EmptyString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                yield break;
            }

            if (valueType == ValueType.String)
            {
                yield return GetOrCreateField(path, (string)value, null, null, storage, indexing, termVector);
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

            if (valueType == ValueType.BoostedValue)
            {
                var boostedValue = (BoostedValue)value;
                foreach (var fieldFromCollection in GetRegularFields(field, boostedValue.Value, nestedArray: false))
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

                    foreach (var fieldFromCollection in GetRegularFields(field, itemToIndex, nestedArray: true))
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
            if (value == null || value is DynamicNullObject) return ValueType.Null;

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

            if (value is BoostedValue) return ValueType.BoostedValue;

            if (value is DynamicBlittableJson) return ValueType.DynamicJsonObject;

            if (value is IEnumerable) return ValueType.Enumerable;

            if (value is LazyDoubleValue) return ValueType.Double;

            if (value is AbstractField) return ValueType.Lucene;

            if (value is IConvertible) return ValueType.Convertible;

            if (value is BlittableJsonReaderObject) return ValueType.BlittableJsonObject;

            return ValueType.Numeric;
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
            var fieldName = field.Name + "_Range";

            var cacheKey = new FieldCacheKey(field.Name, null, storage, termVector, _multipleItemsSameFieldCount.ToArray());

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

        public void Dispose()
        {
            foreach (var cachedFieldItem in _fieldsCache.Values)
            {
                cachedFieldItem.Dispose();
            }
        }

        private enum ValueType
        {
            Null,

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

            Enum,

            Lucene
        }
    }
}