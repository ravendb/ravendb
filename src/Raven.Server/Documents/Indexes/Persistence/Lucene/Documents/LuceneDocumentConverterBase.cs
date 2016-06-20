using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Lucene.Net.Documents;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public abstract class LuceneDocumentConverterBase : IDisposable
    {
        private const string IsArrayFieldSuffix = "_IsArray";

        private const string TrueString = "true";

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

            var defaultIndexing = valueType == ValueType.CompressedString || valueType == ValueType.String
                                      ? Field.Index.ANALYZED
                                      : Field.Index.ANALYZED_NO_NORMS;

            var indexing = field.Indexing.GetLuceneValue(field.Analyzer, @default: defaultIndexing);
            var storage = field.Storage.GetLuceneValue();
            var termVector = field.TermVector.GetLuceneValue();

            if (valueType == ValueType.Null)
            {
                yield return GetOrCreateField(path, Constants.NullValue, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                yield break;
            }

            if (valueType == ValueType.EmptyString)
            {
                yield return GetOrCreateField(path, Constants.EmptyString, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                yield break;
            }

            if (valueType == ValueType.String || valueType == ValueType.CompressedString)
            {
                LazyStringValue lazyStringValue;
                if (valueType == ValueType.CompressedString)
                    lazyStringValue = ((LazyCompressedStringValue)value).ToLazyStringValue();
                else
                    lazyStringValue = (LazyStringValue)value;

                yield return GetOrCreateField(path, null, lazyStringValue, storage, indexing, termVector);
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
                var itemsToIndex = value as IEnumerable;
                int count = 1;

                if (nestedArray == false)
                    yield return GetOrCreateField(path + IsArrayFieldSuffix, TrueString, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

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

            if (valueType == ValueType.Double)
            {
                yield return GetOrCreateField(path, null, ((LazyDoubleValue)value).Inner, storage, indexing, termVector);
            }
            else if (valueType == ValueType.Convertible) // we need this to store numbers in invariant format, so JSON could read them
            {
                yield return GetOrCreateField(path, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), null, storage, indexing, termVector); // TODO arek - ToString()? anything better?
            }

            foreach (var numericField in GetOrCreateNumericField(field, value, storage))
                yield return numericField;
        }

        private static ValueType GetValueType(object value)
        {
            if (value == null || value is DynamicNullObject) return ValueType.Null;

            if (Equals(value, string.Empty)) return ValueType.EmptyString;

            if (value is LazyStringValue) return ValueType.String;

            if (value is LazyCompressedStringValue) return ValueType.CompressedString;

            if (value is BoostedValue) return ValueType.BoostedValue;

            if (value is IEnumerable) return ValueType.Enumerable;

            if (value is LazyDoubleValue) return ValueType.Double;

            if (value is IConvertible) return ValueType.Convertible;

            return ValueType.Numeric;
        }

        protected Field GetOrCreateKeyField(LazyStringValue key)
        {
            if (_reduceOutput == false)
                return GetOrCreateField(Constants.DocumentIdFieldName, null, key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

            return GetOrCreateField(Constants.ReduceKeyFieldName, null, key, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        }

        protected Field GetOrCreateField(string name, string value, LazyStringValue lazyValue, Field.Store store, Field.Index index, Field.TermVector termVector)
        {
            var cacheKey = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray());

            Field field;
            CachedFieldItem<Field> cached;

            if (_fieldsCache.TryGetValue(cacheKey, out cached) == false)
            {
                LazyStringReader reader = null;

                if (lazyValue != null && store.IsStored() == false && index.IsIndexed() && index.IsAnalyzed())
                {
                    reader = new LazyStringReader();

                    field = new Field(CreateFieldName(name), reader.GetTextReaderFor(lazyValue), termVector);
                }
                else
                {
                    if (value == null)
                        reader = new LazyStringReader();

                    field = new Field(CreateFieldName(name), value ?? reader.GetStringFor(lazyValue), store, index, termVector);
                }

                field.Boost = 1;
                field.OmitNorms = true;

                _fieldsCache[cacheKey] = new CachedFieldItem<Field>
                {
                    Field = field,
                    LazyStringReader = reader
                };
            }
            else
            {
                field = cached.Field;

                if (lazyValue != null && store.IsStored() == false && index.IsIndexed() && index.IsAnalyzed())
                    field.SetValue(cached.LazyStringReader.GetTextReaderFor(lazyValue));
                else
                    field.SetValue(value ?? cached.LazyStringReader.GetStringFor(lazyValue));
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

            if (value == null)
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

            CompressedString,

            Enumerable,

            Double,

            Convertible,

            Numeric,

            BoostedValue
        }
    }
}