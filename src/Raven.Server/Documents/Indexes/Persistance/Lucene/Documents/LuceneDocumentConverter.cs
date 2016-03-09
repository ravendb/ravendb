using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Documents.Fields;
using Raven.Server.Json;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene.Documents
{
    public class LuceneDocumentConverter : IDisposable
    {
        private const string IsArrayFieldSuffix = "_IsArray";

        private const string TrueString = "true";

        private static readonly FieldCacheKeyEqualityComparer Comparer = new FieldCacheKeyEqualityComparer();

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<Field>> _fieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<Field>>(Comparer);

        private readonly Dictionary<FieldCacheKey, CachedFieldItem<NumericField>> _numericFieldsCache = new Dictionary<FieldCacheKey, CachedFieldItem<NumericField>>(Comparer);

        private readonly global::Lucene.Net.Documents.Document _document = new global::Lucene.Net.Documents.Document();

        
        private readonly List<int> _multipleItemsSameFieldCount = new List<int>();

        private readonly BlittableJsonTraverser _blittableTraverser = new BlittableJsonTraverser();

        private readonly IndexField[] _fields;

        public LuceneDocumentConverter(IndexField[] fields)
        {
            _fields = fields;
        }

        // returned document needs to be written do index right after conversion because the same cached instance is used here
        public global::Lucene.Net.Documents.Document ConvertToCachedDocument(Document document)
        {
            _document.GetFields().Clear();
          
            foreach (var field in GetFields(document))
            {
                _document.Add(field);
            }

            return _document;
        }
        
        private IEnumerable<AbstractField> GetFields(Document document)
        {
            if (document.Key != null)
            {
                yield return GetOrCreateField(Constants.DocumentIdFieldName, null, document.Key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
            }
            
            foreach (var indexField in _fields)
            {
                object value;

                if (_blittableTraverser.TryRead(document.Data, indexField.Name, out value) == false)
                    continue;

                foreach (var luceneField in GetRegularFields(indexField, value))
                    yield return luceneField;
            }
        }

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
        private IEnumerable<AbstractField> GetRegularFields(IndexField field, object value, bool nestedArray = false)
        {
            var path = field.Name;

            var indexing = field.Indexing.GetLuceneValue(@default: Field.Index.ANALYZED_NO_NORMS);
            var storage = field.Storage.GetLuceneValue(@default: Field.Store.NO);

            if (value == null)
            {
                yield return GetOrCreateField(path, Constants.NullValue, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS);
                yield break;
            }

            if (Equals(value, string.Empty))
            {
                yield return GetOrCreateField(path, Constants.EmptyString, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS);
                yield break;
            }

            var lazyStringValue = value as LazyStringValue;

            if (lazyStringValue != null)
            {
                yield return GetOrCreateField(path, null, lazyStringValue, storage, indexing);
                yield break;
            }

            var itemsToIndex = value as IEnumerable;
            if (itemsToIndex != null)
            {
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

            if (value is LazyDoubleValue)
            {
                yield return GetOrCreateField(path, null, ((LazyDoubleValue)value).Inner, storage, indexing);
            }
            else if (value is IConvertible) // we need this to store numbers in invariant format, so JSON could read them // TODO arek - do we still need that?
            {
                yield return GetOrCreateField(path, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), null, storage, indexing); // TODO arek - ToString()? anything better?
            }

            foreach (var numericField in GetOrCreateNumericField(field, value, storage))
                yield return numericField;
        }

        private Field GetOrCreateField(string name, string value, LazyStringValue lazyValue, Field.Store store, Field.Index index, Field.TermVector termVector = Field.TermVector.NO)
        {
            var cacheKey = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray());

            Field field;
            CachedFieldItem<Field> cached;

            if (_fieldsCache.TryGetValue(cacheKey, out cached) == false)
            {
                LazyStringReader reader = null;

                if (lazyValue != null && store.IsStored() == false && index.IsIndexed())
                {
                    reader = new LazyStringReader();

                    field = new Field(CreateFieldName(name), reader.GetTextReaderFor(lazyValue));
                }
                else
                {
                    if (value == null)
                        reader = new LazyStringReader();

                    field = new Field(CreateFieldName(name), value ?? reader.GetStringFor(lazyValue), store, index);
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

                if (lazyValue != null && store.IsStored() == false && index.IsIndexed())
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

            if (value is LazyDoubleValue)
            {
                var doubleValue = double.Parse(cached.LazyStringReader.GetStringFor(((LazyDoubleValue) value).Inner)); // TODO arek - 

                yield return numericField.SetDoubleValue(doubleValue);
            }
            else if (value is long)
            {
                if (field.SortOption == SortOptions.NumbericDouble)
                    yield return numericField.SetDoubleValue((long)value);
                else
                    yield return numericField.SetLongValue((long) value);
            }
            else
            {
                throw new InvalidOperationException($"Could not create numeric field for the value '{value}' of the given type: {value.GetType().FullName}");
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
    }
}