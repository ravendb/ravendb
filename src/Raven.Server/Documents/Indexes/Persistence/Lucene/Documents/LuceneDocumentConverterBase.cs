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
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using LuceneDocument = Lucene.Net.Documents.Document;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public interface ILuceneDocumentWrapper
    {
        void Add(AbstractField field);
        IList<IFieldable> GetFields();
    }

    public abstract class LuceneDocumentConverterBase : IDisposable
    {
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

        private const string TrueString = "true";

        private const string FalseString = "false";       

        private readonly Field _reduceValueField = new Field(Constants.Documents.Indexing.Fields.ReduceValueFieldName, new byte[0], 0, 0, Field.Store.YES);

        protected readonly ConversionScope Scope = new ConversionScope();

        private readonly FastDictionary<int, CachedFieldItem<Field>, NumericEqualityComparer> _fieldsCache = new FastDictionary<int, CachedFieldItem<Field>, NumericEqualityComparer>(default(NumericEqualityComparer));

        private readonly FastDictionary<int, CachedFieldItem<NumericField>, NumericEqualityComparer> _numericFieldsCache = new FastDictionary<int, CachedFieldItem<NumericField>, NumericEqualityComparer>(default(NumericEqualityComparer));

        public readonly LuceneDocument Document = new LuceneDocument();

        private readonly List<int> _multipleItemsSameFieldCount = new List<int>();

        protected readonly FastDictionary<string, IndexField, OrdinalIgnoreCaseStringStructComparer> _fields;

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
            var dictionary = new FastDictionary<string, IndexField, OrdinalIgnoreCaseStringStructComparer>(fields.Count, default(OrdinalIgnoreCaseStringStructComparer));
            foreach (var field in fields)
                dictionary[field.Name] = field;
            _fields = dictionary;

            _reduceOutput = reduceOutput;

            if (reduceOutput)
                _reduceValueBuffer = new byte[0];
        }

        // returned document needs to be written do index right after conversion because the same cached instance is used here
        public IDisposable SetDocument(LazyStringValue key, object document, JsonOperationContext indexContext, out bool shouldSkip)
        {
            Document.GetFields().Clear();

            int numberOfFields = GetFields( new DefaultDocumentLuceneWrapper (Document), key, document, indexContext);

            shouldSkip = numberOfFields <= 1; // there is always a key field, but we want to filter-out empty documents

            return Scope;
        }

        protected abstract int GetFields<T>(T instance, LazyStringValue key, object document, JsonOperationContext indexContext) where T : ILuceneDocumentWrapper;

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
        public int GetRegularFields<T>(T instance, IndexField field, object value, JsonOperationContext indexContext, bool nestedArray = false) where T : ILuceneDocumentWrapper
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

            int newFields = 0;

            if (valueType == ValueType.Null)
            {
                instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.NullValue, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                newFields++;

                return newFields;
            }

            if (valueType == ValueType.DynamicNull)
            {
                var dynamicNull = (DynamicNullObject)value;
                if (dynamicNull.IsExplicitNull)
                {
                    var sort = field.Sort;
                    if (sort == null
                        || sort.Value == SortOptions.None
                        || sort.Value == SortOptions.String
                        || sort.Value == SortOptions.StringVal
                        //|| sort.Value == SortOptions.Custom // TODO arek
                        )
                    {
                        instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.NullValue, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                        newFields++;
                    }

                    foreach (var numericField in GetOrCreateNumericField(field, double.MinValue, storage))
                    {
                        instance.Add(numericField);
                        newFields++;
                    }                        
                }

                return newFields;
            }

            if (valueType == ValueType.EmptyString)
            {
                instance.Add(GetOrCreateField(path, Constants.Documents.Indexing.Fields.EmptyString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.String)
            {
                string stringValue = value as string ?? value.ToString();

                instance.Add(GetOrCreateField(path, stringValue, null, null, storage, indexing, termVector));
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

                instance.Add(GetOrCreateField(path, null, lazyStringValue, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.Enum)
            {
                instance.Add(GetOrCreateField(path, value.ToString(), null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.Boolean)
            {
                instance.Add(GetOrCreateField(path, (bool)value ? TrueString : FalseString, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.DateTime)
            {
                var dateTime = (DateTime)value;
                var dateAsString = dateTime.GetDefaultRavenFormat(isUtc: dateTime.Kind == DateTimeKind.Utc);

                instance.Add(GetOrCreateField(path, dateAsString, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.DateTimeOffset)
            {
                var dateTimeOffset = (DateTimeOffset)value;

                string dateAsString;
                if (field.Indexing != FieldIndexing.Default && (indexing == Field.Index.NOT_ANALYZED || indexing == Field.Index.NOT_ANALYZED_NO_NORMS))
                    dateAsString = dateTimeOffset.ToString(Default.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture);
                else
                    dateAsString = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);

                instance.Add(GetOrCreateField(path, dateAsString, null, null, storage, indexing, termVector));
                newFields++;
                return newFields;
            }

            if (valueType == ValueType.TimeSpan)
            {
                var timeSpan = (TimeSpan)value;
                instance.Add(GetOrCreateField(path, timeSpan.ToString("c", CultureInfo.InvariantCulture), null, null, storage, indexing, termVector));

                foreach (var numericField in GetOrCreateNumericField(field, timeSpan.Ticks, storage, termVector))
                {
                    instance.Add(numericField);
                    newFields++;
                }
                
                return newFields;
            }

            if (valueType == ValueType.BoostedValue)
            {
                var boostedValue = (BoostedValue)value;

                int boostedFields = GetRegularFields(instance, field, boostedValue.Value, indexContext, nestedArray: false);
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

            if (valueType == ValueType.Enumerable)
            {
                var itemsToIndex = (IEnumerable)value;
                int count = 1;

                if (nestedArray == false)
                {
                    instance.Add(GetOrCreateField(path + IsArrayFieldSuffix, TrueString, null, null, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
                    newFields++;
                }

                foreach (var itemToIndex in itemsToIndex)
                {
                    if (CanCreateFieldsForNestedArray(itemToIndex, indexing) == false)
                        continue;

                    _multipleItemsSameFieldCount.Add(count++);

                    newFields += GetRegularFields(instance, field, itemToIndex, indexContext, nestedArray: true);

                    _multipleItemsSameFieldCount.RemoveAt(_multipleItemsSameFieldCount.Count - 1);
                }

                return newFields;
            }

            if (valueType == ValueType.DynamicJsonObject)
            {
                var dynamicJson = (DynamicBlittableJson)value;

                foreach (var complexObjectField in GetComplexObjectFields(path, dynamicJson.BlittableJson, storage, indexing, termVector))
                {
                    instance.Add(complexObjectField);
                    newFields++;
                }

                return newFields;
            }

            if (valueType == ValueType.BlittableJsonObject)
            {
                var val = (BlittableJsonReaderObject)value;

                foreach (var complexObjectField in GetComplexObjectFields(path, val, storage, indexing, termVector))
                {
                    instance.Add(complexObjectField);
                    newFields++;
                }                    

                return newFields;
            }

            if (valueType == ValueType.Lucene)
            {
                instance.Add((AbstractField)value);
                newFields++;

                return newFields;
            }

            if (valueType == ValueType.ConvertToJson)
            {
                var json = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(value);

                foreach (var jsonField in GetComplexObjectFields(path, Scope.CreateJson(json, indexContext), storage, indexing, termVector))
                {
                    instance.Add(jsonField);
                    newFields++;
                }                    

                return newFields;
            }

            if (valueType == ValueType.Double)
            {
                var ldv = value as LazyDoubleValue;
                if (ldv != null)
                {
                    LazyStringValue doubleAsString;
                    if (TryToTrimTrailingZeros(ldv, indexContext, out doubleAsString) == false)
                        doubleAsString = ldv.Inner;

                    instance.Add(GetOrCreateField(path, null, doubleAsString, null, storage, indexing, termVector));
                    newFields++;
                }
                else
                {
                    double dbl = 0;
                    if (value is double)
                        dbl = (double)value;
                    else if (value is decimal)
                        dbl = (double)(decimal)value;
                    else if (value is float)
                        dbl = (float)value;

                    instance.Add(GetOrCreateField(path, dbl.ToString("G"), null, null, storage, indexing, termVector));
                    newFields++;
                }
            }
            else if (valueType == ValueType.Convertible) // we need this to store numbers in invariant format, so JSON could read them
            {
                instance.Add(GetOrCreateField(path, ((IConvertible)value).ToString(CultureInfo.InvariantCulture), null, null, storage, indexing, termVector));
                newFields++;
            }

            foreach (var numericField in GetOrCreateNumericField(field, value, storage))
            {
                instance.Add(numericField);
                newFields++;
            }                

            return newFields;
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

            if (value is LazyDoubleValue || value is double || value is decimal || value is float) return ValueType.Double;

            if (value is AbstractField) return ValueType.Lucene;

            if (value is char) return ValueType.String;

            if (value is IConvertible) return ValueType.Convertible;

            if (value is BlittableJsonReaderObject) return ValueType.BlittableJsonObject;

            if (IsNumber(value)) return ValueType.Numeric;


            return ValueType.ConvertToJson;
        }

        protected Field GetOrCreateKeyField(LazyStringValue key)
        {
            if (_reduceOutput == false)
                return GetOrCreateField(Constants.Documents.Indexing.Fields.DocumentIdFieldName, null, key, null, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);

            return GetOrCreateField(Constants.Documents.Indexing.Fields.ReduceKeyFieldName, null, key, null, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
        }

        protected Field GetOrCreateField(string name, string value, LazyStringValue lazyValue, BlittableJsonReaderObject blittableValue, Field.Store store, Field.Index index, Field.TermVector termVector)
        {
            int cacheKey = FieldCacheKey.GetHashCode(name, index, store, termVector, _multipleItemsSameFieldCount);

            Field field;
            if (_fieldsCache.TryGetValue(cacheKey, out CachedFieldItem<Field> cached) == false ||
                !cached.Key.IsSame(name, index, store, termVector, _multipleItemsSameFieldCount))
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
                    if (value == null && lazyValue == null)
                        blittableReader = new BlittableObjectReader();

                    field = new Field(CreateFieldName(name),
                        value ?? LazyStringReader.GetStringFor(lazyValue) ?? blittableReader.GetStringFor(blittableValue),
                        store, index, termVector);
                }

                field.Boost = 1;
                field.OmitNorms = true;

                _fieldsCache[cacheKey] = new CachedFieldItem<Field>
                {
                    Key = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray()),
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
                    field.SetValue(value ?? LazyStringReader.GetStringFor(lazyValue) ?? cached.BlittableObjectReader.GetStringFor(blittableValue));
                }
            }

            return field;
        }

        private IEnumerable<AbstractField> GetOrCreateNumericField(IndexField field, object value, Field.Store storage, Field.TermVector termVector = Field.TermVector.NO)
        {
            var fieldNameDouble = field.Name + Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
            var fieldNameLong = field.Name + Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;

            var multipleItemsSameFieldCountArray = _multipleItemsSameFieldCount.ToArray();

            var numericFieldDouble = GetNumericFieldFromCache(fieldNameDouble, null, storage, termVector, multipleItemsSameFieldCountArray);
            var numericFieldLong = GetNumericFieldFromCache(fieldNameLong, null, storage, termVector, multipleItemsSameFieldCountArray);

            double doubleValue;
            long longValue;

            switch (BlittableNumber.Parse(value, out doubleValue, out longValue))
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

        private NumericField GetNumericFieldFromCache(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            int cacheKey = FieldCacheKey.GetHashCode(name, index, store, termVector, _multipleItemsSameFieldCount);

            NumericField numericField;
            if (_numericFieldsCache.TryGetValue(cacheKey, out CachedFieldItem<NumericField> cached) == false ||
                !cached.Key.IsSame(name, index, store, termVector, _multipleItemsSameFieldCount))
            {
                _numericFieldsCache[cacheKey] = new CachedFieldItem<NumericField>
                {
                    Key = new FieldCacheKey(name, index, store, termVector, _multipleItemsSameFieldCount.ToArray()),
                    Field = numericField = new NumericField(CreateFieldName(name), store, true)
                };
            }
            else
            {
                numericField = cached.Field;
            }

            return numericField;
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

        private static bool IsNumber(object value)
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

        private static unsafe bool TryToTrimTrailingZeros(LazyDoubleValue ldv, JsonOperationContext context, out LazyStringValue dblAsString)
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