using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Lucene.Net.Documents;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Transformers;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Abstractions.Data;

namespace Raven.Server.Utils
{
    internal class TypeConverter
    {
        private const string TypePropertyName = "$type";

        private const string ValuesPropertyName = "$values";

        private static readonly string TypeList = typeof(List<>).FullName;

        private static readonly ConcurrentDictionary<Type, PropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();

        public static object ToBlittableSupportedType(object value, bool flattenArrays = false)
        {
            if (value == null || value is DynamicNullObject)
                return null;

            var dynamicDocument = value as DynamicBlittableJson;
            if (dynamicDocument != null)
                return dynamicDocument.BlittableJson;

            var transformerParameter = value as TransformerParameter;
            if (transformerParameter != null)
                return transformerParameter.OriginalValue;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is bool)
                return value;

            if (value is int || value is long || value is double || value is decimal || value is float)
                return value;

            if (value is LazyDoubleValue)
                return value;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return value;

            if (value is Guid)
                return ((Guid)value).ToString("D");

            if (value is Enum)
                return value.ToString();

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return Constants.Indexing.Fields.IgnoredDynamicField;

            var dictionary = value as IDictionary;
            if (dictionary != null)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dictionary.Keys)
                    @object[key.ToString()] = ToBlittableSupportedType(dictionary[key]);

                return @object;
            }

            var charEnumerable = value as IEnumerable<char>;
            if (charEnumerable != null)
                return new string(charEnumerable.ToArray());

            var bytes = value as byte[];
            if (bytes != null)
                return System.Convert.ToBase64String(bytes);

            var enumerable = value as IEnumerable;
            if (enumerable != null)
            {
                if (ShouldTreatAsEnumerable(enumerable))
                {
                    IEnumerable<object> items;

                    var objectEnumerable = value as IEnumerable<object>;
                    if (objectEnumerable != null)
                        items = objectEnumerable.Select(x => ToBlittableSupportedType(x, flattenArrays));
                    else
                        items = enumerable.Cast<object>().Select(x => ToBlittableSupportedType(x, flattenArrays));

                    return new DynamicJsonArray(flattenArrays ? Flatten(items) : items);
                }
            }

            var inner = new DynamicJsonValue();
            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.PropertiesInOrder)
            {
                var propertyValue = property.Value.GetValue(value);
                var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                if (propertyValueAsEnumerable != null && ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(x => ToBlittableSupportedType(x)));
                    continue;
                }

                inner[property.Key] = ToBlittableSupportedType(propertyValue);
            }

            return inner;
        }

        private static IEnumerable<object> Flatten(IEnumerable items)
        {
            foreach (var item in items)
            {
                var enumerable = item as IEnumerable;

                if (enumerable != null && ShouldTreatAsEnumerable(enumerable))
                {
                    foreach (var nestedItem in Flatten(enumerable))
                    {
                        yield return nestedItem;
                    }

                    yield break;
                }

                yield return item;
            }
        }

        public static dynamic ToDynamicType(object value)
        {
            if (value == null)
                return DynamicNullObject.Null;

            var jsonObject = value as BlittableJsonReaderObject;
            if (jsonObject != null)
                return new DynamicBlittableJson(jsonObject);

            var jsonArray = value as BlittableJsonReaderArray;
            if (jsonArray != null)
                return new DynamicArray(jsonArray);

            return ConvertForIndexing(value);
        }

        public static unsafe object ConvertForIndexing(object value)
        {
            if (value == null)
                return null;

            var blittableJsonObject = value as BlittableJsonReaderObject;
            if (blittableJsonObject != null)
            {
                string type;
                if (blittableJsonObject.TryGet(TypePropertyName, out type) == false)
                    return blittableJsonObject;

                if (type == null)
                    return blittableJsonObject;

                if (type.StartsWith(TypeList) == false)
                    return blittableJsonObject;

                // TODO [ppekrol] probably we will have to support many more cases here

                BlittableJsonReaderArray values;
                if (blittableJsonObject.TryGet(ValuesPropertyName, out values))
                    return values;

                throw new NotSupportedException($"Detected list type '{type}' but could not extract '{values}'.");
            }

            var lazyString = value as LazyStringValue;
            if (lazyString == null)
            {
                var lazyCompressedStringValue = value as LazyCompressedStringValue;
                if (lazyCompressedStringValue != null)
                    lazyString = lazyCompressedStringValue.ToLazyStringValue();
            }

            if (lazyString != null)
            {
                DateTime dt;
                DateTimeOffset dto;
                switch (LazyStringParser.TryParseDateTime(lazyString.Buffer, lazyString.Size, out dt, out dto))
                {
                    case LazyStringParser.Result.DateTime:
                        return dt;
                    case LazyStringParser.Result.DateTimeOffset:
                        return dto;
                }

                TimeSpan ts;
                if (LazyStringParser.TryParseTimeSpan(lazyString.Buffer, lazyString.Size, out ts))
                    return ts;

                return lazyString; // ensure that the decompressed lazy string value is returned
            }

            return value;
        }

        public static T Convert<T>(object value, bool cast)
        {
            if (value == null || value is DynamicNullObject)
                return default(T);


            if (cast)
            {
                // HACK
                return (T)value;
            }
          
            if (value is T)
                return (T)value;

            Type targetType = typeof(T);

            if (targetType.GetTypeInfo().IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                targetType = Nullable.GetUnderlyingType(targetType);
            }

            if (targetType == typeof(Guid))
            {
                return (T)(object)new Guid(value.ToString());
            }

            if (targetType == typeof(string))
            {
                return (T)(object)value.ToString();
            }

            if (targetType == typeof(DateTime))
            {
                if (value is DateTimeOffset)
                    return (T)(object)((DateTimeOffset)value).DateTime;

                var s = value as string;
                if (s == null)
                {
                    var lzv = value as LazyStringValue;
                    if (lzv != null)
                        s = lzv;
                }

                if (s != null)
                {
                    DateTime dateTime;
                    if (DateTime.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out dateTime))
                        return (T)(object)dateTime;

                    dateTime = RavenJsonTextReader.ParseDateMicrosoft(s);
                    return (T)(object)dateTime;
                }
            }

            if (targetType == typeof(DateTimeOffset))
            {
                var s = value as string ?? value as LazyStringValue;

                if (s != null)
                {
                    DateTimeOffset dateTimeOffset;
                    if (DateTimeOffset.TryParseExact(s, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out dateTimeOffset))
                        return (T)(object)dateTimeOffset;

                    return default(T);
                }
            }

            var lsv = value as LazyStringValue;
            if (lsv != null)
                value = (string)lsv;

            try
            {
                return (T)System.Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined ", value), e);
            }
        }

        public static PropertyAccessor GetPropertyAccessor(object value)
        {
            var type = value.GetType();
            return PropertyAccessorCache.GetOrAdd(type, PropertyAccessor.Create);
        }

        public static PropertyAccessor GetPropertyAccessorForMapReduceOutput(object value, HashSet<string> groupByFields)
        {
            var type = value.GetType();
            return PropertyAccessorCache.GetOrAdd(type, x => PropertyAccessor.CreateMapReduceOutputAccessor(x, groupByFields));
        }

        public static bool ShouldTreatAsEnumerable(object item)
        {
            if (item == null || item is DynamicNullObject)
                return false;

            if (item is DynamicBlittableJson)
                return false;

            if (item is string || item is LazyStringValue)
                return false;

            if (item is IDictionary)
                return false;

            return true;
        }
    }
}