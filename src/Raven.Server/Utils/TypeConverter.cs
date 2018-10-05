using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Lucene.Net.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Exceptions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Extensions;

namespace Raven.Server.Utils
{
    internal static class TypeConverter
    {
        private const string TypePropertyName = "$type";

        private const string ValuesPropertyName = "$values";

        private static readonly string TypeList = typeof(List<>).FullName;

        private static readonly ConcurrentDictionary<Type, PropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();

        public static bool IsSupportedType(object value)
        {
            if (value == null || value is DynamicNullObject)
                return true;

            if (value is DynamicBlittableJson)
                return true;

            if (value is string)
                return true;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return true;

            if (value is bool)
                return true;

            if (value is int || value is long || value is double || value is decimal || value is float)
                return true;

            if (value is LazyNumberValue)
                return true;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return true;

            if (value is Guid)
                return true;

            if (value is Enum)
                return true;

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return true;

            List<bool> supporeted = new List<bool>();

            if (value is IDictionary dictionary)
            {
                foreach (var key in dictionary.Keys)
                    supporeted.Add(IsSupportedType(dictionary[key]));

                return supporeted.All(v => v);
            }

            if (value is IEnumerable<char>)
                return true;

            if (value is byte[])
                return true;

            if (value is IEnumerable enumerable)
            {
                if (ShouldTreatAsEnumerable(enumerable))
                {
                    var objectEnumerable = value as IEnumerable<object>;
                    var supporetedEnumerable = objectEnumerable?.Select(IsSupportedType) ?? enumerable.Cast<object>().Select(IsSupportedType);

                    return supporetedEnumerable.All(v => v);
                }
            }

            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.PropertiesInOrder)
            {
                var propertyValue = property.Value.GetValue(value);
                var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                if (propertyValueAsEnumerable != null && ShouldTreatAsEnumerable(propertyValue))
                {
                    supporeted.Add(propertyValueAsEnumerable.Select(IsSupportedType).All(v => v));
                    continue;
                }

                supporeted.Add(IsSupportedType(propertyValue));
            }

            return supporeted.All(v => v);
        }

        public static object ToBlittableSupportedType(object value, bool flattenArrays = false)
        {
            return ToBlittableSupportedType(value, value, flattenArrays, 0);
        }

        private static object ToBlittableSupportedType(object root, object value, bool flattenArrays, int recursiveLevel)
        {
            if (recursiveLevel > MaxAllowedRecursiveLevelForType)
            {
                NestingLevelTooDeep(root);
            }
            if (value == null || value is DynamicNullObject)
                return null;

            var dynamicDocument = value as DynamicBlittableJson;
            if (dynamicDocument != null)
                return dynamicDocument.BlittableJson;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is bool)
                return value;

            if (value is int || value is long || value is double || value is decimal || value is float || value is short || value is byte)
                return value;

            if (value is LazyNumberValue)
                return value;

            if (value is DateTime || value is DateTimeOffset || value is TimeSpan)
                return value;

            if (value is Guid)
                return ((Guid)value).ToString("D");

            if (value is Enum)
                return value.ToString();

            if (value is IEnumerable<IFieldable> || value is IFieldable)
                return "__ignored";

            var dictionary = value as IDictionary;
            if (dictionary != null)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dictionary.Keys)
                    @object[key.ToString()] = ToBlittableSupportedType(root, dictionary[key], flattenArrays, recursiveLevel: recursiveLevel + 1);

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
                        items = Enumerable.Select(objectEnumerable, x => ToBlittableSupportedType(root, x, flattenArrays, recursiveLevel: recursiveLevel + 1));
                    else
                        items = Enumerable.Select(enumerable.Cast<object>(), x => ToBlittableSupportedType(root, x, flattenArrays, recursiveLevel: recursiveLevel + 1));

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
                    inner[property.Key] = new DynamicJsonArray(Enumerable.Select(propertyValueAsEnumerable, x => ToBlittableSupportedType(root, x, flattenArrays, recursiveLevel: recursiveLevel + 1)));
                    continue;
                }

                inner[property.Key] = ToBlittableSupportedType(root, propertyValue, flattenArrays, recursiveLevel: recursiveLevel + 1);
            }

            return inner;
        }

        private static void NestingLevelTooDeep(object value)
        {
            throw new SerializationNestedLevelTooDeepException(
                                $"Reached nesting level of {MaxAllowedRecursiveLevelForType} for type {value.GetType().Name}, reccursive types that exceed the allowed nesting level are not supported.");
        }

        public static int MaxAllowedRecursiveLevelForType { get; private set; } = 100;

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
            if (value is DynamicNullObject)
                return value;

            if (value == null)
                return DynamicNullObject.ExplicitNull;

            BlittableJsonReaderArray jsonArray;
            var jsonObject = value as BlittableJsonReaderObject;
            if (jsonObject != null)
            {
                if (jsonObject.TryGetWithoutThrowingOnError("$values", out jsonArray))
                    return new DynamicArray(jsonArray);

                return new DynamicBlittableJson(jsonObject);
            }

            jsonArray = value as BlittableJsonReaderArray;
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
                if (blittableJsonObject.TryGet(TypePropertyName, out string type) == false)
                    return blittableJsonObject;

                if (type == null)
                    return blittableJsonObject;

                if (type.StartsWith(TypeList) == false)
                    return blittableJsonObject;

                if (blittableJsonObject.TryGet(ValuesPropertyName, out BlittableJsonReaderArray values))
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
                var result = LazyStringParser.TryParseDateTime(lazyString.Buffer, lazyString.Size, out DateTime dt, out DateTimeOffset dto);
                if (result == LazyStringParser.Result.DateTime)
                    return dt;
                if (result == LazyStringParser.Result.DateTimeOffset)
                    return dto;

                if (LazyStringParser.TryParseTimeSpan(lazyString.Buffer, lazyString.Size, out TimeSpan ts))
                    return ts;

                return lazyString; // ensure that the decompressed lazy string value is returned
            }

            if (value is string s)
            {
                fixed (char* str = s)
                {
                    var result = LazyStringParser.TryParseDateTime(str, s.Length, out DateTime dt, out DateTimeOffset dto);
                    if (result == LazyStringParser.Result.DateTime)
                        return dt;
                    if (result == LazyStringParser.Result.DateTimeOffset)
                        return dto;

                    if (LazyStringParser.TryParseTimeSpan(str, s.Length, out TimeSpan ts))
                        return ts;
                }
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
                    if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out DateTime dateTime))
                        return (T)(object)dateTime;

                    dateTime = RavenDateTimeExtensions.ParseDateMicrosoft(s);
                    return (T)(object)dateTime;
                }
            }

            if (targetType == typeof(DateTimeOffset))
            {
                var s = value as string ?? value as LazyStringValue;

                if (s != null)
                {
                    if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind, out DateTimeOffset dateTimeOffset))
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

            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorCache.GetOrAdd(type, x => PropertyAccessor.Create(type, value));
        }

        public static PropertyAccessor GetPropertyAccessorForMapReduceOutput(object value, HashSet<CompiledIndexField> groupByFields)
        {
            var type = value.GetType();

            if (value is Dictionary<string, object>) // don't use cache when using dictionaries
                return PropertyAccessor.Create(type, value);

            return PropertyAccessorCache.GetOrAdd(type, x => PropertyAccessor.CreateMapReduceOutputAccessor(type, value, groupByFields));
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
