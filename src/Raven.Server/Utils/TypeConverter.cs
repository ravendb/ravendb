using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Transformers;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    internal class TypeConverter
    {
        private static readonly ConcurrentDictionary<Type, PropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();

        public static object ConvertType(object value, JsonOperationContext context)
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

            if (value is DateTime)
                return value;

            var dictionary = value as IDictionary;
            if (dictionary != null)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dictionary.Keys)
                    @object[key.ToString()] = ConvertType(dictionary[key], context);

                return @object;
            }

            var objectEnumerable = value as IEnumerable<object>;
            if (objectEnumerable != null)
            {
                if (AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(objectEnumerable))
                    return new DynamicJsonArray(objectEnumerable.Select(x => ConvertType(x, context)));
            }

            var charEnumerable = value as IEnumerable<char>;
            if (charEnumerable != null)
                return new string(charEnumerable.ToArray());

            var inner = new DynamicJsonValue();
            var accessor = GetPropertyAccessor(value);

            foreach (var property in accessor.Properties)
            {
                var propertyValue = property.Value(value);
                var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                if (propertyValueAsEnumerable != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(x => ConvertType(x, context)));
                    continue;
                }

                inner[property.Key] = ConvertType(propertyValue, context);
            }

            return inner;
        }

        public static unsafe dynamic DynamicConvert(object value)
        {
            if (value == null)
                return DynamicNullObject.Null;

            var jsonObject = value as BlittableJsonReaderObject;
            if (jsonObject != null)
                return new DynamicBlittableJson(jsonObject);

            var jsonArray = value as BlittableJsonReaderArray;
            if (jsonArray != null)
                return new DynamicArray(jsonArray);

            var lazyString = value as LazyStringValue;
            if (lazyString == null)
            {
                var lazyCompressedStringValue = value as LazyCompressedStringValue;
                if (lazyCompressedStringValue != null)
                    lazyString = lazyCompressedStringValue.ToLazyStringValue();
            }

            if (lazyString != null)
            {
                if (lazyString.Size == 0)
                    return value;

                var firstChar = (char)lazyString.Buffer[0];

                //optimizations, don't try to call TryParse if first char isn't a digit or '-'
                if (Char.IsDigit(firstChar) == false && firstChar != '-')
                    return value;

                // optimize this
                var valueAsString = lazyString.ToString();

                DateTime dateTime;
                if (DateTime.TryParseExact(valueAsString, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
                {
                    if (valueAsString.EndsWith("Z"))
                        return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                    return dateTime;
                }

                DateTimeOffset dateTimeOffset;
                if (DateTimeOffset.TryParseExact(valueAsString, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
                    return dateTimeOffset;

                TimeSpan timeSpan;
                if (valueAsString.Contains(":") && valueAsString.Length >= 6 && TimeSpan.TryParseExact(valueAsString, "c", CultureInfo.InvariantCulture, out timeSpan))
                    return timeSpan;
            }

            return value;
        }

        public static T Convert<T>(object value, bool cast)
        {
            if (cast)
            {
                // HACK
                return (T)value;
            }

            if (value == null)
                return default(T);

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
                var s = value as string ?? value as LazyStringValue;

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

            try
            {
                return (T)System.Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(String.Format("Unable to find suitable conversion for {0} since it is not predefined ", value), e);
            }
        }

        public static PropertyAccessor GetPropertyAccessor(object value)
        {
            var type = value.GetType();
            return PropertyAccessorCache.GetOrAdd(type, PropertyAccessor.Create);
        }
    }
}