using System;
using System.Globalization;
using System.Reflection;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    internal class TypeConverter
    {
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
                if (char.IsDigit(firstChar) == false && firstChar != '-')
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
                throw new InvalidOperationException(string.Format("Unable to find suitable conversion for {0} since it is not predefined ", value), e);
            }
        }
    }
}