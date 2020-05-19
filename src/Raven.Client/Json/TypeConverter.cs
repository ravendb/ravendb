using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json
{
    internal static class TypeConverter
    {
        public static object ToBlittableSupportedType(object value, DocumentConventions conventions, JsonOperationContext context)
        {
            if (value == null)
                return null;

            if (value is BlittableJsonReaderObject)
                return value;

            var type = value.GetType();
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                type = underlyingType;

            if (type == typeof(string))
                return value;

            if (type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue))
                return value;

            if (type == typeof(bool))
                return value;

            if (type == typeof(int) || type == typeof(uint) ||
                type == typeof(decimal) || type == typeof(double) || type == typeof(float) ||
                type == typeof(long) || type == typeof(ulong) ||
                type == typeof(short) || type == typeof(ushort) ||
                type == typeof(byte) || type == typeof(sbyte))
                return value;

            if (type == typeof(LazyNumberValue))
                return value;

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan))
                return value;

            if (type == typeof(Guid))
                return ((Guid)value).ToString("D");

            if (type.GetTypeInfo().IsSubclassOf(typeof(Enum)))
                return value.ToString();

            if (value is byte[] bytes)
            {
                return Convert.ToBase64String(bytes);
            }

            if (value is IDictionary dictionary)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dictionary.Keys)
                {
                    var keyAsString = KeyAsString(key: ToBlittableSupportedType(key, conventions, context));
                    @object[keyAsString] = ToBlittableSupportedType(dictionary[key], conventions, context);
                }

                return @object;
            }

            if (value is IDictionary<object, object> dDictionary)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dDictionary.Keys)
                {
                    var keyAsString = KeyAsString(key: ToBlittableSupportedType(key, conventions, context));
                    @object[keyAsString] = ToBlittableSupportedType(dDictionary[key], conventions, context);
                }

                return @object;
            }

            if (value is IEnumerable enumerable)
            {
                var dja = new DynamicJsonArray();

                foreach (var x in enumerable)
                {
                    dja.Add(ToBlittableSupportedType(x, conventions, context));
                }

                return dja;
            }

            using (var writer = conventions.Serialization.CreateWriter(context))
            {
                var serializer = conventions.Serialization.CreateSerializer();

                writer.WriteStartObject();
                writer.WritePropertyName("Value");

                serializer.Serialize(writer, value);

                writer.WriteEndObject();

                writer.FinalizeDocument();

                var reader = writer.CreateReader();
                return reader["Value"];
            }
        }

        private static string KeyAsString(object key)
        {
            string kvpKeyAsString;
            switch (key)
            {
                case null:
                    kvpKeyAsString = Constants.Documents.Indexing.Fields.NullValue;
                    break;
                case LazyStringValue lsv:
                    kvpKeyAsString = lsv.Size == 0 ? Constants.Documents.Indexing.Fields.EmptyString : lsv;
                    break;
                case LazyCompressedStringValue lcsv:
                    kvpKeyAsString = lcsv.ToLazyStringValue();
                    break;
                case DateTime dateTime:
                    kvpKeyAsString = dateTime.GetDefaultRavenFormat();
                    break;
                case DateTimeOffset dateTimeOffset:
                    kvpKeyAsString = dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(isUtc: true);
                    break;
                case TimeSpan timeSpan:
                    kvpKeyAsString = timeSpan.ToString("c", CultureInfo.InvariantCulture);
                    break;
                default:
                    kvpKeyAsString = key.ToString();
                    break;
            }

            return kvpKeyAsString;
        }

    }
}
