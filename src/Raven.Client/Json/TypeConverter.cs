using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Json
{
    internal static class TypeConverter
    {

        private enum BlittableSupportedReturnType : int
        {
            Null = 0,
            Same = 1,
            Runtime = 3,
            Dictionary = 5,
            GenericDictionary = 6,
            Enumerable = 7,
            String = 8
        }

        private static readonly TypeCache<BlittableSupportedReturnType> _supportedTypeCache = new(512);

        private static BlittableSupportedReturnType DoBlittableSupportedTypeInternal(Type type, object value)
        {
            if (type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue) || type == typeof(LazyNumberValue) ||
                type == typeof(string) || type == typeof(bool) || type == typeof(int) || type == typeof(long) ||
                type == typeof(double) || type == typeof(decimal) || type == typeof(float) || type == typeof(short) ||
                type == typeof(byte) || value is BlittableJsonReaderObject ||
                type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan))
                return BlittableSupportedReturnType.Same;

            if (value is IDictionary)
                return BlittableSupportedReturnType.Dictionary;

            if (value is IDictionary<object, object>)
                return BlittableSupportedReturnType.GenericDictionary;

            if (value is Enum)
                return BlittableSupportedReturnType.String;

            if (type == typeof(byte[]) || value is IEnumerable)
                return BlittableSupportedReturnType.Enumerable;

            // If it is a Nullable type we are going to get the underlying Type. 
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null)
                return DoBlittableSupportedTypeInternal(underlyingType, value);

            return BlittableSupportedReturnType.Runtime;
        }


        public static object ToBlittableSupportedType(object value, DocumentConventions conventions, JsonOperationContext context)
        {
            if (value == null)
                return null;

            // We cache the return type.
            var type = value.GetType();
            if (!_supportedTypeCache.TryGet(type, out BlittableSupportedReturnType returnType))
            {
                returnType = DoBlittableSupportedTypeInternal(type ,value);
                _supportedTypeCache.Put(type, returnType);
            }

            if (returnType == BlittableSupportedReturnType.Same)
                return value;

            switch (returnType)
            {
                case BlittableSupportedReturnType.Null:
                    return null;
                case BlittableSupportedReturnType.String:
                    return value.ToString();
                case BlittableSupportedReturnType.Enumerable:
                    {
                        if (value is byte[] bytes)
                            return Convert.ToBase64String(bytes);

                        var dja = new DynamicJsonArray();

                        var enumerable = (IEnumerable)value;
                        foreach (var x in enumerable)
                        {
                            dja.Add(ToBlittableSupportedType(x, conventions, context));
                        }

                        return dja;
                    }
                case BlittableSupportedReturnType.Dictionary:
                    {
                        var @object = new DynamicJsonValue();

                        var dictionary = (IDictionary)value;
                        foreach (var key in dictionary.Keys)
                        {
                            var keyAsString = KeyAsString(key: ToBlittableSupportedType(key, conventions, context));
                            @object[keyAsString] = ToBlittableSupportedType(dictionary[key], conventions, context);
                        }

                        return @object;
                    }
                case BlittableSupportedReturnType.GenericDictionary:
                    {
                        var @object = new DynamicJsonValue();

                        var dDictionary = (IDictionary<object, object>)value;
                        foreach (var key in dDictionary.Keys)
                        {
                            var keyAsString = KeyAsString(key: ToBlittableSupportedType(key, conventions, context));
                            @object[keyAsString] = ToBlittableSupportedType(dDictionary[key], conventions, context);
                        }

                        return @object;
                    }
            }

            if (value is Guid guid)
                return guid.ToString("D");

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
