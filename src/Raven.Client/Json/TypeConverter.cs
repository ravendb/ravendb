using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Conventions;
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

            if (type == typeof(int) || type == typeof(long) || type == typeof(double) || type == typeof(decimal) || type == typeof(float))
                return value;

            if (type == typeof(LazyNumberValue))
                return value;

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan))
                return value;

            if (type == typeof(Guid))
                return ((Guid)value).ToString("D");

            if (type.GetTypeInfo().IsSubclassOf(typeof(Enum)))
                return value.ToString();

            var dictionary = value as IDictionary;
            if (dictionary != null)
            {
                var @object = new DynamicJsonValue();
                foreach (var key in dictionary.Keys)
                    @object[key.ToString()] = ToBlittableSupportedType(dictionary[key], conventions, context);

                return @object;
            }

            var enumerable = value as IEnumerable;
            if (enumerable != null)
            {
                var objectEnumerable = value as IEnumerable<object>;
                var items = objectEnumerable != null
                    ? objectEnumerable.Select(x => ToBlittableSupportedType(x, conventions, context))
                    : enumerable.Cast<object>().Select(x => ToBlittableSupportedType(x, conventions, context));

                return new DynamicJsonArray(items);
            }

            using (var writer = new BlittableJsonWriter(context))
            {
                var serializer = conventions.CreateSerializer();

                serializer.Serialize(writer, value);
                writer.FinalizeDocument();

                return writer.CreateReader();
            }
        }
    }
}
