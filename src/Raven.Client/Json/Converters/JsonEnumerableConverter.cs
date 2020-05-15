using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonEnumerableConverter : RavenJsonConverter
    {
        public static JsonEnumerableConverter Instance = new JsonEnumerableConverter();

        private readonly ConcurrentDictionary<Type, bool> _cache = new ConcurrentDictionary<Type, bool>();

        public override bool CanRead => false;

        public override bool CanConvert(Type objectType)
        {
            if (objectType == null)
                return false;

            return _cache.GetOrAdd(objectType, CanConvertInternal);
        }

        private bool CanConvertInternal(Type objectType)
        {
            if (objectType.IsArray)
            {
                if (objectType.GetArrayRank() != 1)
                    return false;

                return CanConvertElementType(objectType.GetElementType());
            }

            if (objectType.IsGenericType == false)
                return objectType == typeof(Enumerable);

            if (typeof(IDictionary).IsAssignableFrom(objectType))
                return false;

            var genericType = objectType.GetGenericTypeDefinition();
            if (typeof(IDictionary<,>).IsAssignableFrom(genericType) || typeof(Dictionary<,>).IsAssignableFrom(genericType))
                return false;

            var isEnumerable = typeof(IEnumerable).IsAssignableFrom(genericType);
            if (isEnumerable == false)
                return false;

            return CanConvertElementType(objectType.GetGenericArguments()[0]);

            static bool CanConvertElementType(Type elementType)
            {
                return elementType != typeof(byte) && elementType != typeof(object);
            }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotSupportedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            writer.WriteStartArray();

            foreach (object val in (IEnumerable)value)
                serializer.Serialize(writer, val);

            writer.WriteEndArray();
        }
    }
}
