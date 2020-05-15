using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonEnumerableConverter : RavenJsonConverter
    {
        public static JsonEnumerableConverter Instance = new JsonEnumerableConverter();

        private Dictionary<Type, bool> _cache = new Dictionary<Type, bool>();

        public override bool CanRead => false;

        public override bool CanConvert(Type objectType)
        {
            if (_cache.TryGetValue(objectType, out bool canConvert) == false)
                _cache[objectType] = canConvert = CanConvertInternal(objectType);

            return canConvert;
        }

        private bool CanConvertInternal(Type objectType)
        {
            if (objectType.IsArray)
                return CanConvertElementType(objectType.GetElementType());

            if (objectType.IsGenericType == false)
                return objectType == typeof(Enumerable);

            var genericType = objectType.GetGenericTypeDefinition();
            if (typeof(Dictionary<,>).IsAssignableFrom(genericType))
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
