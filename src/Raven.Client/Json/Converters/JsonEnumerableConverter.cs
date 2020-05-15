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

        public override bool CanRead => false;

        public override bool CanConvert(Type objectType)
        {
            if (objectType.IsArray)
            {
                var elementType = objectType.GetElementType();
                return elementType != typeof(byte);
            }

            if (objectType.IsGenericType == false)
                return objectType == typeof(Enumerable);

            var genericType = objectType.GetGenericTypeDefinition();
            if (typeof(Dictionary<,>).IsAssignableFrom(genericType))
                return false;

            return typeof(IEnumerable).IsAssignableFrom(objectType.GetGenericTypeDefinition());
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
