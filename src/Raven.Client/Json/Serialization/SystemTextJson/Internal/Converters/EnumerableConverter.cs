using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class EnumerableConverter : JsonConverterFactory
    {
        public static readonly EnumerableConverter Instance = new();

        private Dictionary<Type, bool> _canConvertCache = new();

        private EnumerableConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            if (typeToConvert == null)
                return false;

            if (typeToConvert == typeof(string))
                return false;

            if (typeToConvert == typeof(LazyStringValue))
                return false;

            if (typeToConvert == typeof(BlittableJsonReaderArray))
                return false;

            if (_canConvertCache.TryGetValue(typeToConvert, out bool canConvert) == false)
            {
                canConvert = ComputeCanConvert(typeToConvert);

                // PERF: We are expecting a race condition here, this is an optimistic switch-on-change scheme.
                //       It mostly works because the frequency of this call is very low.
                _canConvertCache = new Dictionary<Type, bool>(_canConvertCache) { [typeToConvert] = canConvert };
            }

            return canConvert;
        }

        private static bool ComputeCanConvert(Type typeToConvert)
        {
            if (typeToConvert.IsArray)
            {
                if (typeToConvert.GetArrayRank() > 1)
                    return false;

                Type elementType = typeToConvert.GetElementType();
                return elementType != null && elementType != typeof(object);
            }

            foreach (Type interfaceType in typeToConvert.GetInterfaces())
            {
                if (interfaceType.IsGenericType == false)
                    continue;

                if (interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    Type itemType = interfaceType.GetGenericArguments()[0];
                    return itemType != typeof(object);
                }
            }

            return false;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type elementType = GetElementType(typeToConvert);
            Type converterType = typeof(EnumerableConverterInner<>).MakeGenericType(elementType);
            return (JsonConverter)Activator.CreateInstance(converterType, nonPublic: true);
        }

        private static Type GetElementType(Type typeToConvert)
        {
            if (typeToConvert.IsArray)
                return typeToConvert.GetElementType() ?? typeof(object);

            foreach (Type interfaceType in typeToConvert.GetInterfaces())
            {
                if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    return interfaceType.GetGenericArguments()[0];
            }

            return typeof(object);
        }

        private sealed class EnumerableConverterInner<T> : JsonConverter<IEnumerable<T>>
        {
            public override bool CanConvert(Type typeToConvert)
            {
                return typeof(IEnumerable<T>).IsAssignableFrom(typeToConvert);
            }

            public override IEnumerable<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(EnumerableConverter)} does not support reading.");
            }

            public override void Write(Utf8JsonWriter writer, IEnumerable<T> value, JsonSerializerOptions options)
            {
                if (value == null)
                {
                    writer.WriteNullValue();
                    return;
                }

                writer.WriteStartArray();
                foreach (T item in value)
                    JsonSerializer.Serialize(writer, item, typeof(T), options);
                writer.WriteEndArray();
            }
        }
    }
}
