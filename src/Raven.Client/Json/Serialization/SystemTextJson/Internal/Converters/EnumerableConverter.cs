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

            if (typeof(IDictionary).IsAssignableFrom(typeToConvert))
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

            // Only handle interface and abstract types — concrete collection types (List<T>, HashSet<T>,
            // Queue<T>, etc.) are already handled by STJ's built-in converters. If we intercept them,
            // our Read path always returns List<T>, which breaks for HashSet<T>, Queue<T>, etc.
            if (typeToConvert.IsInterface == false && typeToConvert.IsAbstract == false)
                return false;

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

            // Also check the type itself (it may be the IEnumerable<T> interface directly)
            if (typeToConvert.IsGenericType && typeToConvert.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                Type itemType = typeToConvert.GetGenericArguments()[0];
                return itemType != typeof(object);
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
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException($"Expected StartArray token but got {reader.TokenType}.");

                var list = new List<T>();
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndArray)
                        break;

                    T item = JsonSerializer.Deserialize<T>(ref reader, options);
                    list.Add(item);
                }

                if (typeToConvert.IsArray)
                    return list.ToArray();

                return list;
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
