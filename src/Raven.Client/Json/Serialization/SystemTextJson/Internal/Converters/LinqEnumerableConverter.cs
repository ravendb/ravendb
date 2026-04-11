using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    /// <summary>
    /// This converter is used when a property is a Linq-To-Entities query, enumerating and
    /// then serializing it as a json array.
    /// </summary>
    internal sealed class LinqEnumerableConverter : JsonConverterFactory
    {
        public static readonly LinqEnumerableConverter Instance = new();

        private Dictionary<Type, bool> _canConvertCache = new();

        private LinqEnumerableConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            if (typeToConvert.Namespace == null || typeToConvert == typeof(string) || typeToConvert.IsClass == false)
                return false;

            if (_canConvertCache.TryGetValue(typeToConvert, out bool canConvert) == false)
            {
                canConvert = false;
                foreach (Type interfaceType in typeToConvert.GetInterfaces())
                {
                    if (interfaceType.IsGenericType == false)
                        continue;

                    Type genericInterfaceType = interfaceType.GetGenericTypeDefinition();
                    if (typeof(IEnumerable<>) == genericInterfaceType && typeToConvert.Namespace.StartsWith("System.Linq"))
                    {
                        canConvert = true;
                        break;
                    }
                }

                // PERF: We are expecting a race condition here, this is an optimistic switch-on-change scheme.
                //       It mostly works because the frequency of this call is very low.
                _canConvertCache = new Dictionary<Type, bool>(_canConvertCache) { [typeToConvert] = canConvert };
            }

            return canConvert;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type elementType = GetEnumerableElementType(typeToConvert);
            Type converterType = typeof(LinqEnumerableConverterInner<>).MakeGenericType(elementType);
            return (JsonConverter)Activator.CreateInstance(converterType, nonPublic: true);
        }

        private static Type GetEnumerableElementType(Type type)
        {
            foreach (Type interfaceType in type.GetInterfaces())
            {
                if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    return interfaceType.GetGenericArguments()[0];
            }

            return typeof(object);
        }

        private sealed class LinqEnumerableConverterInner<T> : JsonConverter<IEnumerable<T>>
        {
            public override bool CanConvert(Type typeToConvert)
            {
                return typeof(IEnumerable<T>).IsAssignableFrom(typeToConvert);
            }

            public override IEnumerable<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(LinqEnumerableConverter)} should not be used to deserialize collections from json - if this exception gets thrown, it is probably a bug.");
            }

            public override void Write(Utf8JsonWriter writer, IEnumerable<T> value, JsonSerializerOptions options)
            {
                writer.WriteStartArray();
                foreach (T item in value)
                    JsonSerializer.Serialize(writer, item, options);
                writer.WriteEndArray();
            }
        }
    }
}
