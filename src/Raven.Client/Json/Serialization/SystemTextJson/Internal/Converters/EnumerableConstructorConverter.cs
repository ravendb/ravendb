using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class EnumerableConstructorConverter : JsonConverterFactory
    {
        public static readonly EnumerableConstructorConverter Instance = new();

        private EnumerableConstructorConverter()
        {
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return GetConstructor(typeToConvert, out _) != null;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            ConstructorInfo constructor = GetConstructor(typeToConvert, out Type elementType);
            Type converterType = typeof(EnumerableConstructorConverterInner<,>).MakeGenericType(typeToConvert, elementType);
            return (JsonConverter)Activator.CreateInstance(converterType, constructor);
        }

        private static ConstructorInfo GetConstructor(Type type, out Type elementType)
        {
            elementType = null;

            if (type.IsClass == false || type.IsAbstract || type.IsArray || type == typeof(string))
                return null;

            if (type.Namespace != null && (type.Namespace == "System" || type.Namespace.StartsWith("System.", StringComparison.Ordinal)))
                return null;

            if (typeof(IDictionary).IsAssignableFrom(type) || typeof(IList).IsAssignableFrom(type))
                return null;

            Type[] enumerableInterfaces = type.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                .ToArray();

            if (enumerableInterfaces.Length != 1)
                return null;

            Type candidateElementType = enumerableInterfaces[0].GetGenericArguments()[0];
            if (typeof(ICollection<>).MakeGenericType(candidateElementType).IsAssignableFrom(type))
                return null;

            Type listType = typeof(List<>).MakeGenericType(candidateElementType);
            foreach (ConstructorInfo constructor in type.GetConstructors())
            {
                ParameterInfo[] parameters = constructor.GetParameters();
                if (parameters.Length == 1 && parameters[0].ParameterType.IsAssignableFrom(listType))
                {
                    elementType = candidateElementType;
                    return constructor;
                }
            }

            return null;
        }

        private sealed class EnumerableConstructorConverterInner<TEnumerable, TElement> : JsonConverter<TEnumerable>
            where TEnumerable : IEnumerable<TElement>
        {
            private readonly ConstructorInfo _constructor;

            public EnumerableConstructorConverterInner(ConstructorInfo constructor)
            {
                _constructor = constructor;
            }

            public override TEnumerable Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                List<TElement> items = JsonSerializer.Deserialize<List<TElement>>(ref reader, options);
                return (TEnumerable)_constructor.Invoke([items]);
            }

            public override void Write(Utf8JsonWriter writer, TEnumerable value, JsonSerializerOptions options)
            {
                writer.WriteStartArray();
                foreach (TElement item in value)
                    JsonSerializer.Serialize(writer, item, options);
                writer.WriteEndArray();
            }
        }
    }
}
