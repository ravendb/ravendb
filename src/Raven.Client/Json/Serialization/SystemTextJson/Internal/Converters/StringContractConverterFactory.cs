using System;
using System.Collections;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class StringContractConverterFactory : JsonConverterFactory
    {
        public static readonly StringContractConverterFactory Instance = new();

        private const string NewtonsoftJsonContainerAttributeName = "Newtonsoft.Json.JsonContainerAttribute";

        private static readonly ConcurrentDictionary<Type, System.ComponentModel.TypeConverter> _converters = new();

        public override bool CanConvert(Type typeToConvert)
        {
            return typeof(Type).IsAssignableFrom(typeToConvert) || GetTypeConverter(typeToConvert) != null;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            if (typeof(Type).IsAssignableFrom(typeToConvert))
                return SystemTypeConverter.Instance;

            return (JsonConverter)Activator.CreateInstance(typeof(TypeDescriptorConverter<>).MakeGenericType(typeToConvert), GetTypeConverter(typeToConvert));
        }

        private static System.ComponentModel.TypeConverter GetTypeConverter(Type type)
        {
            return _converters.GetOrAdd(type, static t => IsWrittenAsString(t) ? TypeDescriptor.GetConverter(t) : null);
        }

        private static bool IsWrittenAsString(Type type)
        {
            if (type.IsPrimitive || type.IsEnum || type.IsPointer || type.IsByRef || type.ContainsGenericParameters || Nullable.GetUnderlyingType(type) != null)
                return false;

            if (type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeSpan) ||
                type == typeof(Guid) || type == typeof(Uri) || type == typeof(Version) || typeof(IEnumerable).IsAssignableFrom(type))
                return false;

            if (type.IsDefined(typeof(JsonConverterAttribute), inherit: true) || HasNewtonsoftContainerAttribute(type))
                return false;

            System.ComponentModel.TypeConverter converter = TypeDescriptor.GetConverter(type);
            Type converterType = converter.GetType();
            if (converterType == typeof(System.ComponentModel.TypeConverter) ||
                converterType.FullName is "System.ComponentModel.ComponentConverter" or "System.ComponentModel.ReferenceConverter" or "System.Windows.Forms.Design.DataSourceConverter")
                return false;

            return converter.CanConvertTo(typeof(string));
        }

        private static bool HasNewtonsoftContainerAttribute(Type type)
        {
            foreach (object attribute in type.GetCustomAttributes(true))
            {
                for (Type current = attribute.GetType(); current != null; current = current.BaseType)
                {
                    if (current.FullName == NewtonsoftJsonContainerAttributeName)
                        return true;
                }
            }

            return false;
        }

        private sealed class TypeDescriptorConverter<T> : JsonConverter<T>
        {
            private readonly System.ComponentModel.TypeConverter _converter;

            public TypeDescriptorConverter(System.ComponentModel.TypeConverter converter)
            {
                _converter = converter;
            }

            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.String)
                    throw new JsonException($"Expected a string for '{typeof(T).FullName}' but got {reader.TokenType}.");

                return (T)_converter.ConvertFromInvariantString(reader.GetString());
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(_converter.ConvertToInvariantString(value));
            }

            public override T ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return (T)_converter.ConvertFromInvariantString(reader.GetString());
            }

            public override void WriteAsPropertyName(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                writer.WritePropertyName(_converter.ConvertToInvariantString(value));
            }
        }

        private sealed class SystemTypeConverter : JsonConverter<Type>
        {
            public static readonly SystemTypeConverter Instance = new();

            public override bool CanConvert(Type typeToConvert) => typeof(Type).IsAssignableFrom(typeToConvert);

            public override Type Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.String)
                    throw new JsonException($"Expected a string for '{typeof(Type).FullName}' but got {reader.TokenType}.");

                return Type.GetType(reader.GetString(), throwOnError: true);
            }

            public override void Write(Utf8JsonWriter writer, Type value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.AssemblyQualifiedName);
            }
        }
    }
}
