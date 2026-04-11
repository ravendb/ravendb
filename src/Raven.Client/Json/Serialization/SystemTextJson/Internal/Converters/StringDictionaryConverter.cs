using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters
{
    internal sealed class StringDictionaryConverter : JsonConverterFactory
    {
        private static readonly StringDictionaryConverter CurrentCulture = new(StringComparison.CurrentCulture);

        private static readonly StringDictionaryConverter CurrentCultureIgnoreCase = new(StringComparison.CurrentCultureIgnoreCase);

        private static readonly StringDictionaryConverter InvariantCulture = new(StringComparison.InvariantCulture);

        private static readonly StringDictionaryConverter InvariantCultureIgnoreCase = new(StringComparison.InvariantCultureIgnoreCase);

        private static readonly StringDictionaryConverter Ordinal = new(StringComparison.Ordinal);

        private static readonly StringDictionaryConverter OrdinalIgnoreCase = new(StringComparison.OrdinalIgnoreCase);

        private readonly StringComparer _comparer;

        private StringDictionaryConverter(StringComparison stringComparison)
        {
            _comparer = GetStringComparer(stringComparison);
        }

        public static StringDictionaryConverter For(StringComparison stringComparison)
        {
            switch (stringComparison)
            {
                case StringComparison.CurrentCulture:
                    return CurrentCulture;
                case StringComparison.CurrentCultureIgnoreCase:
                    return CurrentCultureIgnoreCase;
                case StringComparison.InvariantCulture:
                    return InvariantCulture;
                case StringComparison.InvariantCultureIgnoreCase:
                    return InvariantCultureIgnoreCase;
                case StringComparison.Ordinal:
                    return Ordinal;
                case StringComparison.OrdinalIgnoreCase:
                    return OrdinalIgnoreCase;
                default:
                    throw new ArgumentOutOfRangeException(nameof(stringComparison));
            }
        }

        public override bool CanConvert(Type typeToConvert)
        {
            if (typeToConvert.IsGenericType == false)
                return false;

            Type genericDef = typeToConvert.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>))
                return false;

            return typeToConvert.GetGenericArguments()[0] == typeof(string);
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            Type valueType = typeToConvert.GetGenericArguments()[1];
            Type converterType = typeof(StringDictionaryConverterInner<>).MakeGenericType(valueType);
            return (JsonConverter)Activator.CreateInstance(converterType, _comparer);
        }

        private static StringComparer GetStringComparer(StringComparison stringComparison)
        {
            switch (stringComparison)
            {
                case StringComparison.CurrentCulture:
                    return StringComparer.CurrentCulture;
                case StringComparison.CurrentCultureIgnoreCase:
                    return StringComparer.CurrentCultureIgnoreCase;
                case StringComparison.InvariantCulture:
                    return StringComparer.InvariantCulture;
                case StringComparison.InvariantCultureIgnoreCase:
                    return StringComparer.InvariantCultureIgnoreCase;
                case StringComparison.Ordinal:
                    return StringComparer.Ordinal;
                case StringComparison.OrdinalIgnoreCase:
                    return StringComparer.OrdinalIgnoreCase;
                default:
                    throw new ArgumentOutOfRangeException(nameof(stringComparison));
            }
        }

        private sealed class StringDictionaryConverterInner<TValue> : JsonConverter<Dictionary<string, TValue>>
        {
            private readonly StringComparer _comparer;

            public StringDictionaryConverterInner(StringComparer comparer)
            {
                _comparer = comparer;
            }

            public override bool CanConvert(Type typeToConvert)
            {
                return typeof(Dictionary<string, TValue>).IsAssignableFrom(typeToConvert)
                    || typeToConvert == typeof(IDictionary<string, TValue>);
            }

            public override Dictionary<string, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.Null)
                    return null;

                if (reader.TokenType != JsonTokenType.StartObject)
                    throw new JsonException($"Expected StartObject, got {reader.TokenType}");

                Dictionary<string, TValue> result = new(_comparer);

                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                        return result;

                    if (reader.TokenType != JsonTokenType.PropertyName)
                        throw new JsonException($"Expected PropertyName, got {reader.TokenType}");

                    string propertyName = reader.GetString();
                    reader.Read();
                    TValue propertyValue = JsonSerializer.Deserialize<TValue>(ref reader, options);
                    result.Add(propertyName, propertyValue);
                }

                throw new JsonException("Unexpected end of JSON.");
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<string, TValue> value, JsonSerializerOptions options)
            {
                throw new NotSupportedException($"{nameof(StringDictionaryConverter)} does not support writing.");
            }
        }
    }
}
