using System;
using System.Collections;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal class StringDictionaryConverter : JsonConverter
    {
        private static readonly StringDictionaryConverter CurrentCulture = new StringDictionaryConverter(StringComparison.CurrentCulture);

        private static readonly StringDictionaryConverter CurrentCultureIgnoreCase = new StringDictionaryConverter(StringComparison.CurrentCultureIgnoreCase);

        private static readonly StringDictionaryConverter InvariantCulture = new StringDictionaryConverter(StringComparison.InvariantCulture);

        private static readonly StringDictionaryConverter InvariantCultureIgnoreCase = new StringDictionaryConverter(StringComparison.InvariantCultureIgnoreCase);

        private static readonly StringDictionaryConverter Ordinal = new StringDictionaryConverter(StringComparison.Ordinal);

        private static readonly StringDictionaryConverter OrdinalIgnoreCase = new StringDictionaryConverter(StringComparison.OrdinalIgnoreCase);

        private readonly object[] _constructorParameters;

        public override bool CanWrite => false;

        private StringDictionaryConverter(StringComparison stringComparison)
        {
            _constructorParameters = new object[] { GetStringComparer(stringComparison) };
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

        public override bool CanConvert(Type objectType)
        {
            return true;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            if (reader.TokenType != JsonToken.StartObject)
                throw new InvalidOperationException();

            var contract = (JsonDictionaryContract)serializer.ContractResolver.ResolveContract(objectType);
            var result = (IDictionary)Activator.CreateInstance(contract.CreatedType, _constructorParameters);

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.EndObject)
                    break;

                if (reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException();

                var propertyName = (string)reader.Value;
                if (reader.Read() == false)
                    throw new InvalidOperationException();

                var propertyValue = serializer.Deserialize(reader, contract.DictionaryValueType);

                result.Add(propertyName, propertyValue);
            }

            return result;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotSupportedException();
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
    }
}
