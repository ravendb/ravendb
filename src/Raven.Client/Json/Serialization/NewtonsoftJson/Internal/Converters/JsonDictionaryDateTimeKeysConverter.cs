using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class JsonDictionaryDateTimeKeysConverter : JsonConverter
    {
        private readonly MethodInfo _genericWriteJsonMethodInfo = typeof(JsonDictionaryDateTimeKeysConverter).GetMethod(nameof(GenericWriteJson));
        private readonly MethodInfo _genericReadJsonMethodInfo = typeof(JsonDictionaryDateTimeKeysConverter).GetMethod(nameof(GenericReadJson));

        public static readonly JsonDictionaryDateTimeKeysConverter Instance = new JsonDictionaryDateTimeKeysConverter();

        private JsonDictionaryDateTimeKeysConverter()
        {
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var genericArguments = value.GetType().GetGenericArguments();
            var makeGenericMethod = _genericWriteJsonMethodInfo.MakeGenericMethod(genericArguments);
            makeGenericMethod.Invoke(this, new[] { writer, value, serializer });
        }

        public void GenericWriteJson<TKey, TValue>(JsonWriter writer, Dictionary<TKey, TValue> value, JsonSerializer serializer)
        {
            writer.WriteStartObject();

            foreach (var kvp in value)
            {
                object key = kvp.Key;
                if (key is DateTime)
                {
                    var dateTime = (DateTime)key;
                    if (dateTime.Kind == DateTimeKind.Unspecified)
                        dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Local);
                    writer.WritePropertyName(dateTime.GetDefaultRavenFormat());
                }
                else if (key is DateTimeOffset)
                {
                    var dateTimeOffset = (DateTimeOffset)key;
                    writer.WritePropertyName(dateTimeOffset.Offset == TimeSpan.Zero
                        ? dateTimeOffset.UtcDateTime.GetDefaultRavenFormat(true)
                        : dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture));
                }
                else
                    throw new ArgumentException($"Not idea how to process argument: '{value}'");

                serializer.Serialize(writer, kvp.Value);
            }

            writer.WriteEndObject();
        }

        public Dictionary<TKey, TValue> GenericReadJson<TKey, TValue>(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var result = new Dictionary<TKey, TValue>();
            do
            {
                reader.Read();
                if (reader.TokenType == JsonToken.EndObject)
                    return result;
                if (reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException("Expected PropertyName, Got " + reader.TokenType);

                object key;
                if (reader.Value is string s)
                {
                    if (typeof(TKey) == typeof(DateTime) || typeof(TKey) == typeof(DateTime?))
                    {
                        if (DateTime.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                                                   DateTimeStyles.RoundtripKind, out DateTime time))
                        {
                            key = time.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(time, DateTimeKind.Local) : time;
                        }
                        else
                        {
                            throw new InvalidOperationException("Could not parse date time from " + s);
                        }
                    }
                    else if (typeof(TKey) == typeof(DateTimeOffset) || typeof(TKey) == typeof(DateTimeOffset?))
                    {
                        if (DateTimeOffset.TryParseExact(s, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                                                         DateTimeStyles.RoundtripKind, out DateTimeOffset time))
                        {
                            key = time;
                        }
                        else
                        {
                            throw new InvalidOperationException("Could not parse date time offset from " + s);
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("No idea how to parse " + typeof(TKey));
                    }
                }
                else
                {
                    key = reader.Value;
                }
                reader.Read();// read the value
                result[(TKey)key] = (TValue)serializer.Deserialize(reader, typeof(TValue));
            } while (true);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var genericArguments = objectType.GetGenericArguments();
            var makeGenericMethod = _genericReadJsonMethodInfo.MakeGenericMethod(genericArguments);
            return makeGenericMethod.Invoke(this, new[] { reader, objectType, existingValue, serializer });
        }

        public override bool CanConvert(Type objectType)
        {
            if (objectType.IsGenericType == false)
                return false;
            if (objectType.GetGenericTypeDefinition() != typeof(Dictionary<,>) && objectType.GetGenericTypeDefinition() != typeof(IDictionary<,>))
                return false;

            var keyType = objectType.GetGenericArguments()[0];
            return typeof(DateTime) == keyType ||
                typeof(DateTimeOffset) == keyType ||
                typeof(DateTimeOffset?) == keyType ||
                typeof(DateTime?) == keyType;
        }
    }
}
