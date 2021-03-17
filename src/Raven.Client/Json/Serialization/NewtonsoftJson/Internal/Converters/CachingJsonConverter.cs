using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal class CachingJsonConverter : JsonConverter
    {
        private readonly JsonConverter[] _converters;
        private Dictionary<Type, JsonConverter> _cache = new Dictionary<Type, JsonConverter>();

        public override bool CanRead { get; }
        public override bool CanWrite { get; }

        public CachingJsonConverter(JsonConverter[] converters, bool canRead, bool canWrite)
        {
            CanRead = canRead;
            CanWrite = canWrite;
            _converters = converters;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            Type objectType = value.GetType();
            if (_cache.TryGetValue(objectType, out var converter))
            {
                if (converter == null)
                    ThrowConverterIsNullException(objectType);

                converter.WriteJson(writer, value, serializer);
                return;
            }

            // can happen if the cache was replaced by another copy without the right converter
            // this can happen if we lost the race, UpdateCache return the right converter, so no issue here
            converter = UpdateCache(objectType);
            if (converter == null)
                ThrowConverterIsNullException(objectType);
            converter.WriteJson(writer, value, serializer);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (_cache.TryGetValue(objectType, out var converter))
            {
                if (converter == null)
                    ThrowConverterIsNullException(objectType);

                return converter.ReadJson(reader, objectType, existingValue, serializer);
            }

            // can happen if the cache was replaced by another copy without the right converter
            // this can happen if we lost the race, UpdateCache return the right converter, so no issue here
            converter = UpdateCache(objectType);
            if (converter == null)
                ThrowConverterIsNullException(objectType);

            return converter.ReadJson(reader, objectType, existingValue, serializer);
        }

        public override bool CanConvert(Type objectType)
        {
            if (_cache.TryGetValue(objectType, out var value))
                return value != null;
            return UpdateCache(objectType) != null;
        }

        private JsonConverter UpdateCache(Type objectType)
        {
            foreach (var converter in _converters)
            {
                if (!converter.CanConvert(objectType))
                    continue;
                _cache = new Dictionary<Type, JsonConverter>(_cache) { [objectType] = converter };
                return converter;
            }
            _cache = new Dictionary<Type, JsonConverter>(_cache) { [objectType] = null };
            return null;
        }
        
        private void ThrowConverterIsNullException(Type objectType)
        {
            throw new InvalidOperationException($"Could not find the right converter from cache for '{objectType.FullName}'.");
        }
    }
}
