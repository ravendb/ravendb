using System;
using Newtonsoft.Json;
using Sparrow.Utils;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters
{
    internal sealed class CachingJsonConverter : JsonConverter
    {
        private readonly JsonConverter[] _converters;
        private TypeCache<JsonConverter> _cache = new(256);

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
            if (_cache.TryGet(objectType, out var converter))
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
            if (_cache.TryGet(objectType, out var converter))
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
            if (_cache.TryGet(objectType, out var value))
                return value != null;
            return UpdateCache(objectType) != null;
        }

        private JsonConverter UpdateCache(Type objectType)
        {
            foreach (var converter in _converters)
            {
                if (!converter.CanConvert(objectType))
                    continue;
                _cache.Put(objectType, converter);
                return converter;
            }
            _cache.Put(objectType, null);
            return null;
        }
        
        private void ThrowConverterIsNullException(Type objectType)
        {
            throw new InvalidOperationException($"Could not find the right converter from cache for '{objectType.FullName}'.");
        }
    }
}
