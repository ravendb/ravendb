using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;

namespace Raven.Server.Json.Converters
{
    internal abstract class RavenTypeJsonConverter<T> : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!(writer is BlittableJsonWriter blittableJsonWriter))
            {
                throw new SerializationException(
                    $"Try to write {nameof(T)} property/field by {writer.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonWriter)}");
            }

            if (!(value is T tValue))
            {
                throw new SerializationException($"Try to write {value?.GetType()} with {GetType().Name}. The converter should be used only for {nameof(T)}");
            }

            WriteJson(blittableJsonWriter, tValue, serializer);
        }

        protected abstract void WriteJson(BlittableJsonWriter writer, T value, JsonSerializer serializer);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value == null)
            {
                return null;
            }

            if (!(reader is BlittableJsonReader blittableReader))
            {
                throw new SerializationException(
                    $"Try to read {nameof(T)} property/field by {reader.GetType()} which is unsuitable reader. Should use {nameof(BlittableJsonReader)}");
            }

            return ReadJson(blittableReader);
        }

        internal abstract T ReadJson(BlittableJsonReader blittableReader);

        public override bool CanConvert(Type objectType)
        {
            return typeof(T).IsAssignableFrom(objectType);
        }
    }
}
