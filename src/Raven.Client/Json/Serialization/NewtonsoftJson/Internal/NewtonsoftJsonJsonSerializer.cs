using System;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class NewtonsoftJsonJsonSerializer : JsonSerializer, IJsonSerializer
    {
        object IJsonSerializer.Deserialize(IJsonReader reader, Type type)
        {
            return Deserialize((BlittableJsonReader)reader, type);
        }

        T IJsonSerializer.Deserialize<T>(IJsonReader reader)
        {
            return Deserialize<T>((BlittableJsonReader)reader);
        }

        void IJsonSerializer.Serialize(IJsonWriter writer, object value)
        {
            Serialize((BlittableJsonWriter)writer, value);
        }
    }

    internal static class NewtonsoftJsonJsonSerializerExtensions
    {
        public static void ApplyOptions(this NewtonsoftJsonJsonSerializer jsonSerializer, CreateSerializerOptions options)
        {
            switch (options?.TypeNameHandling)
            {
                case TypeNameHandling.None:
                    jsonSerializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.None;
                    break;
                case TypeNameHandling.Objects:
                    jsonSerializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects;
                    break;
            }
        }
    }
}
