using System;
using Newtonsoft.Json;

namespace Raven.Client.Json.Serialization.JsonNet.Internal
{
    internal class JsonNetJsonSerializer : JsonSerializer, IJsonSerializer
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
}
