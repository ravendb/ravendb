using System;

namespace Raven.Client.Json.Serialization
{
    public interface IJsonSerializer
    {
        void Serialize(IJsonWriter writer, object value);

        void Serialize(IJsonWriter writer, object value, Type objectType);

        object Deserialize(IJsonReader reader, Type type);

        T Deserialize<T>(IJsonReader reader);
    }
}
