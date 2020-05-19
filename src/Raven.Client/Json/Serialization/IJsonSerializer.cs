using System;

namespace Raven.Client.Json.Serialization
{
    public interface IJsonSerializer
    {
        void Serialize(IJsonWriter writer, object value);

        object Deserialize(IJsonReader reader, Type type);

        T Deserialize<T>(IJsonReader reader);
    }
}
