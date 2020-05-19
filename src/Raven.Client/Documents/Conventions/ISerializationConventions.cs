using System;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Conventions
{
    public interface ISerializationConventions
    {
        void Freeze(DocumentConventions conventions);

        IJsonSerializer CreateSerializer();

        IJsonSerializer CreateDeserializer();

        IBlittableJsonConverter DefaultConverter { get; }

        ISessionBlittableJsonConverter CreateConverter(InMemoryDocumentSessionOperations session);

        IJsonWriter CreateWriter(JsonOperationContext context);

        object DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json);

        T DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json);
    }
}
