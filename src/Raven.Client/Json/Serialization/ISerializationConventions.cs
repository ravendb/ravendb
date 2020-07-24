using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization
{
    public interface ISerializationConventions
    {
        DocumentConventions Conventions { get; }

        void Initialize(DocumentConventions conventions);

        IJsonSerializer CreateSerializer(CreateSerializerOptions options = null);

        IJsonSerializer CreateDeserializer(CreateDeserializerOptions options = null);

        IBlittableJsonConverter DefaultConverter { get; }

        ISessionBlittableJsonConverter CreateConverter(InMemoryDocumentSessionOperations session);

        IJsonWriter CreateWriter(JsonOperationContext context);

        object DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json);

        T DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json);
    }
}
