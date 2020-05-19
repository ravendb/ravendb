using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization
{
    public interface IBlittableJsonConverter : IBlittableJsonConverterBase
    {
        BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context);

        BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context, IJsonSerializer jsonSerializer);

        BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context);

        BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context, IJsonSerializer jsonSerializer);

        object FromBlittable(Type type, BlittableJsonReaderObject json, string id = null);

        T FromBlittable<T>(BlittableJsonReaderObject json, string id = null);
    }

    public interface ISessionBlittableJsonConverter : IBlittableJsonConverterBase
    {
        BlittableJsonReaderObject ToBlittable(object entity, DocumentInfo documentInfo);

        object FromBlittable(Type type, ref BlittableJsonReaderObject json, string id, bool trackEntity);

        T FromBlittable<T>(ref BlittableJsonReaderObject json, string id, bool trackEntity);

        void PopulateEntity(object entity, string id, BlittableJsonReaderObject json);

        void PopulateEntity(object entity, string id, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer);

        void RemoveFromMissing<T>(T entity);

        void Clear();
    }

    public interface IBlittableJsonConverterBase
    {
        void PopulateEntity(object entity, BlittableJsonReaderObject json);

        void PopulateEntity(object entity, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer);
    }
}
