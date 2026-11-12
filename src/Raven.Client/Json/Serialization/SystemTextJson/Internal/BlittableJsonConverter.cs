using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class BlittableJsonConverter : BlittableJsonConverterBase, IBlittableJsonConverter
    {
        public BlittableJsonConverter(ISerializationConventions conventions)
            : base(conventions)
        {
        }

        public T FromBlittable<T>(BlittableJsonReaderObject json, string id)
        {
            return (T)FromBlittable(typeof(T), json, id);
        }

        public object FromBlittable(Type type, BlittableJsonReaderObject json, string id)
        {
            try
            {
                return DeserializeEntity(type, json, id, out _);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {type}",
                    ex);
            }
        }

        public BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            var jsonSerializer = ((ISerializationConventions)Conventions).CreateSerializer();
            return ToBlittable(entity, context, jsonSerializer);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            var serializer = (SystemTextJsonJsonSerializer)jsonSerializer;
            Type type = entity.GetType();
            return ConvertToBlittable(entity, Conventions.Conventions, context, removeIdentityProperty: false,
                () => serializer.SerializeToBlittable(entity, type, context));
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            var jsonSerializer = ((ISerializationConventions)Conventions).CreateSerializer();
            return ToBlittable(entity, metadata, context, jsonSerializer);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            var serializer = (SystemTextJsonJsonSerializer)jsonSerializer;
            Type type = entity.GetType();
            return ConvertToBlittable(entity, Conventions.Conventions, context, removeIdentityProperty: true,
                () => serializer.SerializeToBlittableWithMetadata(entity, type, metadata, context));
        }
    }
}
