using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class BlittableJsonConverter : BlittableJsonConverterBase, IBlittableJsonConverter
    {
        private IJsonSerializer _serializer;

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

            return ToBlittable(entity, context, _serializer ??= ((ISerializationConventions)Conventions).CreateSerializer());
        }

        public BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            return ConvertToBlittable(entity, Conventions.Conventions, context, removeIdentityProperty: false,
                (SystemTextJsonJsonSerializer)jsonSerializer, metadata: null, documentInfo: null);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            return ToBlittable(entity, metadata, context, _serializer ??= ((ISerializationConventions)Conventions).CreateSerializer());
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            if (entity is BlittableJsonReaderObject bjro)
                return bjro;

            return ConvertToBlittable(entity, Conventions.Conventions, context, removeIdentityProperty: true,
                (SystemTextJsonJsonSerializer)jsonSerializer, metadata, documentInfo: null);
        }
    }
}
