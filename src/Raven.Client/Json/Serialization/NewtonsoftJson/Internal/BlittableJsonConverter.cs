using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class BlittableJsonConverter : BlittableJsonConverterBase, IBlittableJsonConverter
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
                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(type);
                var entity = defaultValue;

                var documentTypeAsString = Conventions.Conventions.GetClrType(id, json);
                if (documentTypeAsString != null)
                {
                    var documentType = Conventions.Conventions.ResolveTypeFromClrTypeName(documentTypeAsString);
                    if (documentType != null && type.IsAssignableFrom(documentType))
                    {
                        entity = Conventions.DeserializeEntityFromBlittable(documentType, json);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = Conventions.DeserializeEntityFromBlittable(type, json);
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {type}",
                    ex);
            }
        }

        public BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context)
        {
            var jsonSerializer = Conventions.CreateSerializer();
            return ToBlittable(entity, context, jsonSerializer);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            using (var writer = new BlittableJsonWriter(context, documentInfo: null))
                return ToBlittableInternal(entity, Conventions.Conventions, context, jsonSerializer, writer, removeIdentityProperty: false);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context)
        {
            var jsonSerializer = Conventions.CreateSerializer();
            return ToBlittable(entity, metadata, context, jsonSerializer);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, IMetadataDictionary metadata, JsonOperationContext context, IJsonSerializer jsonSerializer)
        {
            using (var writer = new BlittableJsonWriter(context, new DocumentInfo { MetadataInstance = metadata }))
                return ToBlittableInternal(entity, Conventions.Conventions, context, jsonSerializer, writer);
        }
    }
}
