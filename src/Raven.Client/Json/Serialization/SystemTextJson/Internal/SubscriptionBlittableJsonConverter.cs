using System;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SubscriptionBlittableJsonConverter : BlittableJsonConverterWithMissingProperties, ISubscriptionsBlittableJsonConverter
    {
        public SubscriptionBlittableJsonConverter(ISerializationConventions conventions) : base(conventions)
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

                Type entityType = type;
                var documentTypeAsString = Conventions.Conventions.GetClrType(id, json);
                if (documentTypeAsString != null)
                {
                    var documentType = Conventions.Conventions.ResolveTypeFromClrTypeName(documentTypeAsString);
                    if (documentType != null && type.IsAssignableFrom(documentType))
                    {
                        entityType = documentType;
                        entity = Conventions.DeserializeEntityFromBlittable(documentType, json);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = Conventions.DeserializeEntityFromBlittable(type, json);
                }

                if (Conventions.Conventions.PreserveDocumentPropertiesNotFoundOnModel && entity != null)
                {
                    CaptureMissingProperties(entity, entityType, json);
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {type}",
                    ex);
            }
        }
    }
}
