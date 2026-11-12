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
                object entity = DeserializeEntity(type, json, id, out Type entityType);

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
