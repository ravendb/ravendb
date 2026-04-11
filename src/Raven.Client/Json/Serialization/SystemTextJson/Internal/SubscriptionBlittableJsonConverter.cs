using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
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
                if (Conventions.Conventions.PreserveDocumentPropertiesNotFoundOnModel)
                    throw new NotSupportedException(
                        $"{nameof(DocumentConventions.PreserveDocumentPropertiesNotFoundOnModel)} is not yet supported with the System.Text.Json serializer. " +
                        "Disable this convention or use the Newtonsoft.Json serializer.");

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
    }
}
