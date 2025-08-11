using System;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class SubscriptionBlittableJsonConverter : BlittableJsonConverterBase, ISubscriptionsBlittableJsonConverter
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
                ExtensionDataSetter dataSetter = null;
                if (Conventions.Conventions.PreserveDocumentPropertiesNotFoundOnModel)
                {
                    dataSetter = RegisterMissingProperties;
                }

                using (DefaultRavenContractResolver.RegisterExtensionDataSetter(dataSetter))
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
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {type}",
                    ex);
            }
        }
    }
}
