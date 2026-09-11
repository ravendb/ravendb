using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
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

        private void CaptureMissingProperties(object entity, Type entityType, BlittableJsonReaderObject json)
        {
            var knownProperties = GetKnownPropertyNames(entityType);
            var propertyNames = json.GetPropertyNames();

            for (int i = 0; i < propertyNames.Length; i++)
            {
                var propName = propertyNames[i];

                if (propName == Constants.Documents.Metadata.Key)
                    continue;

                if (knownProperties.Contains(propName))
                    continue;

                var propIndex = json.GetPropertyIndex(propName);
                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                json.GetPropertyByIndex(propIndex, ref propDetails);

                RegisterMissingProperties(entity, propName, propDetails.Value);
            }
        }

        // Cache known JSON property names per entity type.
        // Includes both CLR names and attribute-specified JSON names ([JsonPropertyName], [JsonProperty], [DataMember])
        // so that renamed properties are not misclassified as "missing".
        private static readonly ConcurrentDictionary<Type, HashSet<string>> _knownPropertyNamesCache = new();

        private static HashSet<string> GetKnownPropertyNames(Type type)
        {
            return _knownPropertyNamesCache.GetOrAdd(type, static t =>
            {
                var names = new HashSet<string>(StringComparer.Ordinal);
                for (Type current = t; current != null && current != typeof(object); current = current.BaseType)
                {
                    foreach (var prop in current.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                    {
                        names.Add(prop.Name);
                        var jsonName = JsonPropertyNameResolver.GetJsonPropertyName(prop);
                        if (jsonName != null)
                            names.Add(jsonName);
                    }
                    foreach (var field in current.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                    {
                        names.Add(field.Name);
                        var jsonName = JsonPropertyNameResolver.GetJsonPropertyName(field);
                        if (jsonName != null)
                            names.Add(jsonName);
                    }
                }
                return names;
            });
        }
    }
}
