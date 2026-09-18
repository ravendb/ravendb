using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SessionBlittableJsonConverter : BlittableJsonConverterWithMissingProperties, ISessionBlittableJsonConverter
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public SessionBlittableJsonConverter(InMemoryDocumentSessionOperations session)
            : base(session.Conventions.Serialization)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
        }

        public object FromBlittable(Type type, ref BlittableJsonReaderObject json, string id, bool trackEntity)
        {
            try
            {
                if (type == typeof(BlittableJsonReaderObject))
                {
                    return json;
                }

                _session.OnBeforeConversionToEntityInvoke(id, type, ref json);

                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(type);
                var entity = defaultValue;

                Type entityType = type;
                var documentTypeAsString = _session.Conventions.GetClrType(id, json);
                if (documentTypeAsString != null)
                {
                    var documentType = _session.Conventions.ResolveTypeFromClrTypeName(documentTypeAsString);
                    if (documentType != null && type.IsAssignableFrom(documentType))
                    {
                        entityType = documentType;
                        entity = _session.Conventions.Serialization.DeserializeEntityFromBlittable(documentType, json);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = _session.Conventions.Serialization.DeserializeEntityFromBlittable(type, json);
                }

                if (trackEntity && _session.Conventions.PreserveDocumentPropertiesNotFoundOnModel && entity != null)
                {
                    CaptureMissingProperties(entity, entityType, json);
                }

                if (id != null)
                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                return entity;
            }
            catch (Exception ex)
            {
                string jsonAsString = TryReadBlittableAsString(json);

                throw new InvalidOperationException($"Could not convert document {id} to entity of type {type}. Json: {jsonAsString}",
                    ex);
            }

            string TryReadBlittableAsString(BlittableJsonReaderObject jsonToRead)
            {
                var jsString = string.Empty;

                using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
                {
                    try
                    {
                        jsonToRead.WriteJsonToAsync(memoryStream).AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
                    }
                    catch
                    {
                        // ignore
                    }

                    memoryStream.Position = 0;

                    try
                    {
                        using (var sr = new StreamReader(memoryStream))
                        {
                            jsString = sr.ReadToEnd();
                        }
                    }
                    catch
                    {
                        // ignore
                    }
                }

                return jsString;
            }
        }

        public T FromBlittable<T>(ref BlittableJsonReaderObject json, string id, bool trackEntity)
        {
            return (T)FromBlittable(typeof(T), ref json, id, trackEntity);
        }

        public void PopulateEntity(object entity, string id, BlittableJsonReaderObject json)
        {
            var jsonSerializer = _session.Conventions.Serialization.CreateSerializer();
            PopulateEntity(entity, id, json, jsonSerializer);
        }

        public void PopulateEntity(object entity, string id, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            PopulateEntity(entity, json, jsonSerializer);

            _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, DocumentInfo documentInfo)
        {
            if (entity is BlittableJsonReaderObject blittable)
                return blittable;

            if (documentInfo != null)
                _session.OnBeforeConversionToDocumentInvoke(documentInfo.Id, entity);

            BlittableJsonReaderObject document;
            using (var writer = new SystemTextJsonBlittableWriter(_session.Context, documentInfo))
                document = ToBlittableInternal(entity, _session.Conventions, _session.Context, _session.JsonSerializer, writer);

            // Inject missing properties (properties in the original JSON that aren't on the CLR type)
            if (MissingProperties != null && MissingProperties.TryGetValue(entity, out var missingProps) && missingProps.Count > 0)
            {
                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                foreach (var kvp in missingProps)
                    document.Modifications[(string)kvp.Key] = kvp.Value;

                using (var old = document)
                    document = _session.Context.ReadObject(document, "restore/missingProperties");
            }

            if (documentInfo != null)
                _session.OnAfterConversionToDocumentInvoke(documentInfo.Id, entity, ref document);

            return document;
        }

        private BlittableJsonReaderObject SerializeViaParseBuffer(object entity, SystemTextJsonJsonSerializer stjSerializer, DocumentInfo documentInfo)
        {
            var context = _session.Context;
            var conventions = _session.Conventions;
            var type = entity.GetType();

            // Set up identity removal via the resolver
            bool isDynamicObject = entity is IDynamicMetaObjectProvider;
            bool hasIdentityProperty = conventions.GetIdentityProperty(type) != null;

            if (isDynamicObject == false)
            {
                RavenJsonTypeInfoResolver.RootEntity = hasIdentityProperty ? entity : null;
                RavenJsonTypeInfoResolver.RemovedIdentityProperty = false;
            }

            BlittableJsonReaderObject document;
            try
            {
                document = stjSerializer.SerializeToBlittable(entity, type, context);
            }
            finally
            {
                RavenJsonTypeInfoResolver.RootEntity = null;
            }

            // Remove identity property if the resolver didn't handle it
            bool changes = false;
            if (isDynamicObject || (hasIdentityProperty && RavenJsonTypeInfoResolver.RemovedIdentityProperty == false))
            {
                changes = BlittableJsonConverterHelper.TryRemoveIdentityProperty(document, type, conventions, isDynamicObject);
            }

            // Inject @metadata
            changes |= InjectMetadata(document, documentInfo);

            if (changes)
            {
                using (var old = document)
                {
                    document = context.ReadObject(document, "convert/entityToBlittable");
                }
            }

            return document;
        }

        private static bool InjectMetadata(BlittableJsonReaderObject document, DocumentInfo documentInfo)
        {
            if (documentInfo == null)
                return false;

            object metadataValue = null;

            if (documentInfo.Metadata?.Modifications != null && documentInfo.Metadata.Modifications.Properties.Count > 0)
            {
                // Modified metadata blittable — apply modifications, then use the blittable itself
                // (ReadObject will handle the Modifications overlay)
                metadataValue = documentInfo.Metadata;
            }
            else if (documentInfo.Metadata != null)
            {
                // Existing metadata blittable — use directly as nested value
                metadataValue = documentInfo.Metadata;
            }
            else if (documentInfo.MetadataInstance != null)
            {
                var metadata = new DynamicJsonValue();
                foreach (var kvp in documentInfo.MetadataInstance)
                    metadata[kvp.Key] = kvp.Value;
                metadataValue = metadata;
            }
            else if (documentInfo.Collection != null)
            {
                var metadata = new DynamicJsonValue();
                metadata[Constants.Documents.Metadata.Collection] = documentInfo.Collection;
                if (documentInfo.Id != null)
                    metadata[Constants.Documents.Metadata.Id] = documentInfo.Id;
                metadataValue = metadata;
            }

            if (metadataValue == null)
                return false;

            if (document.Modifications == null)
                document.Modifications = new DynamicJsonValue(document);

            document.Modifications[Constants.Documents.Metadata.Key] = metadataValue;
            return true;
        }

        public void Clear()
        {
            MissingProperties?.Clear();
        }

        public void RemoveFromMissing<T>(T entity)
        {
            MissingProperties?.Remove(entity);
        }

        /// <summary>
        /// Compare the blittable's property names against the entity type's known CLR properties.
        /// Any properties in the JSON that don't exist on the CLR type are captured as missing
        /// properties for later round-trip preservation.
        /// </summary>
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

                // This property exists in JSON but not on the CLR type — capture its value
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
