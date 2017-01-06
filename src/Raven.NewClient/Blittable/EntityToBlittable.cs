using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Util;
using Sparrow.Json;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Json;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Blittable
{
    public class EntityToBlittable
    {
        private readonly InMemoryDocumentSessionOperations _session;

        /// <summary>
        /// All the listeners for this session
        /// </summary>
        public EntityToBlittable(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public readonly Dictionary<object, Dictionary<string, object>> MissingDictionary = new Dictionary<object, Dictionary<string, object>>(ObjectReferenceEqualityComparer<object>.Default);
        
        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentInfo documentInfo)
        {
            var writer = new BlittableJsonWriter(_session.Context, documentInfo);
            var serializer = _session.Conventions.CreateSerializer();

            serializer.Serialize(writer, entity);
            writer.FinalizeDocument();
            var reader = writer.CreateReader();

            RemoveIdentityProperty(reader, entity.GetType(), _session.Conventions);

            return reader;
        }

        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentConvention documentConvention, JsonOperationContext jsonOperationContext, DocumentInfo documentInfo = null)
        {
            var writer = new BlittableJsonWriter(jsonOperationContext, documentInfo);
            var serializer = documentConvention.CreateSerializer();

            serializer.Serialize(writer, entity);
            writer.FinalizeDocument();
            var reader = writer.CreateReader();

            RemoveIdentityProperty(reader, entity.GetType(), documentConvention);

            return reader;
        }

        private void RemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConvention conventions)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty != null)
            {
                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                document.Modifications.Remove(identityProperty.Name);
            }
        }

        /// <summary>
        /// Converts a BlittableJsonReaderObject to an entity.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="document">The document found.</param>
        /// <returns>The converted entity</returns>
        public object ConvertToEntity(Type entityType, string id, BlittableJsonReaderObject document)
        {
            //TODO -  Add RegisterMissingProperties ???
            try
            {
                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                var documentType = _session.Conventions.GetClrType(id, document);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null)
                    {
                        entity = _session.Conventions.DeserializeEntityFromBlittable(type, document);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = _session.Conventions.DeserializeEntityFromBlittable(entityType, document);
                }
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {entityType}",
                    ex);
            }
        }

        /// <summary>
        /// Converts a BlittableJsonReaderObject to an entity without a session.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="document">The document found.</param>
        /// <param name="documentConvention">The documentConvention.</param>
        /// <returns>The converted entity</returns>
        public static object ConvertToEntity(Type entityType, string id, BlittableJsonReaderObject document, DocumentConvention documentConvention)
        {
            try
            {
                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                var documentType = documentConvention.GetClrType(id, document);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null)
                    {
                        entity = documentConvention.DeserializeEntityFromBlittable(type, document);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = documentConvention.DeserializeEntityFromBlittable(entityType, document);
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {entityType}",
                    ex);
            }
        }
       
    }
}
