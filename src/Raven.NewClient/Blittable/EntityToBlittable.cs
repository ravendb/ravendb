using System;
using System.Collections.Generic;
using System.IO;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Util;
using Sparrow.Json;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Json;

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
            return reader;
        }

        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentConvention documentConvention, JsonOperationContext jsonOperationContext, DocumentInfo documentInfo = null)
        {
            var writer = new BlittableJsonWriter(jsonOperationContext, documentInfo);
            var serializer = documentConvention.CreateSerializer();

            serializer.Serialize(writer, entity);
            writer.FinalizeDocument();
            var reader = writer.CreateReader();
            return reader;
        }

        /// <summary>
        /// Converts the json document to an entity.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="document">The document found.</param>
        /// <returns></returns>
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
       
    }
}
