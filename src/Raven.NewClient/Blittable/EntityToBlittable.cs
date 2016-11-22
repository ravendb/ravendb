using System;
using System.Collections.Generic;
using System.IO;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Util;
using Sparrow.Json;
using Raven.NewClient.Client.Document;

namespace Raven.NewClient.Client.Blittable
{
    public class EntityToBlittable
    {
        private readonly InMemoryDocumentSessionOperations _session;

        private MemoryStream _stream;

        public StreamWriter _streamWriter;

        /// <summary>
        /// All the listeners for this session
        /// </summary>
        public EntityToBlittable(InMemoryDocumentSessionOperations session)
        {
            _session = session;
            _stream = new MemoryStream();
            _streamWriter = new StreamWriter(_stream, StreamWriter.Null.Encoding, 1024, true);

        }
        public readonly Dictionary<object, Dictionary<string, object>> MissingDictionary = new Dictionary<object, Dictionary<string, object>>(ObjectReferenceEqualityComparer<object>.Default);
        
        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentInfo documentInfo)
        {
            _stream.Position = 0;
            _session.Conventions.SerializeEntityToJsonStream(entity, _streamWriter);
            InsertMetadataToStream(documentInfo);
            return _session.Context.ReadForMemory(_stream, "convention.Serialize");
        }

        private void InsertMetadataToStream(DocumentInfo documentInfo)
        {
            _stream.Position--;
            var writer = new BlittableJsonTextWriter(_session.Context, _stream);
            writer.WriteComma();
            writer.WritePropertyName(Constants.Metadata.Key);
            if ((documentInfo.Metadata.Modifications != null) &&
                (documentInfo.Metadata.Modifications.Properties.Count > 0))
            {
                writer.WriteObject(_session.Context.ReadObject(documentInfo.Metadata.Modifications, documentInfo.Id));
                documentInfo.Metadata.Modifications = null;
            }
            else
            {
                writer.WriteObject(documentInfo.Metadata);
            }
            writer.WriteEndObject();
            writer.Flush();
            _stream.Position = 0;
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
