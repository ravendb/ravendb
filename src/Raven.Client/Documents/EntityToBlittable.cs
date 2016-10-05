using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Client.Linq;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DocumentInfo = Raven.Client.Documents.InMemoryDocumentSessionOperations.DocumentInfo;
using System.Text;

namespace Raven.Client.Documents
{
    public class EntityToBlittable
    {
        private readonly InMemoryDocumentSessionOperations _session;

        private MemoryStream _stream;

        public StreamReader _streamReader;

        public StreamWriter _streamWriter;

        /// <summary>
        /// All the listeners for this session
        /// </summary>
        public EntityToBlittable(InMemoryDocumentSessionOperations session)
        {
            _session = session;
            _stream = new MemoryStream();
            _streamReader = new StreamReader(_stream, Encoding.UTF8, true, 1024, true);
            _streamWriter = new StreamWriter(_stream, StreamWriter.Null.Encoding, 1024, true);

        }
        public readonly Dictionary<object, Dictionary<string, JToken>> MissingDictionary = new Dictionary<object, Dictionary<string, JToken>>(ObjectReferenceEqualityComparer<object>.Default);
        
        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentInfo documentInfo)
        {
            _stream.Position = 0;
            try
            {
                _session.Conventions.SerializeEntityToJsonStream(entity, _streamWriter);
                _stream.Position = 0;
            
                var json = _session.Context.ReadForMemory(_stream, "convention.Serialize");

                if (json.Modifications == null)
                {
                    json.Modifications = new DynamicJsonValue(documentInfo.Metadata)
                    {
                        [Constants.Metadata.Key] = documentInfo.Metadata
                    };
                }

                return _session.Context.ReadObject(json, documentInfo.Id);
            }
            finally
            {
                _stream.Position = 0;
            }
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
            try
            {
                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                _stream.Position = 0;

                var documentType = _session.Conventions.GetClrType(id, document);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null)
                    {
                        document.WriteJsonTo(_stream);
                        _stream.Position = 0;
                        entity = _session.Conventions.DeserializeEntityFromJsonStream(type, _streamReader);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    document.WriteJsonTo(_stream);
                    _stream.Position = 0;
                    entity = _session.Conventions.DeserializeEntityFromJsonStream(entityType, _streamReader);
                }
                _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {entityType}",
                    ex);
            }
            finally
            {
                _streamReader.DiscardBufferedData();
                _stream.Position = 0;
            }
        }
       
    }
}
