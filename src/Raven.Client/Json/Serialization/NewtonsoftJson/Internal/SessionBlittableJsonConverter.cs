using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class SessionBlittableJsonConverter : BlittableJsonConverterBase, ISessionBlittableJsonConverter
    {
        private readonly InMemoryDocumentSessionOperations _session;

        private readonly Dictionary<object, Dictionary<object, object>> _missingDictionary = new Dictionary<object, Dictionary<object, object>>(ObjectReferenceEqualityComparer<object>.Default);

        public SessionBlittableJsonConverter(InMemoryDocumentSessionOperations session)
            : base(session.Conventions.Serialization)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
        }

        public void Clear()
        {
            _missingDictionary.Clear();
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

                ExtensionDataSetter dataSetter = null;
                if (trackEntity)
                {
                    dataSetter = RegisterMissingProperties;
                }

                using (DefaultRavenContractResolver.RegisterExtensionDataSetter(dataSetter))
                {
                    var documentTypeAsString = _session.Conventions.GetClrType(id, json);
                    if (documentTypeAsString != null)
                    {
                        var documentType = Type.GetType(documentTypeAsString);
                        if (documentType != null && type.IsAssignableFrom(documentType))
                        {
                            entity = _session.Conventions.Serialization.DeserializeEntityFromBlittable(documentType, json);
                        }
                    }

                    if (Equals(entity, defaultValue))
                    {
                        entity = _session.Conventions.Serialization.DeserializeEntityFromBlittable(type, json);
                    }
                }

                if (id != null)
                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                _session.OnAfterConversionToEntityInvoke(id, json, entity);

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

                using (var memoryStream = new MemoryStream())
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

        public void RemoveFromMissing<T>(T entity)
        {
            _missingDictionary.Remove(entity);
        }

        public BlittableJsonReaderObject ToBlittable(object entity, DocumentInfo documentInfo)
        {
            //maybe we don't need to do anything..
            if (entity is BlittableJsonReaderObject blittable)
                return blittable;

            if (documentInfo != null)
                _session.OnBeforeConversionToDocumentInvoke(documentInfo.Id, entity);

            using (DefaultRavenContractResolver.RegisterExtensionDataGetter(FillMissingProperties))
            using (var writer = new BlittableJsonWriter(_session.Context, documentInfo))
            {
                var document = ToBlittableInternal(entity, _session.Conventions, _session.Context, _session.JsonSerializer, writer);

                if (documentInfo != null)
                    _session.OnAfterConversionToDocumentInvoke(documentInfo.Id, entity, ref document);

                return document;
            }
        }

        private IEnumerable<KeyValuePair<object, object>> FillMissingProperties(object o)
        {
            _missingDictionary.TryGetValue(o, out var props);
            return props;
        }

        private void RegisterMissingProperties(object o, string id, object value)
        {
            if (_session.Conventions.PreserveDocumentPropertiesNotFoundOnModel == false ||
                id == Constants.Documents.Metadata.Key)
                return;

            if (_missingDictionary.TryGetValue(o, out var dictionary) == false)
            {
                _missingDictionary[o] = dictionary = new Dictionary<object, object>();
            }

            dictionary[id] = value;
        }
    }
}
