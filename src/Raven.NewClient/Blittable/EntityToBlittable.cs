using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Raven.NewClient.Abstractions.Data;
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
            using (var writer = new BlittableJsonWriter(_session.Context, documentInfo))
            {
                var serializer = _session.Conventions.CreateSerializer();

                serializer.Serialize(writer, entity);
                writer.FinalizeDocument();
                var reader = writer.CreateReader();
                var type = entity.GetType();

                RemoveIdentityProperty(reader, type, _session.Conventions);
                SimplifyJson(reader);

                return reader;
            }
        }

        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentConvention documentConvention, JsonOperationContext jsonOperationContext, DocumentInfo documentInfo = null)
        {
            using (var writer = new BlittableJsonWriter(jsonOperationContext, documentInfo))
            {
                var serializer = documentConvention.CreateSerializer();

                serializer.Serialize(writer, entity);
                writer.FinalizeDocument();
                var reader = writer.CreateReader();
                var type = entity.GetType();

                RemoveIdentityProperty(reader, type, documentConvention);
                SimplifyJson(reader);

                return reader;
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

                if (id != null)
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

        private static void RemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConvention conventions)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty != null)
            {
                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                document.Modifications.Remove(identityProperty.Name);
            }
        }

        private static void SimplifyJson(BlittableJsonReaderObject document)
        {
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyValue = document[propertyName];

                var propertyArray = propertyValue as BlittableJsonReaderArray;
                if (propertyArray != null)
                {
                    SimplifyJson(propertyArray);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                string type;
                if (propertyObject.TryGet(Constants.Json.Fields.Type, out type) == false)
                {
                    SimplifyJson(propertyObject);
                    continue;
                }

                if (ShouldSimplifyJsonBasedOnType(type) == false)
                    continue;

                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                BlittableJsonReaderArray values;
                if (propertyObject.TryGet(Constants.Json.Fields.Values, out values) == false)
                {
                    if (propertyObject.Modifications == null)
                        propertyObject.Modifications = new DynamicJsonValue(propertyObject);

                    propertyObject.Modifications.Remove(Constants.Json.Fields.Type);
                    continue;
                }

                document.Modifications[propertyName] = values;

                SimplifyJson(values);
            }
        }

        private static void SimplifyJson(BlittableJsonReaderArray array)
        {
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                SimplifyJson(itemObject);
            }
        }

        private static readonly Regex ArrayEndRegex = new Regex(@"\[\], [\w\.-]+$", RegexOptions.Compiled);

        private static bool ShouldSimplifyJsonBasedOnType(string typeValue)
        {
            if (typeValue == null)
                return false;
            if (typeValue.StartsWith("System.Collections.Generic.List`1[["))
                return true;
            if (typeValue.StartsWith("System.Collections.Generic.Dictionary`2[["))
                return true;
            if (ArrayEndRegex.IsMatch(typeValue)) // array
                return true;
            return false;
        }
    }
}
