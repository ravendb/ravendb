using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session
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
            //maybe we don't need to do anything..
            var blittable = entity as BlittableJsonReaderObject;
            if(blittable != null)
                return blittable;            

            using (var writer = new BlittableJsonWriter(_session.Context, documentInfo))
            {
                var serializer = _session.Conventions.CreateSerializer();

                serializer.Serialize(writer, entity);
                writer.FinalizeDocument();
                var reader = writer.CreateReader();
                var type = entity.GetType();

                var changes = TryRemoveIdentityProperty(reader, type, _session.Conventions);
                changes |= TrySimplifyJson(reader);

                if (changes)
                    reader = _session.Context.ReadObject(reader, "convert/entityToBlittable");

                return reader;
            }
        }

        public static BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentConventions conventions, JsonOperationContext context, DocumentInfo documentInfo = null)
        {
            using (var writer = new BlittableJsonWriter(context, documentInfo))
            {

                var serializer = conventions.CreateSerializer();

                serializer.Serialize(writer, entity);
                writer.FinalizeDocument();
                var reader = writer.CreateReader();
                var type = entity.GetType();

                var changes = TryRemoveIdentityProperty(reader, type, conventions);
                changes |= TrySimplifyJson(reader);

                if (changes)
                    reader = context.ReadObject(reader, "convert/entityToBlittable");

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
                    if (type != null && entityType.IsAssignableFrom(type))
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
        /// <param name="conventions">The conventions.</param>
        /// <returns>The converted entity</returns>
        public static object ConvertToEntity(Type entityType, string id, BlittableJsonReaderObject document, DocumentConventions conventions)
        {
            try
            {
                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                var documentType = conventions.GetClrType(id, document);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null && entityType.IsAssignableFrom(type))
                    {
                        entity = conventions.DeserializeEntityFromBlittable(type, document);
                    }
                }

                if (Equals(entity, defaultValue))
                {
                    entity = conventions.DeserializeEntityFromBlittable(entityType, document);
                }

                return entity;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not convert document {id} to entity of type {entityType}",
                    ex);
            }
        }

        private static bool TryRemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConventions conventions)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
                return false;

            if (document.Modifications == null)
                document.Modifications = new DynamicJsonValue(document);

            document.Modifications.Remove(identityProperty.Name);
            return true;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderObject document)
        {
            var simplified = false;
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyValue = document[propertyName];

                var propertyArray = propertyValue as BlittableJsonReaderArray;
                if (propertyArray != null)
                {
                    simplified |= TrySimplifyJson(propertyArray);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                string type;
                if (propertyObject.TryGet(Constants.Json.Fields.Type, out type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject);
                    continue;
                }

                if (ShouldSimplifyJsonBasedOnType(type) == false)
                    continue;

                simplified = true;

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

                simplified |= TrySimplifyJson(values);
            }

            return simplified;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderArray array)
        {
            var simplified = false;
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                simplified |= TrySimplifyJson(itemObject);
            }

            return simplified;
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
