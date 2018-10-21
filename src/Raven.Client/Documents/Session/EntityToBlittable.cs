using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
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

        private readonly Dictionary<object, Dictionary<object, object>> _missingDictionary =
            new Dictionary<object, Dictionary<object, object>>(ObjectReferenceEqualityComparer<object>.Default);

        public BlittableJsonReaderObject ConvertEntityToBlittable(object entity, DocumentInfo documentInfo)
        {
            //maybe we don't need to do anything..
            if (entity is BlittableJsonReaderObject blittable)
                return blittable;

            using (DefaultRavenContractResolver.RegisterExtensionDataGetter(FillMissingProperties))
            using (var writer = new BlittableJsonWriter(_session.Context, documentInfo))
            {
                return ConvertEntityToBlittableInternal(entity, _session.Conventions, _session.Context, _session.JsonSerializer, writer);
            }
        }

        private IEnumerable<KeyValuePair<object, object>> FillMissingProperties(object o)
        {
            _missingDictionary.TryGetValue(o, out var props);
            return props;
        }


        internal static object ConvertToBlittableIfNeeded(
            object value,
            DocumentConventions conventions,
            JsonOperationContext context,
            JsonSerializer serializer,
            DocumentInfo documentInfo,
            bool isCompareExchange = false)
        {
            if (value is ValueType || 
                value is string || 
                value is BlittableJsonReaderArray || 
                value is BlittableJsonReaderArray)
                return value;

            if (value is IEnumerable && !(value is IDictionary))
            {
                return ((IEnumerable)value).Cast<object>()
                    .Select(v=> ConvertToBlittableIfNeeded(v, conventions, context, serializer, documentInfo, isCompareExchange));
            }
            
            using (var writer = new BlittableJsonWriter(context, documentInfo))
                return ConvertEntityToBlittableInternal(value, conventions, context, serializer, writer, isCompareExchange);
        }

        internal static BlittableJsonReaderObject ConvertEntityToBlittable(
            object entity,
            DocumentConventions conventions,
            JsonOperationContext context,
            JsonSerializer serializer,
            DocumentInfo documentInfo)
        {
            using (var writer = new BlittableJsonWriter(context, documentInfo))
                return ConvertEntityToBlittableInternal(entity, conventions, context, serializer, writer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static BlittableJsonReaderObject ConvertCommandToBlittable(
            object entity,
            JsonOperationContext context
            )
        {
            using (var writer = new BlittableJsonWriter(context))
            {
                DocumentConventions.Default.CreateSerializer().Serialize(writer, entity);
                writer.FinalizeDocument();
                return writer.CreateReader();
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static BlittableJsonReaderObject ConvertEntityToBlittableInternal(
            object entity,
            DocumentConventions conventions,
            JsonOperationContext context,
            JsonSerializer serializer,
            BlittableJsonWriter writer,
            bool isCompareExchange = false)
        {
            serializer.Serialize(writer, entity);
            writer.FinalizeDocument();
            var reader = writer.CreateReader();
            var type = entity.GetType();

            var changes = isCompareExchange == false && TryRemoveIdentityProperty(reader, type, conventions);
            changes |= TrySimplifyJson(reader);

            if (changes)
            {
                using (var old = reader)
                {
                    reader = context.ReadObject(reader, "convert/entityToBlittable");
                }
            }

            return reader;
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

        /// <summary>
        /// Converts a BlittableJsonReaderObject to an entity.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="document">The document found.</param>
        /// <returns>The converted entity</returns>
        public object ConvertToEntity(Type entityType, string id, BlittableJsonReaderObject document)
        {
            try
            {
                if (entityType == typeof(BlittableJsonReaderObject))
                {
                    return document;
                }

                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                using (DefaultRavenContractResolver.RegisterExtensionDataSetter(RegisterMissingProperties))
                {
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

        internal void PopulateEntity(object entity, LazyStringValue id, BlittableJsonReaderObject document, JsonSerializer serializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (serializer == null)
                throw new ArgumentNullException(nameof(serializer));

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Init(document);

                    serializer.Populate(reader, entity);

                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity {id}", ex);
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

                if (propertyValue is BlittableJsonReaderArray propertyArray)
                {
                    simplified |= TrySimplifyJson(propertyArray);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                if (propertyObject.TryGet(Constants.Json.Fields.Type, out string type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject);
                    continue;
                }

                if (ShouldSimplifyJsonBasedOnType(type) == false)
                    continue;

                simplified = true;

                if (document.Modifications == null)
                    document.Modifications = new DynamicJsonValue(document);

                if (propertyObject.TryGet(Constants.Json.Fields.Values, out BlittableJsonReaderArray values) == false)
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

        private static bool ShouldSimplifyJsonBasedOnType(string typeValue)
        {
            var type = Type.GetType(typeValue);

            if (type == null)
                return false;

            if (type.IsArray)
                return true;

            if (type.GetGenericArguments().Length == 0)
                return type == typeof(Enumerable);

            return typeof(IEnumerable).IsAssignableFrom(type.GetGenericTypeDefinition());
        }

        public object ConvertToBlittableIfNeeded(object value)
        {
            if (value is ValueType ||
                  value is string ||
                  value is BlittableJsonReaderArray ||
                  value is BlittableJsonReaderArray)
                return value;

            if (value is IEnumerable && !(value is IDictionary))
            {
                return ((IEnumerable)value).Cast<object>().Select(ConvertToBlittableIfNeeded);

            }
            return ConvertEntityToBlittable(value, documentInfo: null);
        }
    }
}
