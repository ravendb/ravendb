using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
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

            if (documentInfo != null)
                _session.OnBeforeConversionToDocumentInvoke(documentInfo.Id, entity);

            using (DefaultRavenContractResolver.RegisterExtensionDataGetter(FillMissingProperties))
            using (var writer = new BlittableJsonWriter(_session.Context, documentInfo))
            {
                var document = ConvertEntityToBlittableInternal(entity, _session.Conventions, _session.Context, _session.JsonSerializer, writer);

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

        internal static object ConvertToBlittableForCompareExchangeIfNeeded(
            object value,
            DocumentConventions conventions,
            JsonOperationContext context,
            JsonSerializer serializer,
            DocumentInfo documentInfo,
            bool removeIdentityProperty = true)
        {
            if (value == null)
                return null;

            if (value is ValueType ||
                value is string ||
                value is BlittableJsonReaderArray)
                return value;

            if (value is IEnumerable enumerable && !(enumerable is IDictionary))
            {
                return enumerable.Cast<object>()
                    .Select(v => ConvertToBlittableForCompareExchangeIfNeeded(v, conventions, context, serializer, documentInfo, removeIdentityProperty));
            }

            using (var writer = new BlittableJsonWriter(context, documentInfo))
                return ConvertEntityToBlittableInternal(value, conventions, context, serializer, writer, removeIdentityProperty);
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

                var reader = writer.CreateReader();
                var type = entity.GetType();

                var changes = TrySimplifyJson(reader, type);

                if (changes)
                {
                    using (var old = reader)
                    {
                        return context.ReadObject(reader, "convert/entityToBlittable");
                    }
                }

                return reader;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static BlittableJsonReaderObject ConvertEntityToBlittableInternal(
            object entity,
            DocumentConventions conventions,
            JsonOperationContext context,
            JsonSerializer serializer,
            BlittableJsonWriter writer,
            bool removeIdentityProperty = true)
        {
            serializer.Serialize(writer, entity);
            writer.FinalizeDocument();
            var reader = writer.CreateReader();
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;

            var changes = removeIdentityProperty && TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
            changes |= TrySimplifyJson(reader, type);

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
        public object ConvertToEntity(Type entityType, string id, ref BlittableJsonReaderObject document, bool trackEntity)
        {
            try
            {
                if (entityType == typeof(BlittableJsonReaderObject))
                {
                    return document;
                }

                _session.OnBeforeConversionToEntityInvoke(id, entityType, ref document);

                var defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(entityType);
                var entity = defaultValue;

                ExtensionDataSetter dataSetter = null;
                if (trackEntity)
                {
                    dataSetter = RegisterMissingProperties;
                }

                using (DefaultRavenContractResolver.RegisterExtensionDataSetter(dataSetter))
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

                _session.OnAfterConversionToEntityInvoke(id, document, entity);

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
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            PopulateEntity(entity, document, serializer);

            _session.GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);
        }

        internal static void PopulateEntity(object entity, BlittableJsonReaderObject document, JsonSerializer serializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            if (serializer == null)
                throw new ArgumentNullException(nameof(serializer));

            var old = serializer.ObjectCreationHandling;
            serializer.ObjectCreationHandling = ObjectCreationHandling.Replace;

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Init(document);

                    serializer.Populate(reader, entity);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity.", ex);
            }
            finally
            {
                serializer.ObjectCreationHandling = old;
            }
        }

        private static bool TryRemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConventions conventions, bool isDynamicObject)
        {
            var identityProperty = conventions.GetIdentityProperty(entityType);
            if (identityProperty == null)
            {
                if (conventions.AddIdFieldToDynamicObjects && isDynamicObject)
                {
                    if (document.Modifications == null)
                        document.Modifications = new DynamicJsonValue(document);

                    document.Modifications.Remove("Id");
                    return true;
                }

                return false;
            }

            if (document.Modifications == null)
                document.Modifications = new DynamicJsonValue(document);

            document.Modifications.Remove(identityProperty.Name);
            return true;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderObject document, Type rootType)
        {
            var simplified = false;
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyType = GetPropertyType(propertyName, rootType);
                if (propertyType == typeof(JObject) || propertyType == typeof(JArray) || propertyType == typeof(JValue))
                {
                    // don't simplify the property if it's a JObject
                    continue;
                }

                var propertyValue = document[propertyName];

                if (propertyValue is BlittableJsonReaderArray propertyArray)
                {
                    simplified |= TrySimplifyJson(propertyArray, propertyType);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                if (propertyObject.TryGet(Constants.Json.Fields.Type, out string type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject, propertyType);
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

                simplified |= TrySimplifyJson(values, propertyType);
            }

            return simplified;
        }

        private static bool TrySimplifyJson(BlittableJsonReaderArray array, Type rootType)
        {
            var itemType = GetItemType();

            var simplified = false;
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                simplified |= TrySimplifyJson(itemObject, itemType);
            }

            return simplified;

            Type GetItemType()
            {
                if (rootType == null)
                    return null;

                if (rootType.IsArray)
                    return rootType.GetElementType();

                var enumerableInterface = rootType.GetInterface(typeof(IEnumerable<>).Name);
                if (enumerableInterface == null)
                    return null;

                return enumerableInterface.GenericTypeArguments[0];
            }
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

        internal static Type GetPropertyType(string propertyName, Type rootType)
        {
            if (rootType == null)
                return null;

            MemberInfo memberInfo = null;
            try
            {
                memberInfo = ReflectionUtil.GetPropertyOrFieldFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic, propertyName);
            }
            catch (AmbiguousMatchException)
            {
                var memberInfos = ReflectionUtil.GetPropertiesAndFieldsFor(rootType, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic)
                    .Where(x => x.Name == propertyName)
                    .ToList();

                while (typeof(object) != rootType)
                {
                    memberInfo = memberInfos.FirstOrDefault(x => x.DeclaringType == rootType);
                    if (memberInfo != null)
                        break;

                    if (rootType.BaseType == null)
                        break;

                    rootType = rootType.BaseType;
                }
            }

            switch (memberInfo)
            {
                case PropertyInfo pi:
                    return pi.PropertyType;
                case FieldInfo fi:
                    return fi.FieldType;
                default:
                    return null;
            }
        }

        public object ConvertToBlittableIfNeeded(object value)
        {
            if (value is ValueType ||
                  value is string ||
                  value is BlittableJsonReaderObject ||
                  value is BlittableJsonReaderArray)
                return value;

            if (value is IEnumerable && !(value is IDictionary))
            {
                return ((IEnumerable)value).Cast<object>().Select(ConvertToBlittableIfNeeded);
            }
            return ConvertEntityToBlittable(value, documentInfo: null);
        }

        internal void RemoveFromMissing(object entity)
        {
            _missingDictionary.Remove(entity);
        }

        internal void Clear()
        {
            _missingDictionary.Clear();
        }
    }
}
