using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        private static readonly byte[] TypePropertyName = Encoding.UTF8.GetBytes(Constants.Json.Fields.Type);

        protected readonly SystemTextJsonSerializationConventions Conventions;

        protected BlittableJsonConverterBase(ISerializationConventions conventions)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            Conventions = conventions as SystemTextJsonSerializationConventions
                          ?? throw new ArgumentException($"Expected {nameof(SystemTextJsonSerializationConventions)} but got {conventions.GetType()}", nameof(conventions));
        }

        protected object DeserializeEntity(Type type, BlittableJsonReaderObject json, string id, out Type entityType)
        {
            DocumentConventions conventions = Conventions.Conventions;
            object defaultValue = InMemoryDocumentSessionOperations.GetDefaultValue(type);
            object entity = defaultValue;
            entityType = type;

            string documentTypeAsString = conventions.GetClrType(id, json);
            if (documentTypeAsString != null)
            {
                Type documentType = conventions.ResolveTypeFromClrTypeName(documentTypeAsString);
                if (documentType != null && type.IsAssignableFrom(documentType))
                {
                    entityType = documentType;
                    entity = Conventions.DeserializeEntityFromBlittable(documentType, json);
                }
            }

            if (Equals(entity, defaultValue))
                entity = Conventions.DeserializeEntityFromBlittable(type, json);

            return entity;
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json)
        {
            var jsonSerializer = ((ISerializationConventions)Conventions).CreateSerializer();
            PopulateEntity(entity, json, jsonSerializer);
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json, IJsonSerializer jsonSerializer)
        {
            if (entity == null)
                throw new ArgumentNullException(nameof(entity));
            if (json == null)
                throw new ArgumentNullException(nameof(json));
            if (jsonSerializer == null)
                throw new ArgumentNullException(nameof(jsonSerializer));

            var options = ((SystemTextJsonJsonSerializer)jsonSerializer).Options;

            try
            {
                PopulateEntityInternal(entity, json, options);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not populate entity.", ex);
            }
        }

        private void PopulateEntityInternal(object entity, BlittableJsonReaderObject json, JsonSerializerOptions options)
        {
            var type = entity.GetType();
            object newInstance;

            using (var reader = new SystemTextJsonBlittableReader())
            {
                reader.Initialize(json);
                newInstance = Conventions.Deserialize(reader.GetUtf8Json(), type, options);
            }

            if (newInstance == null || TryPopulateDictionary(entity, newInstance))
                return;

            JsonPropertyMap properties = Conventions.GetJsonProperties(type);
            foreach (string propertyName in json.GetPropertyNames())
            {
                if (properties.TryGetValue(propertyName, out JsonPropertyInfo property) == false || property.Get == null)
                    continue;

                GetPopulateSetter(property)?.Invoke(entity, property.Get(newInstance));
            }

            foreach (JsonPropertyInfo property in properties.ByName.Values)
            {
                if (property.IsExtensionData)
                    PopulateExtensionData(entity, newInstance, property);
            }
        }

        private static void PopulateExtensionData(object entity, object source, JsonPropertyInfo property)
        {
            object values = property.Get(source);
            if (values == null)
                return;

            object existing = property.Get(entity);
            if (existing == null)
            {
                GetPopulateSetter(property)?.Invoke(entity, values);
                return;
            }

            switch (existing)
            {
                case IDictionary target when values is IDictionary entries:
                    foreach (DictionaryEntry entry in entries)
                        target[entry.Key] = entry.Value;
                    break;
                case IDictionary<string, JsonNode> target when values is IDictionary<string, JsonNode> entries:
                    foreach (KeyValuePair<string, JsonNode> entry in entries)
                        target[entry.Key] = entry.Value?.DeepClone();
                    break;
            }
        }

        private static Action<object, object> GetPopulateSetter(JsonPropertyInfo property)
        {
            switch (property.AttributeProvider)
            {
                case PropertyInfo { SetMethod: null }:
                    return null;
                case FieldInfo { IsInitOnly: true } field:
                    return RavenJsonTypeInfoResolver.HasNewtonsoftMemberAttribute(field) ? field.SetValue : null;
                default:
                    return property.Set ?? GetMemberSetter(property.AttributeProvider);
            }
        }

        private static bool TryPopulateDictionary(object entity, object source)
        {
            switch (entity)
            {
                case IDictionary<string, object> target when source is IDictionary<string, object> values:
                    foreach (KeyValuePair<string, object> entry in values)
                        target[entry.Key] = entry.Value;
                    return true;
                case IDictionary target when source is IDictionary values:
                    foreach (DictionaryEntry entry in values)
                        target[entry.Key] = entry.Value;
                    return true;
                default:
                    return false;
            }
        }

        private static Action<object, object> GetMemberSetter(object attributeProvider)
        {
            switch (attributeProvider)
            {
                case PropertyInfo propertyInfo when propertyInfo.SetMethod != null:
                    return (target, value) => propertyInfo.SetValue(target, value);
                case FieldInfo fieldInfo when fieldInfo.IsInitOnly == false:
                    return (target, value) => fieldInfo.SetValue(target, value);
                default:
                    return null;
            }
        }

        protected static BlittableJsonReaderObject ConvertToBlittable(
            object entity,
            DocumentConventions conventions,
            JsonOperationContext context,
            bool removeIdentityProperty,
            SystemTextJsonJsonSerializer serializer,
            IMetadataDictionary metadata,
            DocumentInfo documentInfo)
        {
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;
            var hasIdentityProperty = conventions.GetIdentityProperty(type) != null;
            var useResolver = isDynamicObject == false;

            BlittableJsonReaderObject reader;
            if (useResolver)
            {
                using (RavenJsonTypeInfoResolver.EnterRoot(entity, removeIdentityProperty, hasIdentityProperty))
                    reader = serializer.SerializeToBlittable(entity, type, metadata, documentInfo, context);
            }
            else
            {
                reader = serializer.SerializeToBlittable(entity, type, metadata, documentInfo, context);
            }

            if (useResolver == false || hasIdentityProperty && RavenJsonTypeInfoResolver.RemovedIdentityProperty == false)
            {
                var changes = removeIdentityProperty && BlittableJsonConverterHelper.TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
                changes |= reader.HasPropertyNameInDocument(TypePropertyName) && HasTypedObjectMember(reader.AsView()) &&
                           BlittableJsonConverterHelper.TrySimplifyJson(reader, type, ShouldSkipSimplification);

                if (changes)
                {
                    using (var old = reader)
                    {
                        reader = context.ReadObject(reader, "convert/entityToBlittable");
                    }
                }
            }

            return reader;
        }

        private static bool HasTypedObjectMember(in BlittableJsonReaderObject.ObjectView json)
        {
            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyName(i, out BlittableJsonToken token, out int position);
                switch (token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.StartObject:
                        BlittableJsonReaderObject.ObjectView member = json.Owner.GetObjectView(token, position);
                        if (member.GetPropertyIndex(TypePropertyName) >= 0 || HasTypedObjectMember(member))
                            return true;
                        break;
                    case BlittableJsonToken.StartArray:
                        if (HasTypedObjectMember(json.Owner.GetArrayView(token, position)))
                            return true;
                        break;
                    case BlittableJsonToken.EmbeddedBlittable:
                        return true;
                }
            }

            return false;
        }

        private static bool HasTypedObjectMember(in BlittableJsonReaderObject.ArrayView items)
        {
            for (int i = 0; i < items.Count; i++)
            {
                BlittableJsonToken token = items.GetItem(i, out int position);
                switch (token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.StartObject:
                        if (HasTypedObjectMember(items.Owner.GetObjectView(token, position)))
                            return true;
                        break;
                    case BlittableJsonToken.StartArray:
                        if (HasTypedObjectMember(items.Owner.GetArrayView(token, position)))
                            return true;
                        break;
                    case BlittableJsonToken.EmbeddedBlittable:
                        return true;
                }
            }

            return false;
        }

        private static bool ShouldSkipSimplification(Type propertyType)
        {
            return propertyType == typeof(JsonElement) || propertyType == typeof(JsonNode) ||
                   propertyType == typeof(JsonObject) || propertyType == typeof(JsonArray) ||
                   propertyType == typeof(JsonValue);
        }
    }
}
