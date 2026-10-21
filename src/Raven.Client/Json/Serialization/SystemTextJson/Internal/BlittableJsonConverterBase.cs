using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        protected readonly SystemTextJsonSerializationConventions Conventions;

        protected BlittableJsonConverterBase(ISerializationConventions conventions)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            Conventions = conventions as SystemTextJsonSerializationConventions
                          ?? throw new ArgumentException($"Expected {nameof(SystemTextJsonSerializationConventions)} but got {conventions.GetType()}", nameof(conventions));
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

            var type = entity.GetType();
            var options = ((SystemTextJsonJsonSerializer)jsonSerializer).Options;
            object newInstance;

            using (var reader = new SystemTextJsonBlittableReader())
            {
                reader.Initialize(json);
                newInstance = JsonSerializer.Deserialize(reader.GetUtf8Json(), type, options);
            }

            if (newInstance == null)
                return;

            Dictionary<string, JsonPropertyInfo> properties = Conventions.GetJsonProperties(type);
            foreach (string propertyName in json.GetPropertyNames())
            {
                if (properties.TryGetValue(propertyName, out JsonPropertyInfo property) == false || property.Get == null)
                    continue;

                Action<object, object> setter = property.Set ?? GetMemberSetter(property.AttributeProvider);
                setter?.Invoke(entity, property.Get(newInstance));
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

        protected static BlittableJsonReaderObject ToBlittableInternal(
            object entity,
            DocumentConventions conventions,
            JsonOperationContext context,
            IJsonSerializer serializer,
            IJsonWriter writer,
            bool removeIdentityProperty = true)
        {
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;
            var hasIdentityProperty = conventions.GetIdentityProperty(type) != null;
            var useResolver = isDynamicObject == false;

            if (useResolver)
            {
                RavenJsonTypeInfoResolver.RootEntity = removeIdentityProperty && hasIdentityProperty ? entity : null;
                RavenJsonTypeInfoResolver.RemovedIdentityProperty = false;

                try
                {
                    serializer.Serialize(writer, entity);
                }
                finally
                {
                    RavenJsonTypeInfoResolver.RootEntity = null;
                }
            }
            else
            {
                serializer.Serialize(writer, entity);
            }

            writer.FinalizeDocument();

            var reader = writer.CreateReader();

            if (useResolver == false || hasIdentityProperty && RavenJsonTypeInfoResolver.RemovedIdentityProperty == false)
            {
                var changes = removeIdentityProperty && BlittableJsonConverterHelper.TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
                changes |= BlittableJsonConverterHelper.TrySimplifyJson(reader, type, ShouldSkipSimplification);

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

        private static bool ShouldSkipSimplification(Type propertyType)
        {
            return propertyType == typeof(JsonElement) || propertyType == typeof(JsonNode) ||
                   propertyType == typeof(JsonObject) || propertyType == typeof(JsonArray) ||
                   propertyType == typeof(JsonValue);
        }
    }
}
