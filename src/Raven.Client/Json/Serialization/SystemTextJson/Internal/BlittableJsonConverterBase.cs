using System;
using System.Dynamic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal abstract class BlittableJsonConverterBase : IBlittableJsonConverterBase
    {
        protected readonly ISerializationConventions Conventions;

        protected BlittableJsonConverterBase(ISerializationConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        }

        public void PopulateEntity(object entity, BlittableJsonReaderObject json)
        {
            var jsonSerializer = Conventions.CreateSerializer();
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

            // STJ has no Populate() equivalent. Deserialize to a new instance and copy properties.
            using (var reader = new SystemTextJsonBlittableReader())
            {
                reader.Initialize(json);
                var type = entity.GetType();
                var serializer = (SystemTextJsonJsonSerializer)jsonSerializer;
                object newInstance = JsonSerializer.Deserialize(reader.GetUtf8Json(), type, serializer.Options);

                if (newInstance == null)
                    return;

                CopyProperties(newInstance, entity, type);
            }
        }

        private static void CopyProperties(object source, object target, Type type)
        {
            for (Type currentType = type; currentType != null && currentType != typeof(object); currentType = currentType.BaseType)
            {
                PropertyInfo[] properties = currentType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                for (int i = 0; i < properties.Length; i++)
                {
                    PropertyInfo property = properties[i];
                    if (property.CanRead == false || property.CanWrite == false)
                        continue;

                    if (property.GetIndexParameters().Length > 0)
                        continue;

                    object value = property.GetValue(source);
                    property.SetValue(target, value);
                }

                FieldInfo[] fields = currentType.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                for (int i = 0; i < fields.Length; i++)
                {
                    FieldInfo field = fields[i];
                    object value = field.GetValue(source);
                    field.SetValue(target, value);
                }
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
