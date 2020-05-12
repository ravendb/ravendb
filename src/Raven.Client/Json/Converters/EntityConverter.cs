using System;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Json.Converters
{
    internal class EntityConverter : RavenJsonConverter
    {
        private readonly Type _entityType;
        private readonly MemberInfo _identityProperty;
        private readonly DocumentConventions _conventions;
        private readonly bool _isDynamicObject;

        [ThreadStatic]
        public static bool Used;

        public override bool CanRead => false;

        public EntityConverter(Type entityType, bool isDynamicObject, DocumentConventions conventions)
        {
            _entityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _isDynamicObject = isDynamicObject;
            _identityProperty = conventions.GetIdentityProperty(entityType);
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == _entityType;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotSupportedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            Used = true;

            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            var contract = (JsonObjectContract)serializer.ContractResolver.ResolveContract(_entityType);

            writer.WriteStartObject();
            foreach (var property in contract.Properties)
            {
                if (_identityProperty != null && _identityProperty.Name == property.PropertyName)
                    continue;

                if (_isDynamicObject && _conventions.AddIdFieldToDynamicObjects && property.PropertyName == "Id")
                    continue;

                if (property.Ignored || property.Readable == false || ShouldSerialize(property, value) == false || IsSpecified(property, value) == false)
                    continue;

                writer.WritePropertyName(property.PropertyName);
                serializer.Serialize(writer, property.ValueProvider.GetValue(value), property.PropertyType);
            }
            writer.WriteEndObject();

            static bool ShouldSerialize(JsonProperty property, object target)
            {
                if (property.ShouldSerialize == null)
                    return true;

                return property.ShouldSerialize(target);
            }

            static bool IsSpecified(JsonProperty property, object target)
            {
                if (property.GetIsSpecified == null)
                    return true;

                return property.GetIsSpecified(target);
            }
        }
    }
}
