using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;
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
                object newInstance;
                try
                {
                    newInstance = JsonSerializer.Deserialize(reader.GetUtf8Json(), type, serializer.Options);
                }
                finally
                {
                    reader.ReturnMemory();
                }

                if (newInstance == null)
                    return;

                GetCompiledCopier(type)(newInstance, entity);
            }
        }

        private static readonly ConcurrentDictionary<Type, Action<object, object>> _copiers = new();

        private static Action<object, object> GetCompiledCopier(Type type)
        {
            return _copiers.GetOrAdd(type, CompileCopier);
        }

        private static Action<object, object> CompileCopier(Type type)
        {
            var sourceParam = Expression.Parameter(typeof(object), "source");
            var targetParam = Expression.Parameter(typeof(object), "target");

            var sourceTyped = Expression.Variable(type, "s");
            var targetTyped = Expression.Variable(type, "t");

            var body = new List<Expression>
            {
                Expression.Assign(sourceTyped, Expression.Convert(sourceParam, type)),
                Expression.Assign(targetTyped, Expression.Convert(targetParam, type))
            };

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

                    // t.Prop = s.Prop
                    body.Add(Expression.Assign(
                        Expression.Property(targetTyped, property),
                        Expression.Property(sourceTyped, property)));
                }

                FieldInfo[] fields = currentType.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                for (int i = 0; i < fields.Length; i++)
                {
                    FieldInfo field = fields[i];

                    // t.Field = s.Field
                    body.Add(Expression.Assign(
                        Expression.Field(targetTyped, field),
                        Expression.Field(sourceTyped, field)));
                }
            }

            var block = Expression.Block(new[] { sourceTyped, targetTyped }, body);
            return Expression.Lambda<Action<object, object>>(block, sourceParam, targetParam).Compile();
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
