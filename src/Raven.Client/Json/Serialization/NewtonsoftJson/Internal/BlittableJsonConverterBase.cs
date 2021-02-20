using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
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

            var serializer = (NewtonsoftJsonJsonSerializer)jsonSerializer;
            var old = serializer.ObjectCreationHandling;
            serializer.ObjectCreationHandling = ObjectCreationHandling.Replace;

            try
            {
                using (var reader = new BlittableJsonReader())
                {
                    reader.Initialize(json);

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

        protected static BlittableJsonReaderObject ToBlittableInternal(
             object entity,
             DocumentConventions conventions,
             JsonOperationContext context,
             IJsonSerializer serializer,
             IJsonWriter writer,
             bool removeIdentityProperty = true)
        {
            var usesDefaultContractResolver = ((JsonSerializer)serializer).ContractResolver.GetType() == typeof(DefaultRavenContractResolver);
            var type = entity.GetType();
            var isDynamicObject = entity is IDynamicMetaObjectProvider;
            var willUseDefaultContractResolver = usesDefaultContractResolver && isDynamicObject == false;
            var hasIdentityProperty = conventions.GetIdentityProperty(type) != null;

            try
            {
                if (willUseDefaultContractResolver)
                {
                    DefaultRavenContractResolver.RootEntity = removeIdentityProperty && hasIdentityProperty ? entity : null;
                    DefaultRavenContractResolver.RemovedIdentityProperty = false;
                }

                serializer.Serialize(writer, entity);
            }
            finally
            {
                if (willUseDefaultContractResolver)
                    DefaultRavenContractResolver.RootEntity = null;
            }

            writer.FinalizeDocument();
            var reader = writer.CreateReader();

            if (willUseDefaultContractResolver == false || hasIdentityProperty && DefaultRavenContractResolver.RemovedIdentityProperty == false)
            {
                //This is to handle the case when user defined it's own contract resolver
                //or we are serializing dynamic object

                var changes = removeIdentityProperty && TryRemoveIdentityProperty(reader, type, conventions, isDynamicObject);
                changes |= TrySimplifyJson(reader, type);

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

        internal class TypeCacheItem : IEquatable<TypeCacheItem>
        {
            public string Property;
            public Type Type;

            public bool Equals(TypeCacheItem other)
            {
                if (other == null)
                    return false;
                return this.Type == other.Type && this.Property == other.Property;
            }

            public override int GetHashCode()
            {
                return Sparrow.Hashing.HashCombiner.CombineInline(Property.GetHashCode(), Type.GetHashCode());
            }
        }

        private static Dictionary<TypeCacheItem, Type> _propertyTypeCache = new();

        internal static Type GetPropertyType(string propertyName, Type rootType)
        {
            if (rootType == null)
                return null;

            var item = new TypeCacheItem() { Property = propertyName, Type = rootType };
            if (_propertyTypeCache.TryGetValue(item, out var result))
                return result;

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

            Type type = null;
            switch (memberInfo)
            {
                case PropertyInfo pi:
                    type = pi.PropertyType;
                    break;
                case FieldInfo fi:
                    type = fi.FieldType;
                    break;
            }

            var dict = new Dictionary<TypeCacheItem, Type>(_propertyTypeCache);
            dict[item] = type;
            _propertyTypeCache = dict;
            return type;
        }
    }
}
