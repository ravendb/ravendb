using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Json.Serialization
{
    internal static class BlittableJsonConverterHelper
    {
        internal static bool TryRemoveIdentityProperty(BlittableJsonReaderObject document, Type entityType, DocumentConventions conventions, bool isDynamicObject)
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

            document.Modifications.Remove(conventions.GetConvertedPropertyNameFor(identityProperty));
            return true;
        }

        internal static bool TrySimplifyJson(BlittableJsonReaderObject document, Type rootType, Func<Type, bool> shouldSkipSimplification)
        {
            var simplified = false;
            foreach (var propertyName in document.GetPropertyNames())
            {
                var propertyType = GetPropertyType(propertyName, rootType);
                if (propertyType != null && shouldSkipSimplification(propertyType))
                {
                    // don't simplify the property if the caller indicates it should be skipped
                    continue;
                }

                var propertyValue = document[propertyName];

                if (propertyValue is BlittableJsonReaderArray propertyArray)
                {
                    simplified |= TrySimplifyJson(propertyArray, propertyType, shouldSkipSimplification);
                    continue;
                }

                var propertyObject = propertyValue as BlittableJsonReaderObject;
                if (propertyObject == null)
                    continue;

                if (propertyObject.TryGet(Constants.Json.Fields.Type, out string type) == false)
                {
                    simplified |= TrySimplifyJson(propertyObject, propertyType, shouldSkipSimplification);
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

                simplified |= TrySimplifyJson(values, propertyType, shouldSkipSimplification);
            }

            return simplified;
        }

        internal static bool TrySimplifyJson(BlittableJsonReaderArray array, Type rootType, Func<Type, bool> shouldSkipSimplification)
        {
            var itemType = GetItemType();

            var simplified = false;
            foreach (var item in array)
            {
                var itemObject = item as BlittableJsonReaderObject;
                if (itemObject == null)
                    continue;

                simplified |= TrySimplifyJson(itemObject, itemType, shouldSkipSimplification);
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

        internal static bool ShouldSimplifyJsonBasedOnType(string typeValue)
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

        // Cache: (rootType, propertyName) → property/field Type.
        // Types are immutable, so the mapping never changes.
        private static readonly ConcurrentDictionary<(Type, string), Type> _propertyTypeCache = new();

        internal static Type GetPropertyType(string propertyName, Type rootType)
        {
            if (rootType == null)
                return null;

            return _propertyTypeCache.GetOrAdd((rootType, propertyName), static key => ResolvePropertyType(key.Item2, key.Item1));
        }

        private static Type ResolvePropertyType(string propertyName, Type rootType)
        {
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
    }
}
