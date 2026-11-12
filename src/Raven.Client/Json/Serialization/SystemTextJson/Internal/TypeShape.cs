using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class TypeShape
    {
        private static readonly ConcurrentDictionary<Type, TypeShape> _shapes = new();

        public readonly bool IsDictionary;

        public readonly Type KeyType;

        public readonly Type ValueType;

        public readonly Type ItemType;

        public readonly Type CreatedType;

        public readonly bool IsPlainObject;

        public readonly bool IsCollection;

        private TypeShape(Type type)
        {
            IsDictionary = ComputeDictionaryTypes(type, out KeyType, out ValueType);
            ItemType = ComputeItemType(type);
            CreatedType = ComputeCreatedType(type);
            bool isEnumerable = typeof(IEnumerable).IsAssignableFrom(type);
            IsPlainObject = type != typeof(object) && type.IsInterface == false && type.IsAbstract == false && IsDictionary == false && isEnumerable == false;
            IsCollection = isEnumerable && IsDictionary == false && type != typeof(string);
        }

        public static TypeShape Of(Type type) => _shapes.GetOrAdd(type, static t => new TypeShape(t));

        private static bool ComputeDictionaryTypes(Type type, out Type keyType, out Type valueType)
        {
            Type dictionaryInterface = type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(IDictionary<,>) || type.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>))
                ? type
                : type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)) ??
                  type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>));

            if (dictionaryInterface == null)
            {
                if (typeof(IDictionary).IsAssignableFrom(type))
                {
                    keyType = valueType = typeof(object);
                    return true;
                }

                keyType = valueType = null;
                return false;
            }

            Type[] arguments = dictionaryInterface.GetGenericArguments();
            keyType = arguments[0];
            valueType = arguments[1];
            return true;
        }

        private static Type ComputeItemType(Type type)
        {
            if (type.IsArray)
                return type.GetElementType();

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];

            Type enumerableInterface = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return enumerableInterface?.GetGenericArguments()[0] ?? typeof(object);
        }

        private static Type ComputeCreatedType(Type declaredType)
        {
            if (declaredType.IsInterface == false)
                return declaredType;

            if (declaredType.IsGenericType)
            {
                Type definition = declaredType.GetGenericTypeDefinition();
                Type[] arguments = declaredType.GetGenericArguments();

                if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>))
                    return typeof(Dictionary<,>).MakeGenericType(arguments);

                if (definition == typeof(ISet<>))
                    return typeof(HashSet<>).MakeGenericType(arguments);

                if (definition == typeof(IEnumerable<>) || definition == typeof(ICollection<>) || definition == typeof(IList<>) ||
                    definition == typeof(IReadOnlyCollection<>) || definition == typeof(IReadOnlyList<>))
                    return typeof(List<>).MakeGenericType(arguments);

                return declaredType;
            }

            if (declaredType == typeof(IDictionary))
                return typeof(Dictionary<object, object>);

            if (declaredType == typeof(IEnumerable) || declaredType == typeof(ICollection) || declaredType == typeof(IList))
                return typeof(List<object>);

            return declaredType;
        }
    }
}
