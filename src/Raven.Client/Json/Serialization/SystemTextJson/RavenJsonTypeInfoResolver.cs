using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    internal sealed class RavenJsonTypeInfoResolver : DefaultJsonTypeInfoResolver
    {
        private static readonly ConcurrentDictionary<Type, Action<object, IEnumerable>> _collectionAdders = new();

        private static readonly MethodInfo _addToCollectionMethod = typeof(RavenJsonTypeInfoResolver).GetMethod(nameof(AddToCollection), BindingFlags.NonPublic | BindingFlags.Static);

        private readonly SystemTextJsonSerializationConventions _conventions;

        private readonly TypeNameHandling _typeNameHandling;

        [ThreadStatic]
        internal static object RootEntity;

        [ThreadStatic]
        internal static bool RemovedIdentityProperty;

        public RavenJsonTypeInfoResolver(SystemTextJsonSerializationConventions conventions, TypeNameHandling typeNameHandling)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _typeNameHandling = typeNameHandling;
            Modifiers.Add(ModifyTypeInfo);
        }

        /// <summary>
        /// Returns a resolver that tries the user's source-generated context first,
        /// then falls back to reflection, applying Raven modifications to both.
        /// </summary>
        internal IJsonTypeInfoResolver WithSourceGenerationContext(JsonSerializerContext context)
        {
            return new SourceGenCombinedResolver(context, this);
        }

        private sealed class SourceGenCombinedResolver : IJsonTypeInfoResolver
        {
            private readonly JsonSerializerContext _context;
            private readonly RavenJsonTypeInfoResolver _fallback;

            public SourceGenCombinedResolver(JsonSerializerContext context, RavenJsonTypeInfoResolver fallback)
            {
                _context = context;
                _fallback = fallback;
            }

            public JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
            {
                // Try the source-generated context first (returns null for unknown types)
                JsonTypeInfo typeInfo = ((IJsonTypeInfoResolver)_context).GetTypeInfo(type, options);
                if (typeInfo != null)
                {
                    // Apply Raven modifications (identity property handling, property filtering)
                    _fallback.ModifyTypeInfo(typeInfo);
                    return typeInfo;
                }

                // Fall back to reflection-based resolver (Raven mods applied via Modifiers)
                return ((IJsonTypeInfoResolver)_fallback).GetTypeInfo(type, options);
            }
        }

        public override JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            AssertMembersCanBeResolved(type);
            return base.GetTypeInfo(type, options);
        }

        private void AssertMembersCanBeResolved(Type type)
        {
            foreach (PropertyInfo property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                AssertMemberTypeCanBeResolved(property.PropertyType);

            foreach (FieldInfo field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
                AssertMemberTypeCanBeResolved(field.FieldType);
        }

        private void AssertMemberTypeCanBeResolved(Type memberType)
        {
#if NETSTANDARD2_0
            if (memberType.IsByRef && _conventions.IgnoreByRefMembers == false)
#else
            if ((memberType.IsByRef || memberType.IsByRefLike) && _conventions.IgnoreByRefMembers == false)
#endif
                ThrowByRefNotSupported();

            if (memberType.IsPointer && _conventions.IgnoreUnsafeMembers == false)
                ThrowPointersNotSupported();
        }

        private void ModifyTypeInfo(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Kind != JsonTypeInfoKind.Object)
                return;

            bool isRecord = typeInfo.Type.IsRecord();

            FilterProperties(typeInfo, isRecord);
            AddNonPublicMembers(typeInfo, isRecord);
            ApplyStringDictionaryComparers(typeInfo);
            ApplyTypeNameHandling(typeInfo);
            ApplyPropertyNameConverter(typeInfo);
            SetupNonPublicConstructor(typeInfo);
            PopulateReadOnlyMembers(typeInfo);
            SetupIdentityPropertyConditionalSerialization(typeInfo);
        }

        private void AddNonPublicMembers(JsonTypeInfo typeInfo, bool isRecord)
        {
            var memberNames = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member)
                    continue;

                memberNames.Add(member.Name);

                if (member is PropertyInfo propertyInfo)
                {
                    property.Get ??= CreateGetter(propertyInfo);
                    property.Set ??= CreateSetter(propertyInfo);
                }
            }

            for (Type type = typeInfo.Type; type != null && type != typeof(object); type = type.BaseType)
            {
                foreach (PropertyInfo propertyInfo in type.GetProperties(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (propertyInfo.GetIndexParameters().Length > 0 || memberNames.Add(propertyInfo.Name) == false)
                        continue;

                    if (ShouldFilterProperty(propertyInfo, isRecord) || IsIgnoredBySystemTextJson(propertyInfo))
                        continue;

                    typeInfo.Properties.Add(CreateJsonProperty(typeInfo, propertyInfo, propertyInfo.PropertyType, CreateGetter(propertyInfo), CreateSetter(propertyInfo)));
                }

                foreach (FieldInfo fieldInfo in type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (fieldInfo.IsDefined(typeof(ForceJsonSerializationAttribute)) == false || memberNames.Add(fieldInfo.Name) == false)
                        continue;

                    if (ShouldFilterField(fieldInfo) || IsIgnoredBySystemTextJson(fieldInfo))
                        continue;

                    Action<object, object> set = fieldInfo.IsInitOnly ? null : fieldInfo.SetValue;
                    typeInfo.Properties.Add(CreateJsonProperty(typeInfo, fieldInfo, fieldInfo.FieldType, fieldInfo.GetValue, set));
                }
            }
        }

        private static JsonPropertyInfo CreateJsonProperty(JsonTypeInfo typeInfo, MemberInfo member, Type memberType, Func<object, object> get, Action<object, object> set)
        {
            string name = member.GetCustomAttribute<JsonPropertyNameAttribute>(inherit: true)?.Name
                          ?? typeInfo.Options.PropertyNamingPolicy?.ConvertName(member.Name)
                          ?? member.Name;

            JsonPropertyInfo property = typeInfo.CreateJsonPropertyInfo(memberType, name);
            property.AttributeProvider = member;
            property.Get = get;
            property.Set = set;
            return property;
        }

        private static Func<object, object> CreateGetter(PropertyInfo propertyInfo)
        {
            if (propertyInfo.GetMethod == null)
                return null;

            return propertyInfo.GetValue;
        }

        private static Action<object, object> CreateSetter(PropertyInfo propertyInfo)
        {
            if (propertyInfo.SetMethod == null)
                return null;

            return propertyInfo.SetValue;
        }

        private static bool IsIgnoredBySystemTextJson(MemberInfo member)
        {
            return member.GetCustomAttribute<JsonIgnoreAttribute>(inherit: true) is { Condition: JsonIgnoreCondition.Always };
        }

        private static void PopulateReadOnlyMembers(JsonTypeInfo typeInfo)
        {
            JsonSerializerOptions options = typeInfo.Options;
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.Set != null || property.Get == null || property.PropertyType.IsValueType || property.PropertyType == typeof(string))
                    continue;

                Func<object, object> get = property.Get;
                property.Set = (target, value) => PopulateExisting(get(target), value, options);
            }
        }

        private static void PopulateExisting(object target, object value, JsonSerializerOptions options)
        {
            if (target == null || value == null || ReferenceEquals(target, value) || target is string)
                return;

            if (target is IEnumerable)
            {
                AddItems(target, value);
                return;
            }

            Type targetType = target.GetType();
            Type valueType = value.GetType();
            Type contractType = targetType.IsAssignableFrom(valueType) ? targetType : valueType.IsAssignableFrom(targetType) ? valueType : null;
            if (contractType == null)
                return;

            JsonTypeInfo typeInfo = options.GetTypeInfo(contractType);
            if (typeInfo.Kind != JsonTypeInfoKind.Object)
                return;

            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.Get != null && property.Set != null)
                    property.Set(target, property.Get(value));
            }
        }

        private static void AddItems(object target, object items)
        {
            if (items is not IEnumerable source)
                return;

            switch (target)
            {
                case IDictionary dictionary:
                    if (dictionary.IsReadOnly || items is not IDictionary sourceDictionary)
                        return;

                    foreach (DictionaryEntry entry in sourceDictionary)
                        dictionary[entry.Key] = entry.Value;
                    return;

                case IList list:
                    if (list.IsReadOnly || list.IsFixedSize)
                        return;

                    foreach (object item in source)
                        list.Add(item);
                    return;
            }

            _collectionAdders.GetOrAdd(target.GetType(), CreateCollectionAdder)?.Invoke(target, source);
        }

        private static Action<object, IEnumerable> CreateCollectionAdder(Type type)
        {
            foreach (Type @interface in type.GetInterfaces())
            {
                if (@interface.IsGenericType && @interface.GetGenericTypeDefinition() == typeof(ICollection<>))
                {
                    MethodInfo method = _addToCollectionMethod.MakeGenericMethod(@interface.GetGenericArguments());
                    return (Action<object, IEnumerable>)method.CreateDelegate(typeof(Action<object, IEnumerable>));
                }
            }

            return null;
        }

        private static void AddToCollection<T>(object target, IEnumerable items)
        {
            var collection = (ICollection<T>)target;
            if (collection.IsReadOnly)
                return;

            foreach (object item in items)
                collection.Add((T)item);
        }

        private void ApplyTypeNameHandling(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.CustomConverter != null)
                    continue;

                JsonConverter converter = TypeNameConverter.TryCreate(property.PropertyType, _typeNameHandling, typeInfo.Options);
                if (converter != null)
                    property.CustomConverter = converter;
            }
        }

        private static void ApplyStringDictionaryComparers(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.Set == null || property.AttributeProvider is not MemberInfo member)
                    continue;

                JsonDeserializationStringDictionaryAttribute attribute = member.GetCustomAttribute<JsonDeserializationStringDictionaryAttribute>();
                if (attribute == null)
                    continue;

                Type dictionaryType = GetStringDictionaryType(property.PropertyType);
                if (dictionaryType == null)
                    continue;

                StringComparer comparer = JsonDeserializationBase.GetStringComparer(attribute.StringComparison);
                Action<object, object> set = property.Set;
                property.Set = (target, value) =>
                {
                    if (value is IDictionary source)
                    {
                        var dictionary = (IDictionary)Activator.CreateInstance(dictionaryType, comparer);
                        foreach (DictionaryEntry entry in source)
                            dictionary.Add(entry.Key, entry.Value);

                        value = dictionary;
                    }

                    set(target, value);
                };
            }
        }

        private static Type GetStringDictionaryType(Type type)
        {
            if (type.IsGenericType == false)
                return null;

            Type definition = type.GetGenericTypeDefinition();
            if (definition != typeof(Dictionary<,>) && definition != typeof(IDictionary<,>))
                return null;

            Type[] arguments = type.GetGenericArguments();
            return arguments[0] == typeof(string) ? typeof(Dictionary<,>).MakeGenericType(arguments) : null;
        }

        private void ApplyPropertyNameConverter(JsonTypeInfo typeInfo)
        {
            DocumentConventions conventions = _conventions.Conventions;
            if (conventions?.PropertyNameConverter == null)
                return;

            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member)
                    continue;

                if (member.IsDefined(typeof(JsonPropertyNameAttribute), inherit: true))
                    continue;

                property.Name = conventions.GetConvertedPropertyNameFor(member);
            }
        }

        private void FilterProperties(JsonTypeInfo typeInfo, bool isRecord)
        {
            for (int i = typeInfo.Properties.Count - 1; i >= 0; i--)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (ShouldFilterProperty(property, isRecord))
                    typeInfo.Properties.RemoveAt(i);
            }
        }

        private bool ShouldFilterProperty(JsonPropertyInfo property, bool isRecord)
        {
            ICustomAttributeProvider attributeProvider = property.AttributeProvider;
            if (attributeProvider == null)
                return false;

            if (attributeProvider is EventInfo)
                return true;

            if (attributeProvider is FieldInfo fieldInfo)
                return ShouldFilterField(fieldInfo);

            if (attributeProvider is PropertyInfo propertyInfo)
                return ShouldFilterProperty(propertyInfo, isRecord);

            if (HasCompilerGeneratedAttribute(attributeProvider))
                return true;

            if (HasJsonIgnoreAttribute(attributeProvider))
                return true;

            return false;
        }

        private bool ShouldFilterField(FieldInfo fieldInfo)
        {
            if (fieldInfo.IsPublic == false && fieldInfo.IsDefined(typeof(ForceJsonSerializationAttribute)) == false)
                return true;

#if NETSTANDARD2_0
            if (fieldInfo.FieldType.IsByRef)
#else
            if (fieldInfo.FieldType.IsByRef || fieldInfo.FieldType.IsByRefLike)
#endif
            {
                if (_conventions.IgnoreByRefMembers == false)
                    ThrowByRefNotSupported();
                return true;
            }

            if (fieldInfo.FieldType == typeof(IntPtr) || fieldInfo.FieldType.IsPointer)
            {
                if (_conventions.IgnoreUnsafeMembers == false)
                    ThrowPointersNotSupported();
                return true;
            }

            if (HasCompilerGeneratedAttribute(fieldInfo))
                return true;

            if (HasJsonIgnoreAttribute(fieldInfo))
                return true;

            return false;
        }

        private bool ShouldFilterProperty(PropertyInfo propertyInfo, bool isRecord)
        {
            if (isRecord && propertyInfo.Name == Sparrow.Extensions.TypeExtensions.RecordEqualityContractPropertyName)
                return true;

#if NETSTANDARD2_0
            if (propertyInfo.PropertyType.IsByRef)
#else
            if (propertyInfo.PropertyType.IsByRef || propertyInfo.PropertyType.IsByRefLike)
#endif
            {
                if (_conventions.IgnoreByRefMembers == false)
                    ThrowByRefNotSupported();
                return true;
            }

            if (propertyInfo.PropertyType == typeof(IntPtr) || propertyInfo.PropertyType.IsPointer)
            {
                if (_conventions.IgnoreUnsafeMembers == false)
                    ThrowPointersNotSupported();
                return true;
            }

            if (HasCompilerGeneratedAttribute(propertyInfo))
                return true;

            if (HasJsonIgnoreAttribute(propertyInfo))
                return true;

            return false;
        }

        private static bool HasCompilerGeneratedAttribute(ICustomAttributeProvider member)
        {
            return member.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Length > 0;
        }

        private static bool HasJsonIgnoreAttribute(ICustomAttributeProvider member)
        {
            // Check both Newtonsoft and STJ JsonIgnore by attribute full name to avoid
            // hard dependency issues. STJ's own [JsonIgnore] is already handled by
            // DefaultJsonTypeInfoResolver, but Newtonsoft's [JsonIgnore] is not.
            object[] attributes = member.GetCustomAttributes(true);
            for (int i = 0; i < attributes.Length; i++)
            {
                string fullName = attributes[i].GetType().FullName;
                if (fullName == "Newtonsoft.Json.JsonIgnoreAttribute")
                    return true;
            }

            return false;
        }

        private void SetupIdentityPropertyConditionalSerialization(JsonTypeInfo typeInfo)
        {
            if (_conventions.Conventions == null)
                return;

            MemberInfo identityProperty = _conventions.Conventions.GetIdentityProperty(typeInfo.Type);
            if (identityProperty == null)
                return;

            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                bool isIdentityProperty = property.AttributeProvider is MemberInfo member
                    ? member.Name == identityProperty.Name
                    : property.Name == identityProperty.Name;

                if (isIdentityProperty)
                {
                    property.ShouldSerialize = ShouldSerializeIdentityProperty;
                    break;
                }
            }
        }

        private static bool ShouldSerializeIdentityProperty(object containingObject, object propertyValue)
        {
            if (containingObject == null)
                return true;

            object rootEntity = RootEntity;
            if (rootEntity == null)
                return true;

            if (ReferenceEquals(rootEntity, containingObject) == false)
                return true;

            if (RemovedIdentityProperty == false)
            {
                RemovedIdentityProperty = true;
                return false;
            }

            return true;
        }

        private static void SetupNonPublicConstructor(JsonTypeInfo typeInfo)
        {
            // STJ by default only uses public parameterless constructors.
            // Replicate Newtonsoft's AllowNonPublicDefaultConstructor behavior.
            if (typeInfo.CreateObject != null)
                return;

            ConstructorInfo constructor = typeInfo.Type.GetConstructor(
                BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                Type.EmptyTypes,
                modifiers: null);

            if (constructor != null)
            {
                typeInfo.CreateObject = () => constructor.Invoke(null);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowByRefNotSupported() =>
            throw new NotSupportedException(
                "By-ref fields and properties in documents cannot be serialized. " +
                $"You can set RavenDB to ignore them by setting {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.IgnoreByRefMembers)} to 'true'.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowPointersNotSupported() =>
            throw new NotSupportedException(
                "Pointer type fields and properties in documents cannot be serialized. " +
                $"You can set RavenDB to ignore them by setting {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.IgnoreUnsafeMembers)} to 'true'.");
    }
}
