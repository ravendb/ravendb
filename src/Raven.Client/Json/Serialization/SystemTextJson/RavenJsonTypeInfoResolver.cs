using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    internal sealed class RavenJsonTypeInfoResolver : DefaultJsonTypeInfoResolver
    {
        private const string NewtonsoftJsonConverterAttributeName = "Newtonsoft.Json.JsonConverterAttribute";

        private const string NewtonsoftJsonExtensionDataAttributeName = "Newtonsoft.Json.JsonExtensionDataAttribute";

        private const string NewtonsoftJsonConstructorAttributeName = "Newtonsoft.Json.JsonConstructorAttribute";

        private const string NewtonsoftStringEnumConverterName = "Newtonsoft.Json.Converters.StringEnumConverter";

        private const string NewtonsoftJTokenName = "Newtonsoft.Json.Linq.JToken";

        private const string NewtonsoftJsonPropertyAttributeName = "Newtonsoft.Json.JsonPropertyAttribute";

        private const string NewtonsoftJsonRequiredAttributeName = "Newtonsoft.Json.JsonRequiredAttribute";

        private const string NewtonsoftJsonObjectAttributeName = "Newtonsoft.Json.JsonObjectAttribute";

        private const string NewtonsoftJsonContainerAttributeName = "Newtonsoft.Json.JsonContainerAttribute";

        private const int NewtonsoftMemberSerializationOptIn = 1;

        private const int NewtonsoftMemberSerializationFields = 2;

        private const int NewtonsoftNullValueHandlingIgnore = 1;

        private const int NewtonsoftDefaultValueHandlingIgnore = 1;

        private const int NewtonsoftDefaultValueHandlingPopulate = 2;

        private static readonly ConcurrentDictionary<Type, Action<object, IEnumerable>> _collectionAdders = new();

        private static readonly ConcurrentDictionary<Type, Dictionary<string, int>> _memberRanks = new();

        private static readonly MethodInfo _addToCollectionMethod = typeof(RavenJsonTypeInfoResolver).GetMethod(nameof(AddToCollection), BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly object[] _callbackArguments = { new StreamingContext(StreamingContextStates.All) };

        private readonly SystemTextJsonSerializationConventions _conventions;

        private readonly TypeNameHandling _typeNameHandling;

        private GenerateEntityIdOnTheClient _identity;

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
            if (type == typeof(ExpandoObject))
                return JsonMetadataServices.CreateValueInfo<ExpandoObject>(options, ExpandoObjectConverter.Instance);

            AssertMembersCanBeResolved(type);
            AssertNotNewtonsoftLinqType(type);
            AssertNoNewtonsoftTypeConverter(type);
            AssertNoNewtonsoftReferencePreservation(type);
            AssertNoNewtonsoftNamingStrategy(type);
            AssertNoNewtonsoftItemConverter(type);
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
            ApplyNewtonsoftMemberSerialization(typeInfo);
            ApplyStringDictionaryComparers(typeInfo);
            ApplyTypeNameHandling(typeInfo);
            ApplyPropertyRenames(typeInfo);
            ApplyPropertyNameConverter(typeInfo);
            AddMetadataIdentityProperties(typeInfo);
            SetupNonPublicConstructor(typeInfo);
            SetupUnboundParameterizedConstructor(typeInfo);
            PopulateReadOnlyMembers(typeInfo);
            SetupIdentityPropertyConditionalSerialization(typeInfo);
            ApplyNewtonsoftMemberOrder(typeInfo);
            ApplyNewtonsoftNullValueHandling(typeInfo);
            ApplyNewtonsoftDefaultValueHandling(typeInfo);
            ApplySerializationCallbacks(typeInfo);
            AssertNoNewtonsoftMemberAttributes(typeInfo);
            AssertNewtonsoftConstructorIsUsed(typeInfo.Type);
        }

        private static void AssertNotNewtonsoftLinqType(Type type)
        {
            for (Type current = type; current != null; current = current.BaseType)
            {
                if (current.FullName == NewtonsoftJTokenName)
                    ThrowNewtonsoftLinqTypeNotSupported(type);
            }
        }

        private void AssertNoNewtonsoftTypeConverter(Type type)
        {
            if (type.IsDefined(typeof(JsonConverterAttribute), inherit: true))
                return;

            object converter = GetNewtonsoftAttribute(type, NewtonsoftJsonConverterAttributeName);
            if (converter == null)
            {
                foreach (Type @interface in type.GetInterfaces())
                {
                    converter = GetNewtonsoftAttribute(@interface, NewtonsoftJsonConverterAttributeName);
                    if (converter != null)
                        break;
                }
            }

            if (converter != null && IsTranslatedConverter(converter, type) == false)
                ThrowNewtonsoftConverterNotSupported($"Type '{type.FullName}'");
        }

        private void AssertNoNewtonsoftMemberAttributes(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member)
                    continue;

                if (member.IsDefined(typeof(JsonConverterAttribute), inherit: true) == false)
                {
                    object converter = GetNewtonsoftAttribute(member, NewtonsoftJsonConverterAttributeName);
                    if (converter != null && IsTranslatedConverter(converter, property.PropertyType) == false)
                        ThrowNewtonsoftConverterNotSupported($"Member '{member.DeclaringType?.FullName}.{member.Name}'");
                }

                object jsonProperty = GetNewtonsoftAttribute(member, NewtonsoftJsonPropertyAttributeName);
                if (jsonProperty != null && PreservesNewtonsoftReferences(jsonProperty) && IsWrittenAsArray(property.PropertyType) == false)
                    ThrowNewtonsoftReferencesNotSupported($"Member '{member.DeclaringType?.FullName}.{member.Name}'");

                if (jsonProperty != null && HasNewtonsoftNamingStrategy(jsonProperty))
                    ThrowNewtonsoftNamingStrategyNotSupported($"Member '{member.DeclaringType?.FullName}.{member.Name}'");

                if (jsonProperty != null)
                    AssertNewtonsoftItemConverterIsTranslated(jsonProperty, property.PropertyType, $"Member '{member.DeclaringType?.FullName}.{member.Name}'");

                if (member.IsDefined(typeof(JsonExtensionDataAttribute), inherit: true) == false &&
                    GetNewtonsoftAttribute(member, NewtonsoftJsonExtensionDataAttributeName) != null)
                    ThrowNewtonsoftExtensionDataNotSupported(member);
            }
        }

        private static void AssertNoNewtonsoftReferencePreservation(Type type)
        {
            if (IsWrittenAsArray(type))
                return;

            object container = GetNewtonsoftContainerAttribute(type);
            bool preservesReferences = container != null
                ? PreservesNewtonsoftReferences(container)
                : GetDataContractAttribute(type) is { IsReference: true };

            if (preservesReferences)
                ThrowNewtonsoftReferencesNotSupported($"Type '{type.FullName}'");
        }

        private static object GetNewtonsoftContainerAttribute(Type type)
        {
            object container = FindNewtonsoftContainerAttribute(type);
            if (container != null)
                return container;

            foreach (Type @interface in type.GetInterfaces())
            {
                container = FindNewtonsoftContainerAttribute(@interface);
                if (container != null)
                    return container;
            }

            return null;
        }

        private static object FindNewtonsoftContainerAttribute(Type type)
        {
            foreach (object attribute in type.GetCustomAttributes(true))
            {
                for (Type current = attribute.GetType(); current != null; current = current.BaseType)
                {
                    if (current.FullName == NewtonsoftJsonContainerAttributeName)
                        return attribute;
                }
            }

            return null;
        }

        private static void AssertNoNewtonsoftNamingStrategy(Type type)
        {
            object container = GetNewtonsoftContainerAttribute(type);
            if (container != null && HasNewtonsoftNamingStrategy(container))
                ThrowNewtonsoftNamingStrategyNotSupported($"Type '{type.FullName}'");
        }

        private static bool HasNewtonsoftNamingStrategy(object attribute)
        {
            return attribute.GetType().GetProperty("NamingStrategyType")?.GetValue(attribute) != null;
        }

        private static bool IsWrittenAsArray(Type type)
        {
            if (type == typeof(string) || typeof(IEnumerable).IsAssignableFrom(type) == false || typeof(IDictionary).IsAssignableFrom(type) || IsGenericDictionary(type))
                return false;

            foreach (Type @interface in type.GetInterfaces())
            {
                if (IsGenericDictionary(@interface))
                    return false;
            }

            return true;
        }

        private static bool IsGenericDictionary(Type type)
        {
            if (type.IsGenericType == false)
                return false;

            Type definition = type.GetGenericTypeDefinition();
            return definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>);
        }

        private static bool PreservesNewtonsoftReferences(object attribute)
        {
            Type attributeType = attribute.GetType();
            return attributeType.GetProperty("IsReference")?.GetValue(attribute) is true ||
                   attributeType.GetProperty("ItemIsReference")?.GetValue(attribute) is true;
        }

        private static void AssertNewtonsoftConstructorIsUsed(Type type)
        {
            ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            ConstructorInfo marked = null;
            bool hasParameterlessConstructor = false;
            int publicConstructors = 0;

            foreach (ConstructorInfo constructor in constructors)
            {
                if (constructor.IsDefined(typeof(JsonConstructorAttribute), inherit: false))
                    return;

                if (marked == null && GetNewtonsoftAttribute(constructor, NewtonsoftJsonConstructorAttributeName) != null)
                    marked = constructor;

                if (constructor.GetParameters().Length == 0)
                    hasParameterlessConstructor = true;

                if (constructor.IsPublic)
                    publicConstructors++;
            }

            if (marked == null || marked.GetParameters().Length == 0)
                return;

            if (hasParameterlessConstructor == false && publicConstructors == 1 && marked.IsPublic)
                return;

            ThrowNewtonsoftConstructorNotSupported(type);
        }

        private bool IsTranslatedConverter(object converterAttribute, Type valueType)
        {
            Type attributeType = converterAttribute.GetType();
            return IsTranslatedConverter(
                (Type)attributeType.GetProperty("ConverterType")?.GetValue(converterAttribute),
                (object[])attributeType.GetProperty("ConverterParameters")?.GetValue(converterAttribute),
                valueType);
        }

        private bool IsTranslatedConverter(Type converterType, object[] converterParameters, Type valueType)
        {
            Type enumType = valueType == null ? null : Nullable.GetUnderlyingType(valueType) ?? valueType;
            if (enumType is not { IsEnum: true } || _conventions.Conventions?.SaveEnumsAsIntegers == true)
                return false;

            return converterType?.FullName == NewtonsoftStringEnumConverterName && (converterParameters == null || converterParameters.Length == 0);
        }

        private void AssertNoNewtonsoftItemConverter(Type type)
        {
            object container = GetNewtonsoftContainerAttribute(type);
            if (container != null)
                AssertNewtonsoftItemConverterIsTranslated(container, type, $"Type '{type.FullName}'");
        }

        private void AssertNewtonsoftItemConverterIsTranslated(object attribute, Type collectionType, string target)
        {
            Type attributeType = attribute.GetType();
            var converterType = (Type)attributeType.GetProperty("ItemConverterType")?.GetValue(attribute);
            if (converterType == null)
                return;

            var converterParameters = (object[])attributeType.GetProperty("ItemConverterParameters")?.GetValue(attribute);
            if (IsTranslatedConverter(converterType, converterParameters, GetItemType(collectionType)) == false)
                ThrowNewtonsoftItemConverterNotSupported(target);
        }

        private static Type GetItemType(Type type)
        {
            if (type == typeof(string))
                return null;

            if (type.IsArray)
                return type.GetElementType();

            Type itemType = null;
            foreach (Type candidate in type.GetInterfaces().Prepend(type))
            {
                if (candidate.IsGenericType == false)
                    continue;

                Type definition = candidate.GetGenericTypeDefinition();
                if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>))
                    return candidate.GetGenericArguments()[1];

                if (definition == typeof(IEnumerable<>))
                    itemType = candidate.GetGenericArguments()[0];
            }

            return itemType;
        }

        private static object GetNewtonsoftAttribute(ICustomAttributeProvider provider, string attributeName)
        {
            object[] attributes = provider.GetCustomAttributes(true);
            for (int i = 0; i < attributes.Length; i++)
            {
                if (attributes[i].GetType().FullName == attributeName)
                    return attributes[i];
            }

            return null;
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
                BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
                if (type != typeInfo.Type)
                    flags |= BindingFlags.Public;

                foreach (PropertyInfo propertyInfo in type.GetProperties(flags))
                {
                    if (propertyInfo.GetIndexParameters().Length > 0 || memberNames.Add(propertyInfo.Name) == false)
                        continue;

                    if (ShouldFilterProperty(propertyInfo, isRecord) || IsIgnoredBySystemTextJson(propertyInfo) || IsHiddenBySystemTextJsonIgnoredMember(typeInfo.Type, type, propertyInfo.Name))
                        continue;

                    typeInfo.Properties.Add(CreateJsonProperty(typeInfo, propertyInfo, propertyInfo.PropertyType, CreateGetter(propertyInfo), CreateSetter(propertyInfo)));
                }

                foreach (FieldInfo fieldInfo in type.GetFields(flags))
                {
                    if ((fieldInfo.IsPublic == false && fieldInfo.IsDefined(typeof(ForceJsonSerializationAttribute)) == false) || memberNames.Add(fieldInfo.Name) == false)
                        continue;

                    if (ShouldFilterField(fieldInfo) || IsIgnoredBySystemTextJson(fieldInfo) || IsHiddenBySystemTextJsonIgnoredMember(typeInfo.Type, type, fieldInfo.Name))
                        continue;

                    Action<object, object> set = fieldInfo.IsInitOnly ? null : fieldInfo.SetValue;
                    typeInfo.Properties.Add(CreateJsonProperty(typeInfo, fieldInfo, fieldInfo.FieldType, fieldInfo.GetValue, set));
                }
            }
        }

        private static void ApplyNewtonsoftMemberSerialization(JsonTypeInfo typeInfo)
        {
            int memberSerialization = GetNewtonsoftMemberSerialization(typeInfo.Type);
            if (memberSerialization != NewtonsoftMemberSerializationOptIn && memberSerialization != NewtonsoftMemberSerializationFields)
                return;

            for (int i = typeInfo.Properties.Count - 1; i >= 0; i--)
            {
                if (typeInfo.Properties[i].AttributeProvider is not MemberInfo member)
                    continue;

                bool serialized = memberSerialization == NewtonsoftMemberSerializationFields
                    ? member is FieldInfo
                    : (member.IsDefined(typeof(DataMemberAttribute), inherit: true) && HasDataContract(member.DeclaringType)) ||
                      GetNewtonsoftAttribute(member, NewtonsoftJsonPropertyAttributeName) != null ||
                      GetNewtonsoftAttribute(member, NewtonsoftJsonRequiredAttributeName) != null;

                if (serialized == false)
                    typeInfo.Properties.RemoveAt(i);
            }
        }

        private static int GetNewtonsoftMemberSerialization(Type type)
        {
            object jsonObject = GetNewtonsoftAttribute(type, NewtonsoftJsonObjectAttributeName);
            if (jsonObject == null)
            {
                foreach (Type @interface in type.GetInterfaces())
                {
                    jsonObject = GetNewtonsoftAttribute(@interface, NewtonsoftJsonObjectAttributeName);
                    if (jsonObject != null)
                        break;
                }
            }

            if (jsonObject != null)
                return Convert.ToInt32(jsonObject.GetType().GetProperty("MemberSerialization")?.GetValue(jsonObject));

            return HasDataContract(type) ? NewtonsoftMemberSerializationOptIn : 0;
        }

        private static bool HasDataContract(Type type)
        {
            return GetDataContractAttribute(type) != null;
        }

        private static DataContractAttribute GetDataContractAttribute(Type type)
        {
            for (Type current = type; current != null; current = current.BaseType)
            {
                if (current.GetCustomAttribute<DataContractAttribute>(inherit: false) is { } dataContract)
                    return dataContract;
            }

            return null;
        }

        private static JsonPropertyInfo CreateJsonProperty(JsonTypeInfo typeInfo, MemberInfo member, Type memberType, Func<object, object> get, Action<object, object> set)
        {
            string name = typeInfo.Options.PropertyNamingPolicy?.ConvertName(member.Name) ?? member.Name;

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

        private static void ApplyNewtonsoftMemberOrder(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Properties.Count < 2)
                return;

            Dictionary<string, int> ranks = _memberRanks.GetOrAdd(typeInfo.Type, GetNewtonsoftMemberRanks);
            List<JsonPropertyInfo> ordered = typeInfo.Properties
                .OrderBy(property => property.AttributeProvider is MemberInfo member && ranks.TryGetValue(member.Name, out int rank) ? rank : int.MaxValue)
                .OrderBy(property => property.AttributeProvider is MemberInfo member ? GetNewtonsoftOrder(member) : -1)
                .ToList();

            typeInfo.Properties.Clear();
            foreach (JsonPropertyInfo property in ordered)
                typeInfo.Properties.Add(property);
        }

        private static Dictionary<string, int> GetNewtonsoftMemberRanks(Type type)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

            var ranks = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (FieldInfo field in type.GetFields(flags))
                AddRank(ranks, field.Name);

            for (Type baseType = type.BaseType; baseType != null; baseType = baseType.BaseType)
            {
                foreach (FieldInfo field in baseType.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (field.IsPrivate)
                        AddRank(ranks, field.Name);
                }
            }

            foreach (PropertyInfo property in type.GetProperties(flags))
                AddRank(ranks, property.Name);

            for (Type baseType = type.BaseType; baseType != null; baseType = baseType.BaseType)
            {
                foreach (PropertyInfo property in baseType.GetProperties(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                    AddRank(ranks, property.Name);
            }

            return ranks;
        }

        private static int GetNewtonsoftOrder(MemberInfo member)
        {
            foreach (CustomAttributeData attribute in member.GetCustomAttributesData())
            {
                if (attribute.AttributeType.FullName != NewtonsoftJsonPropertyAttributeName)
                    continue;

                foreach (CustomAttributeNamedArgument argument in attribute.NamedArguments)
                {
                    if (argument.MemberName == "Order")
                        return (int)argument.TypedValue.Value;
                }

                return -1;
            }

            if (member.GetCustomAttribute<DataMemberAttribute>(inherit: true) is { Order: not -1 } dataMember && HasDataContract(member.DeclaringType))
                return dataMember.Order;

            return -1;
        }

        private static void AddRank(Dictionary<string, int> ranks, string name)
        {
            if (ranks.ContainsKey(name) == false)
                ranks[name] = ranks.Count;
        }

        private static void ApplyNewtonsoftNullValueHandling(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member || (property.PropertyType.IsValueType && Nullable.GetUnderlyingType(property.PropertyType) == null))
                    continue;

                object attribute = GetNewtonsoftAttribute(member, NewtonsoftJsonPropertyAttributeName);
                if (attribute == null || IsNullValueIgnored(attribute) == false)
                    continue;

                Func<object, object, bool> shouldSerialize = property.ShouldSerialize;
                property.ShouldSerialize = shouldSerialize == null
                    ? static (_, value) => value != null
                    : (target, value) => value != null && shouldSerialize(target, value);

                Action<object, object> set = property.Set;
                if (set != null)
                {
                    property.Set = (target, value) =>
                    {
                        if (value != null)
                            set(target, value);
                    };
                }
            }
        }

        private static bool IsNullValueIgnored(object jsonPropertyAttribute)
        {
            object value = jsonPropertyAttribute.GetType().GetProperty("NullValueHandling")?.GetValue(jsonPropertyAttribute);
            return value != null && Convert.ToInt32(value) == NewtonsoftNullValueHandlingIgnore;
        }

        private static void ApplyNewtonsoftDefaultValueHandling(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member)
                    continue;

                int handling = GetNewtonsoftDefaultValueHandling(member);
                if ((handling & NewtonsoftDefaultValueHandlingIgnore) == 0)
                    continue;

                object defaultValue = member.GetCustomAttribute<DefaultValueAttribute>(inherit: true) is { } defaultValueAttribute
                    ? defaultValueAttribute.Value
                    : property.PropertyType.IsValueType ? Activator.CreateInstance(property.PropertyType) : null;

                Func<object, object, bool> shouldSerialize = property.ShouldSerialize;
                property.ShouldSerialize = shouldSerialize == null
                    ? (_, value) => IsNewtonsoftDefaultValue(value, defaultValue) == false
                    : (target, value) => IsNewtonsoftDefaultValue(value, defaultValue) == false && shouldSerialize(target, value);

                Action<object, object> set = property.Set;
                if (set != null && (handling & NewtonsoftDefaultValueHandlingPopulate) == 0)
                {
                    property.Set = (target, value) =>
                    {
                        if (IsNewtonsoftDefaultValue(value, defaultValue) == false)
                            set(target, value);
                    };
                }
            }
        }

        private static int GetNewtonsoftDefaultValueHandling(MemberInfo member)
        {
            object jsonProperty = GetNewtonsoftAttribute(member, NewtonsoftJsonPropertyAttributeName);
            if (jsonProperty != null)
                return Convert.ToInt32(jsonProperty.GetType().GetProperty("DefaultValueHandling")?.GetValue(jsonProperty));

            return member.GetCustomAttribute<DataMemberAttribute>(inherit: true) is { EmitDefaultValue: false } && HasDataContract(member.DeclaringType)
                ? NewtonsoftDefaultValueHandlingIgnore
                : 0;
        }

        private static bool IsNewtonsoftDefaultValue(object value, object defaultValue)
        {
            if (Equals(value, defaultValue))
                return true;

            if (value == null || defaultValue == null || value.GetType() == defaultValue.GetType())
                return false;

            if (IsInteger(value) && IsInteger(defaultValue))
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture) == Convert.ToDecimal(defaultValue, CultureInfo.InvariantCulture);

            if (value is double or float or decimal && defaultValue is double or float or decimal)
            {
                double left = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                double right = Convert.ToDouble(defaultValue, CultureInfo.InvariantCulture);
                double tolerance = (Math.Abs(left) + Math.Abs(right) + 10.0) * 2.2204460492503131E-16;
                return left == right || Math.Abs(left - right) < tolerance;
            }

            return false;
        }

        private static bool IsInteger(object value)
        {
            return Type.GetTypeCode(value.GetType()) is >= TypeCode.SByte and <= TypeCode.UInt64;
        }

        private static void ApplySerializationCallbacks(JsonTypeInfo typeInfo)
        {
            List<MethodInfo> onSerializing = null;
            List<MethodInfo> onSerialized = null;
            List<MethodInfo> onDeserializing = null;
            List<MethodInfo> onDeserialized = null;

            var hierarchy = new Stack<Type>();
            for (Type type = typeInfo.Type; type != null && type != typeof(object); type = type.BaseType)
                hierarchy.Push(type);

            foreach (Type type in hierarchy)
            {
                foreach (MethodInfo method in type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (IsSerializationCallback(method) == false)
                        continue;

                    AddCallback(ref onSerializing, method, typeof(OnSerializingAttribute));
                    AddCallback(ref onSerialized, method, typeof(OnSerializedAttribute));
                    AddCallback(ref onDeserializing, method, typeof(OnDeserializingAttribute));
                    AddCallback(ref onDeserialized, method, typeof(OnDeserializedAttribute));
                }
            }

            typeInfo.OnSerializing = CombineCallbacks(typeInfo.OnSerializing, onSerializing);
            typeInfo.OnSerialized = CombineCallbacks(typeInfo.OnSerialized, onSerialized);
            typeInfo.OnDeserializing = CombineCallbacks(typeInfo.OnDeserializing, onDeserializing);
            typeInfo.OnDeserialized = CombineCallbacks(typeInfo.OnDeserialized, onDeserialized);
        }

        private static bool IsSerializationCallback(MethodInfo method)
        {
            if (method.ContainsGenericParameters || method.ReturnType != typeof(void))
                return false;

            ParameterInfo[] parameters = method.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == typeof(StreamingContext);
        }

        private static void AddCallback(ref List<MethodInfo> callbacks, MethodInfo method, Type attributeType)
        {
            if (method.IsDefined(attributeType, inherit: false))
                (callbacks ??= new List<MethodInfo>()).Add(method);
        }

        private static Action<object> CombineCallbacks(Action<object> existing, List<MethodInfo> callbacks)
        {
            if (callbacks == null)
                return existing;

            MethodInfo[] methods = callbacks.ToArray();
            return target =>
            {
                existing?.Invoke(target);
                for (int i = 0; i < methods.Length; i++)
                {
                    try
                    {
                        methods[i].Invoke(target, _callbackArguments);
                    }
                    catch (TargetInvocationException e) when (e.InnerException != null)
                    {
                        ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                    }
                }
            };
        }

        private static bool IsHiddenBySystemTextJsonIgnoredMember(Type type, Type declaringType, string name)
        {
            for (Type current = type; current != declaringType; current = current.BaseType)
            {
                MemberInfo[] members = current.GetMember(name, MemberTypes.Field | MemberTypes.Property, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
                for (int i = 0; i < members.Length; i++)
                {
                    if (IsIgnoredBySystemTextJson(members[i]))
                        return true;
                }
            }

            return false;
        }

        private void AddMetadataIdentityProperties(JsonTypeInfo typeInfo)
        {
            DocumentConventions conventions = _conventions.Conventions;
            if (conventions == null || conventions.GetIdentityProperty(typeInfo.Type) == null)
                return;

            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                string name = typeInfo.Properties[i].Name;
                if (name == Constants.Documents.Metadata.Key || name == Constants.Documents.Metadata.Id)
                    return;
            }

            GenerateEntityIdOnTheClient identity = _identity ??= new GenerateEntityIdOnTheClient(conventions, generateIdAsync: null);

            JsonPropertyInfo metadata = typeInfo.CreateJsonPropertyInfo(typeof(MetadataIdentity), Constants.Documents.Metadata.Key);
            metadata.CustomConverter = MetadataIdentityConverter.Instance;
            metadata.Set = (target, value) =>
            {
                if (value is MetadataIdentity { Id: not null } metadataIdentity)
                    identity.TrySetIdentity(target, metadataIdentity.Id, metadataIdentity.IsProjection);
            };
            typeInfo.Properties.Add(metadata);

            JsonPropertyInfo topLevelId = typeInfo.CreateJsonPropertyInfo(typeof(string), Constants.Documents.Metadata.Id);
            topLevelId.Set = (target, value) =>
            {
                if (value is string id && (identity.TryGetIdFromInstance(target, out string existing) == false || existing == null))
                    identity.TrySetIdentity(target, id);
            };
            typeInfo.Properties.Add(topLevelId);
        }

        private sealed class MetadataIdentity
        {
            public string Id;

            public bool IsProjection;
        }

        private sealed class MetadataIdentityConverter : JsonConverter<MetadataIdentity>
        {
            public static readonly MetadataIdentityConverter Instance = new();

            public override MetadataIdentity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                {
                    reader.Skip();
                    return null;
                }

                var metadataIdentity = new MetadataIdentity();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                {
                    bool isId = reader.ValueTextEquals(Constants.Documents.Metadata.Id);
                    bool isProjection = isId == false && reader.ValueTextEquals(Constants.Documents.Metadata.Projection);
                    reader.Read();

                    if (isId && reader.TokenType == JsonTokenType.String)
                        metadataIdentity.Id = reader.GetString();
                    else if (isProjection && reader.TokenType is JsonTokenType.True or JsonTokenType.False)
                        metadataIdentity.IsProjection = reader.GetBoolean();
                    else
                        reader.Skip();
                }

                return metadataIdentity;
            }

            public override void Write(Utf8JsonWriter writer, MetadataIdentity value, JsonSerializerOptions options)
            {
                throw new NotSupportedException();
            }
        }

        private static void SetupUnboundParameterizedConstructor(JsonTypeInfo typeInfo)
        {
            if (typeInfo.CreateObject != null || typeInfo.Type.IsAbstract || typeInfo.Type.IsInterface)
                return;

            ConstructorInfo constructor = GetParameterizedConstructor(typeInfo.Type);
            if (constructor == null)
                return;

            ParameterInfo[] parameters = constructor.GetParameters();
            if (parameters.Length == 0 || AllParametersBind(typeInfo, parameters))
                return;

            var arguments = new object[parameters.Length];
            for (int i = 0; i < parameters.Length; i++)
            {
                ParameterInfo parameter = parameters[i];
                arguments[i] = parameter.HasDefaultValue
                    ? parameter.DefaultValue
                    : parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType) : null;
            }

            typeInfo.CreateObject = () => constructor.Invoke(arguments);
        }

        private static ConstructorInfo GetParameterizedConstructor(Type type)
        {
            foreach (ConstructorInfo constructor in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            {
                if (constructor.IsDefined(typeof(JsonConstructorAttribute), inherit: false))
                    return constructor;
            }

            ConstructorInfo[] publicConstructors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public);
            return publicConstructors.Length == 1 ? publicConstructors[0] : null;
        }

        private static bool AllParametersBind(JsonTypeInfo typeInfo, ParameterInfo[] parameters)
        {
            foreach (ParameterInfo parameter in parameters)
            {
                bool bound = false;
                for (int i = 0; i < typeInfo.Properties.Count && bound == false; i++)
                {
                    JsonPropertyInfo property = typeInfo.Properties[i];
                    bound = property.AttributeProvider is MemberInfo member &&
                            string.Equals(member.Name, parameter.Name, StringComparison.OrdinalIgnoreCase) &&
                            property.PropertyType == parameter.ParameterType;
                }

                if (bound == false)
                    return false;
            }

            return true;
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

        private static void ApplyPropertyRenames(JsonTypeInfo typeInfo)
        {
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                JsonPropertyInfo property = typeInfo.Properties[i];
                if (property.AttributeProvider is not MemberInfo member)
                    continue;

                string name = JsonPropertyNameResolver.GetSystemTextJsonPropertyName(member);
                if (name != null)
                    property.Name = name;
            }
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

                if (JsonPropertyNameResolver.GetSystemTextJsonPropertyName(member) != null)
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
            if (member is FieldInfo fieldInfo && fieldInfo.IsDefined(typeof(NonSerializedAttribute), inherit: false))
                return true;

            object[] attributes = member.GetCustomAttributes(true);
            for (int i = 0; i < attributes.Length; i++)
            {
                if (attributes[i] is IgnoreDataMemberAttribute || attributes[i].GetType().FullName == "Newtonsoft.Json.JsonIgnoreAttribute")
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

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftConverterNotSupported(string target) =>
            throw new NotSupportedException(
                $"{target} is annotated with {NewtonsoftJsonConverterAttributeName}. " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires System.Text.Json.Serialization.JsonConverterAttribute with a System.Text.Json converter, " +
                $"or a converter registered through {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.CustomizeJsonSerializerOptions)}.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftLinqTypeNotSupported(Type type) =>
            throw new NotSupportedException(
                $"Type '{type.FullName}' is a Newtonsoft.Json LINQ type. " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires object, dynamic, System.Text.Json.JsonElement or System.Text.Json.Nodes.JsonNode for untyped JSON.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftExtensionDataNotSupported(MemberInfo member) =>
            throw new NotSupportedException(
                $"Member '{member.DeclaringType?.FullName}.{member.Name}' is annotated with {NewtonsoftJsonExtensionDataAttributeName}. " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires System.Text.Json.Serialization.JsonExtensionDataAttribute.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftReferencesNotSupported(string target) =>
            throw new NotSupportedException(
                $"{target} preserves object references with Newtonsoft.Json (IsReference or ItemIsReference). " +
                $"{nameof(SystemTextJsonSerializationConventions)} writes every object in full and reads Newtonsoft.Json '$ref' placeholders as empty objects, " +
                "so remove reference preservation and convert documents that contain '$ref' before switching.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftNamingStrategyNotSupported(string target) =>
            throw new NotSupportedException(
                $"{target} names members with a Newtonsoft.Json naming strategy (NamingStrategyType). " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires System.Text.Json.Serialization.JsonPropertyNameAttribute, " +
                $"or a naming policy set through {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.CustomizeJsonSerializerOptions)}.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftItemConverterNotSupported(string target) =>
            throw new NotSupportedException(
                $"{target} converts its items with a Newtonsoft.Json converter (ItemConverterType). " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires a System.Text.Json converter for the item type, " +
                $"registered through {nameof(SystemTextJsonSerializationConventions)}.{nameof(SystemTextJsonSerializationConventions.CustomizeJsonSerializerOptions)}.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNewtonsoftConstructorNotSupported(Type type) =>
            throw new NotSupportedException(
                $"Type '{type.FullName}' selects its constructor with {NewtonsoftJsonConstructorAttributeName}. " +
                $"{nameof(SystemTextJsonSerializationConventions)} requires System.Text.Json.Serialization.JsonConstructorAttribute on that constructor.");
    }
}
