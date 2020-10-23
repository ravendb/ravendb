// <copyright file="DefaultRavenContractResolver.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson
{
    /// <summary>
    /// The default json contract will serialize all properties and all public fields
    /// </summary>
    public class DefaultRavenContractResolver : DefaultContractResolver
    {
        [ThreadStatic]
        private static ExtensionDataSetter _currentExtensionSetter;

        [ThreadStatic]
        private static ExtensionDataGetter _currentExtensionGetter;

        public static BindingFlags? MembersSearchFlag = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        private readonly NewtonsoftJsonSerializationConventions _conventions;

        [ThreadStatic]
        internal static bool RemovedIdentityProperty;

        [ThreadStatic]
        internal static object RootEntity;

        public DefaultRavenContractResolver(ISerializationConventions conventions)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            if (conventions is NewtonsoftJsonSerializationConventions == false)
                throw new ArgumentException($"Conventions must be of '{nameof(NewtonsoftJsonSerializationConventions)}' type.", nameof(conventions));

            _conventions = (NewtonsoftJsonSerializationConventions)conventions;

            if (MembersSearchFlag == null)
            {
                return; // use the JSON.Net default, primarily here because it allows user to turn this off if this is a compact issue.
            }

            var field = typeof(DefaultContractResolver).GetField("DefaultMembersSearchFlags", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            if (field != null)
            {
                field.SetValue(this, MembersSearchFlag);
                return;
            }

            var prop = typeof(DefaultContractResolver).GetProperty("DefaultMembersSearchFlags", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            if (prop != null)
            {
                prop.SetValue(this, MembersSearchFlag);
                return;
            }

            throw new NotSupportedException("Cannot set DefaultMembersSearchFlags via reflection might have been removed. Set DefaultRavenContractResolver.MembersSearchFlag to null to work around this and please report it along with exact version of JSON.Net, please");
        }

        public struct ClearExtensionData : IDisposable
        {
            private readonly ExtensionDataSetter _setter;
            private readonly ExtensionDataGetter _getter;

            public ClearExtensionData(ExtensionDataSetter setter, ExtensionDataGetter getter)
            {
                _setter = setter;
                _getter = getter;
            }

            [SuppressMessage("ReSharper", "DelegateSubtraction")]
            public void Dispose()
            {
                if (_setter != null)
                {
                    _currentExtensionSetter -= _setter;
                }
                if (_getter != null)
                {
                    _currentExtensionGetter -= _getter;
                }
            }
        }

        public static ClearExtensionData RegisterExtensionDataSetter(ExtensionDataSetter setter)
        {
            _currentExtensionSetter += setter;
            return new ClearExtensionData(setter, null);
        }

        public static ClearExtensionData RegisterExtensionDataGetter(ExtensionDataGetter getter)
        {
            _currentExtensionGetter += getter;
            return new ClearExtensionData(null, getter);
        }

        protected override JsonObjectContract CreateObjectContract(Type objectType)
        {
            var jsonObjectContract =
                objectType == typeof(LazyStringValue) ||
                objectType == typeof(BlittableJsonReaderObject)
                ? new JsonObjectContract(objectType)
                : base.CreateObjectContract(objectType);

            jsonObjectContract.ExtensionDataValueType = typeof(JToken);
            jsonObjectContract.ExtensionDataSetter += (o, key, value) =>
            {
                if (jsonObjectContract.Properties.Contains(key))
                    return;
                _currentExtensionSetter?.Invoke(o, key, value);
            };
            jsonObjectContract.ExtensionDataGetter += (o) => _currentExtensionGetter?.Invoke(o);

            var identityProperty = _conventions.Conventions.GetIdentityProperty(objectType);
            if (identityProperty != null)
            {
                var jsonProperty = jsonObjectContract.Properties.GetProperty(identityProperty.Name, StringComparison.Ordinal);
                if (jsonProperty != null)
                    jsonProperty.ShouldSerialize = ShouldSerialize;
            }

            return jsonObjectContract;
        }

        private static bool ShouldSerialize(object value)
        {
            if (value == null)
                return true;

            var rootEntity = RootEntity;
            if (rootEntity == null)
                return true;

            if (ReferenceEquals(rootEntity, value) == false)
                return true;

            if (RemovedIdentityProperty == false)
            {
                RemovedIdentityProperty = true;
                return false;
            }

            return true;
        }

        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);
            if (property.Converter == null)
            {
                var jsonDeserializationDictionaryAttribute = member.GetCustomAttribute<JsonDeserializationStringDictionaryAttribute>();
                if (jsonDeserializationDictionaryAttribute != null)
                    property.Converter = StringDictionaryConverter.For(jsonDeserializationDictionaryAttribute.StringComparison);
            }

            return property;
        }

        /// <summary>
        /// Gets the serializable members for the type.
        /// </summary>
        /// <param name="objectType">The type to get serializable members for.</param>
        /// <returns>The serializable members for the type.</returns>
        protected override List<MemberInfo> GetSerializableMembers(Type objectType)
        {
            var isRecord = objectType.IsRecord();
            var serializableMembers = base.GetSerializableMembers(objectType);
            foreach (var toRemove in serializableMembers
                .Where(x => MembersToFilterOut(x, isRecord))
                .ToArray())
            {
                serializableMembers.Remove(toRemove);
            }

            return serializableMembers;
        }

        private bool MembersToFilterOut(MemberInfo info, bool isRecord)
        {
            if (info is EventInfo)
                return true;
            var fieldInfo = info as FieldInfo;
            if (fieldInfo != null)
            {
                if (fieldInfo.IsPublic == false)
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
            }

            var propertyInfo = info as PropertyInfo;
            if (propertyInfo != null)
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
            }

            return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Length > 0;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowByRefNotSupported() =>
            throw new NotSupportedException("By-ref fields and properties in documents cannot be serialized. You can set RavenDB to ignore them by setting DocumentConventions::Serialization::ThrowErrorOnByRefFields to 'false'. For more details, see https://github.com/JamesNK/Newtonsoft.Json/issues/1552.");

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowPointersNotSupported() =>
            throw new NotSupportedException("Pointer type fields and properties in documents cannot be serialized. You can set RavenDB to ignore them by setting DocumentConventions::Serialization::ThrowOnUnsafeMembers to 'false'");
    }
}
