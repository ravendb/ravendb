//-----------------------------------------------------------------------
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
using Sparrow.Json;

namespace Raven.Client.Documents.Conventions
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

        public DefaultRavenContractResolver()
        {
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
            if(prop != null)
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
            return jsonObjectContract;
        }

        /// <summary>
        /// Gets the serializable members for the type.
        /// </summary>
        /// <param name="objectType">The type to get serializable members for.</param>
        /// <returns>The serializable members for the type.</returns>
        protected override List<MemberInfo> GetSerializableMembers(Type objectType)
        {
            var serializableMembers = base.GetSerializableMembers(objectType);
            foreach (var toRemove in serializableMembers
                .Where(MembersToFilterOut)
                .ToArray())
            {
                serializableMembers.Remove(toRemove);
            }
            return serializableMembers;
        }

        private static bool MembersToFilterOut(MemberInfo info)
        {
            if (info is EventInfo)
                return true;
            var fieldInfo = info as FieldInfo;
            if (fieldInfo != null && !fieldInfo.IsPublic)
                return true;
            return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute), true).Any();
        }
    }
}
