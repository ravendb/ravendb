//-----------------------------------------------------------------------
// <copyright file="DefaultRavenContractResolver.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json.Serialization;

namespace Raven.Client.Documents.Conventions
{
    /// <summary>
    /// The default json contract will serialize all properties and all public fields
    /// </summary>
    internal class DefaultRavenContractResolver : DefaultContractResolver
    {
        [ThreadStatic]
        private static ExtensionDataSetter _currentExtensionData;

        public struct ClearExtensionData : IDisposable
        {
            public void Dispose()
            {
                _currentExtensionData = null;
            }
        }

        public static ClearExtensionData Register(ExtensionDataSetter setter)
        {
            _currentExtensionData = setter;
            return new ClearExtensionData();
        }

        protected override JsonObjectContract CreateObjectContract(Type objectType)
        {
            var jsonObjectContract = base.CreateObjectContract(objectType);
            jsonObjectContract.ExtensionDataSetter += (o, key, value) =>
            {
                if (jsonObjectContract.Properties.Contains(key))
                    return;
                _currentExtensionData?.Invoke(o, key, value);
            };
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
