//-----------------------------------------------------------------------
// <copyright file="DefaultRavenContractResolver.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json.Serialization;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// The default json contract will serialize all properties and all public fields
    /// </summary>
    public class DefaultRavenContractResolver : DefaultContractResolver
    {
        [ThreadStatic]
        private static ExtensionDataSetter currentExtensionData;
        private readonly DisposableAction clearExtensionData = new DisposableAction(() => currentExtensionData = null);

        public IDisposable RegisterForExtensionData(ExtensionDataSetter setter)
        {
            if (currentExtensionData != null)
                throw new InvalidOperationException("Cannot add a data setter because on is already added");
            currentExtensionData = setter;
            return clearExtensionData;
        }

        protected override JsonObjectContract CreateObjectContract(Type objectType)
        {
            var jsonObjectContract = base.CreateObjectContract(objectType);
            jsonObjectContract.ExtensionDataSetter += (o, key, value) =>
            {
                if (jsonObjectContract.Properties.Contains(key))
                    return;
                if (currentExtensionData != null)
                    currentExtensionData(o, key, value);
            };
            return jsonObjectContract;
        }

        /// <summary>
        /// Gets the serializable members for the type.
        /// </summary>
        /// <param name="objectType">The type to get serializable members for.</param>
        /// <returns>The serializable members for the type.</returns>
        protected override System.Collections.Generic.List<MemberInfo> GetSerializableMembers(Type objectType)
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
            return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute),true).Any();
        }
    }
}
