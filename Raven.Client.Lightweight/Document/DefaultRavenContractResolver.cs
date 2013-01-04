//-----------------------------------------------------------------------
// <copyright file="DefaultRavenContractResolver.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Raven.Imports.Newtonsoft.Json.Serialization;

namespace Raven.Client.Document
{
	/// <summary>
	/// The default json contract will serialize all properties and all public fields
	/// </summary>
	public class DefaultRavenContractResolver : DefaultContractResolver
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="DefaultRavenContractResolver"/> class.
		/// </summary>
		/// <param name="shareCache">If set to <c>true</c> the <see cref="T:Raven.Imports.Newtonsoft.Json.Serialization.DefaultContractResolver"/> will use a cached shared with other resolvers of the same type.
		/// Sharing the cache will significantly performance because expensive reflection will only happen once but could cause unexpected
		/// behavior if different instances of the resolver are suppose to produce different results. When set to false it is highly
		/// recommended to reuse <see cref="T:Raven.Imports.Newtonsoft.Json.Serialization.DefaultContractResolver"/> instances with the <see cref="T:Raven.Imports.Newtonsoft.Json.JsonSerializer"/>.</param>
		public DefaultRavenContractResolver(bool shareCache) : base(shareCache)
		{
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
			return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute),true).Length > 0;
		}
	}
}