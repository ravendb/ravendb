//-----------------------------------------------------------------------
// <copyright file="DynamicUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.CSharp.RuntimeBinder;

namespace Raven.Database.Json
{
	/// <summary>
	/// Helper class for working with dynamic values completely dynamically
	/// </summary>
	public static class DynamicUtil
	{
		private static readonly ConcurrentDictionary<string, CallSite<Func<CallSite, object, object>>> callsitesCache = new ConcurrentDictionary<string, CallSite<Func<CallSite, object, object>>>();

		/// <summary>
		/// Gets the value dynamically.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="dynamicMemberName">Name of the dynamic member.</param>
		/// <returns></returns>
		public static object GetValueDynamically(object entity, string dynamicMemberName)
		{
			var callsite = callsitesCache.GetOrAdd(dynamicMemberName, s => CallSite<Func<CallSite, object, object>>.Create(
				Binder.GetMember(
					CSharpBinderFlags.None,
					dynamicMemberName,
					null,
					new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) }
					)));

			return callsite.Target(callsite, entity);
		}
	}
}
#endif
