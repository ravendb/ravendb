//-----------------------------------------------------------------------
// <copyright file="DynamicUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.CSharp.RuntimeBinder;

namespace Raven.Abstractions.Json
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
			Func<string, CallSite<Func<CallSite, object, object>>> valueFactory = s => CallSite<Func<CallSite, object, object>>.Create(
				Binder.GetMember(
					CSharpBinderFlags.None,
					dynamicMemberName,
					null,
					new[] {CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null)}
					));
			var callsite = callsitesCache.GetOrAdd(dynamicMemberName, valueFactory);

			return callsite.Target(callsite, entity);
		}
	}
}
#endif