using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.CSharp.RuntimeBinder;

namespace Raven.Database.Json
{
	public static class DynamicUtil
	{
		private static readonly ConcurrentDictionary<string, CallSite<Func<CallSite, object, object>>> callsitesCache = new ConcurrentDictionary<string, CallSite<Func<CallSite, object, object>>>();

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