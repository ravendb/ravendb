// -----------------------------------------------------------------------
//  <copyright file="PerformanceCountersMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.Linq;

namespace Raven.Database.Util
{
	using System;
	using System.DirectoryServices.AccountManagement;
	using System.Security.Principal;

	public static class PerformanceCountersUtils
	{
		public static void EnsurePerformanceCountersMonitoringAccess(string userName)
		{
			var performanceMonitorUsersGroupSid = new SecurityIdentifier(WellKnownSidType.BuiltinPerformanceMonitoringUsersSid, null);
			var machineCtx = new PrincipalContext(ContextType.Machine);
			Principal userPrincipal;

			if (userName.StartsWith("IIS", StringComparison.OrdinalIgnoreCase) || userName.StartsWith("NT AUTHORITY", StringComparison.OrdinalIgnoreCase))
			{
				// if IIS or NT AUTHORITY user then current principal is GroupPrincipal

				var acc = new NTAccount(userName);
				var sid = acc.Translate(typeof(SecurityIdentifier));
				userPrincipal = GroupPrincipal.FindByIdentity(machineCtx, IdentityType.Sid, sid.Value);
			}
			else
			{
				userPrincipal = UserPrincipal.FindByIdentity(machineCtx, IdentityType.Name, userName);
			}

			if (userPrincipal == null)
			{
				throw new InvalidOperationException("Could not find principal for user " + userName + " to grant him an access to Performance Counters");
			}

			using (var performanceMonitorUsersGroupPrincipal = GroupPrincipal.FindByIdentity(machineCtx, IdentityType.Sid, performanceMonitorUsersGroupSid.Value))
			{
				if (performanceMonitorUsersGroupPrincipal == null)
				{
					throw new InvalidOperationException("Could not find principal for Performance Monitoring Users group");
				}
				try
				{
					if (performanceMonitorUsersGroupPrincipal.Members.Contains(userPrincipal) == false)
					{
						performanceMonitorUsersGroupPrincipal.Members.Add(userPrincipal);
						performanceMonitorUsersGroupPrincipal.Save();
					}
				}
				catch (UnauthorizedAccessException e)
				{
					throw new InvalidOperationException("Could not add user " + userName + " Performance Monitoring Users group", e);
				}
			}
		}

		public static long? SafelyGetPerformanceCounter(string categoryName, string counterName, string processName)
		{
			try
			{
				if (PerformanceCounterCategory.Exists(categoryName) == false)
					return null;
				var category = new PerformanceCounterCategory(categoryName);
				var instances = category.GetInstanceNames();
				var ravenInstance = instances.FirstOrDefault(x => x == processName);
				if (ravenInstance == null || !category.CounterExists(counterName))
				{
					return null;
				}
				using (var counter = new PerformanceCounter(categoryName, counterName, ravenInstance, readOnly: true))
				{
					return counter.NextSample().RawValue;
				}
			}
			catch (Exception e)
			{
				//Don't log anything here, it's up to the calling code to decide what to do
				return null;
			}
		}
	}
}