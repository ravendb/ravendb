// -----------------------------------------------------------------------
//  <copyright file="PerformanceCountersMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
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

			if (userName.StartsWith("IIS")) // if IIS user then current principal is GroupPrincipal
			{
				var acc = new NTAccount(userName);
				var sid = acc.Translate(typeof(SecurityIdentifier));
				userPrincipal = GroupPrincipal.FindByIdentity(machineCtx, IdentityType.Sid, sid.Value);
			}
			else
			{
				userPrincipal = UserPrincipal.Current;
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
	}
}