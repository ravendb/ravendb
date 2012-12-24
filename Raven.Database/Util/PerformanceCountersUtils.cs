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
	using Abstractions.Logging;

	public static class PerformanceCountersUtils
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public static void EnsurePerformanceCountersMonitoringAccess(WindowsIdentity identity)
		{
			if (identity.User == null)
				return;

			var identityPrincipal = new WindowsPrincipal(identity);

			if(identityPrincipal.IsInRole(WindowsBuiltInRole.Administrator))
				return; // administrator already has an access

			var performanceMonitorUsersGroupSid = new SecurityIdentifier(WellKnownSidType.BuiltinPerformanceMonitoringUsersSid, null);

			if(identityPrincipal.IsInRole(performanceMonitorUsersGroupSid))
				return; // it is already added to the group

			var userName = identity.Name;
			var machineCtx = new PrincipalContext(ContextType.Machine);
			Principal userPrincipal;

			if(userName.StartsWith("IIS")) // if IIS user then current principal is GroupPrincipal
			{
				userPrincipal = GroupPrincipal.FindByIdentity(machineCtx, IdentityType.Sid, identity.User.Value);
			}
			else
			{
				userPrincipal = UserPrincipal.Current;
			}

			if (userPrincipal == null)
			{
				log.Error("Could not find principal for user " + identity.Name + " to grant him an access to Performance Counters");
				return;
			}

			var performanceMonitorUsersGroupPrincipal = GroupPrincipal.FindByIdentity(machineCtx, IdentityType.Sid, performanceMonitorUsersGroupSid.Value);

			if (performanceMonitorUsersGroupPrincipal == null)
			{
				log.Error("Could not find principal for Performance Monitoring Users group");
				return;
			}
			try
			{
				if (performanceMonitorUsersGroupPrincipal.Members.Contains(userPrincipal) == false)
				{
					performanceMonitorUsersGroupPrincipal.Members.Add(userPrincipal);
					performanceMonitorUsersGroupPrincipal.Save();
					performanceMonitorUsersGroupPrincipal.Dispose();
				}
			}
			catch (UnauthorizedAccessException e)
			{
				log.ErrorException("Could not add user " + identity.Name + " Performance Monitoring Users group", e);
			}
		}
	}
}