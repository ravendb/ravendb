using System;
using System.Collections.Concurrent;
using System.DirectoryServices.AccountManagement;
using System.DirectoryServices.ActiveDirectory;
using System.Linq;
using System.Security.Principal;
using System.Threading;
using NLog;
using Raven.Abstractions;

namespace Raven.Database.Extensions
{
	public static class AdminFinder
	{
		private static readonly CachingAdminFinder cachingAdminFinder = new CachingAdminFinder();

		public static bool IsAdministrator(this IPrincipal principal)
		{
			if (principal == null)
				return false;

			var windowsPrincipal = principal as WindowsPrincipal;
			if (windowsPrincipal != null)
			{

				var current = WindowsIdentity.GetCurrent();
				var windowsIdentity = ((WindowsIdentity) windowsPrincipal.Identity);

				// if the request was made using the same user as RavenDB is running as, 
				// we consider this to be an administrator request
				if (current != null && current.User == windowsIdentity.User)
					return true;

				if (windowsPrincipal.IsInRole(WindowsBuiltInRole.Administrator))
					return true;

				if (windowsIdentity.User == null)
					return false; // we aren't sure who this use is, probably anonymous?
				// we still need to make this check, to by pass UAC non elevated admin issue
				return cachingAdminFinder.IsAdministrator(windowsIdentity);
			}

			return principal.IsInRole("Administrators");
		}

		public class CachingAdminFinder
		{
			private static readonly Logger log = LogManager.GetCurrentClassLogger();

			private class CachedResult
			{
				public int Usage;
				public DateTime Timestamp;
				public bool Value;
			}

			private const int CacheMaxSize = 25;
			private static readonly TimeSpan maxDuration = TimeSpan.FromMinutes(15);

			private readonly ConcurrentDictionary<SecurityIdentifier, CachedResult> cache =
				new ConcurrentDictionary<SecurityIdentifier, CachedResult>();

			public bool IsAdministrator(WindowsIdentity windowsIdentity)
			{
				CachedResult value;
				if (cache.TryGetValue(windowsIdentity.User, out value) && (SystemTime.UtcNow - value.Timestamp) <= maxDuration)
				{
					Interlocked.Increment(ref value.Usage);
					return value.Value;
				}
				bool isAdministratorNoCache;
				try
				{
					isAdministratorNoCache = IsAdministratorNoCache(windowsIdentity.Name);
				}
				catch (Exception e)
				{
					log.WarnException("Could not determine whatever user is admin or not, assuming not", e);
					return false;
				}
				var cachedResult = new CachedResult
				{
					Usage = value == null ? 1 : value.Usage + 1,
					Value = isAdministratorNoCache,
					Timestamp = SystemTime.UtcNow
				};

				cache.AddOrUpdate(windowsIdentity.User, cachedResult, (_, __) => cachedResult);
				if (cache.Count > CacheMaxSize)
				{
					foreach (var source in cache
						.OrderByDescending(x => x.Value.Usage)
						.ThenBy(x => x.Value.Timestamp)
						.Skip(CacheMaxSize))
					{
						if (source.Key == windowsIdentity.User)
							continue; // we don't want to remove the one we just added
						CachedResult ignored;
						cache.TryRemove(source.Key, out ignored);
					}
				}

				return isAdministratorNoCache;
			}

			private static bool IsAdministratorNoCache(string username)
			{
				PrincipalContext ctx;
				try
				{
					Domain.GetComputerDomain();
					try
					{
						ctx = new PrincipalContext(ContextType.Domain);
					}
					catch (PrincipalServerDownException)
					{
						// can't access domain, check local machine instead 
						ctx = new PrincipalContext(ContextType.Machine);
					}
				}
				catch (ActiveDirectoryObjectNotFoundException)
				{
					// not in a domain
					ctx = new PrincipalContext(ContextType.Machine);
				}
				var up = UserPrincipal.FindByIdentity(ctx, username);
				if (up != null)
				{
					PrincipalSearchResult<Principal> authGroups = up.GetAuthorizationGroups();
					return authGroups.Any(principal =>
											principal.Sid.IsWellKnown(WellKnownSidType.BuiltinAdministratorsSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountDomainAdminsSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountAdministratorSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountEnterpriseAdminsSid));
				}
				return false;
			}
		} 
	}
}