using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.DirectoryServices.ActiveDirectory;
using System.Linq;
using System.Security.Principal;
using System.Threading;

using System.DirectoryServices.AccountManagement;

// Raven.SpecificPlatform.Windows can reference System only dlls. It should completely avoid referencing Raven's dlls.
namespace Raven.SpecificPlatform.Windows
{	
	public static class RoleFinder
	{
		private static readonly CachingRoleFinder cachingRoleFinder = new CachingRoleFinder();

		public static bool IsInRole(WindowsPrincipal windowsPrincipal, bool isModeAdmin, WindowsBuiltInRole role, DateTime now, 
			Action<string> logDebug, bool isOAuthNull, bool isGlobalAdmin, Action<string, Exception>logWarn)
		{
			if (windowsPrincipal != null)
			{
				var current = WindowsIdentity.GetCurrent();
				var windowsIdentity = ((WindowsIdentity)windowsPrincipal.Identity);

				// if the request was made using the same user as RavenDB is running as, 
				// we consider this to be an administrator request
				if (current != null && current.User == windowsIdentity.User)
					return true;

				if (windowsPrincipal.IsInRole(role))
					return true;

				if (windowsIdentity.User == null)
					return false; // we aren't sure who this use is, probably anonymous?
				// we still need to make this check, to by pass UAC non elevated admin issue
				return cachingRoleFinder.IsInRole(windowsIdentity, role, now, logDebug, isOAuthNull, isGlobalAdmin, logWarn);
			}


			if (isOAuthNull == true)
				return false;

			if (role != WindowsBuiltInRole.Administrator)
				return false;

			return isGlobalAdmin;

		}


		public class CachingRoleFinder
		{
			private class CachedResult
			{
				public int Usage;
				public DateTime Timestamp;
				public Lazy<IList<Principal>> AuthorizationGroups;
			}

			private const int CacheMaxSize = 1024;
			private static readonly TimeSpan maxDuration = TimeSpan.FromMinutes(15);

			private readonly ConcurrentDictionary<SecurityIdentifier, CachedResult> cache = new ConcurrentDictionary<SecurityIdentifier, CachedResult>();

			public bool IsInRole(WindowsIdentity windowsIdentity, WindowsBuiltInRole role, DateTime SystemTimeUtcNow, 
				Action<string> logDebug, bool isOAuthNull, bool isGlobalAdmin, Action<string, Exception>logWarnException)
			{
				CachedResult value;
				if (cache.TryGetValue(windowsIdentity.User, out value) && (SystemTimeUtcNow - value.Timestamp) <= maxDuration)
				{
					Interlocked.Increment(ref value.Usage);
					return IsInRole (value, role, SystemTimeUtcNow, logDebug, isOAuthNull, isGlobalAdmin, logWarnException);
				}

				var cachedResult = new CachedResult
				{
					Usage = value == null ? 1 : value.Usage + 1,
					AuthorizationGroups = new Lazy<IList<Principal>>(() => GetUserAuthorizationGroups(windowsIdentity.Name)),
					Timestamp = SystemTimeUtcNow
				};

				cache.AddOrUpdate(windowsIdentity.User, cachedResult, (_, __) => cachedResult);
				if (cache.Count > CacheMaxSize)
				{
					foreach (var source in cache
							.Where(x => (SystemTimeUtcNow - x.Value.Timestamp) > maxDuration))
					{
						CachedResult ignored;
						cache.TryRemove(source.Key, out ignored);
						string exceptionString = String.Format("Removing expired {0} from cache", source.Key);
						logDebug(exceptionString);
					}
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
							string exceptionString = String.Format("Removing expired {0} from cache", source.Key);
							logDebug(exceptionString);
						}
					}
				}

				return IsInRole(cachedResult, role, SystemTimeUtcNow, 
					logDebug, isOAuthNull, isGlobalAdmin, logWarnException);
			}

			private bool IsInRole(CachedResult cachedResult, WindowsBuiltInRole role, DateTime SystemTimeUtcNow, 
				Action<string> logDebug, bool isOAuthNull, bool isGlobalAdmin, Action<string, Exception>logWarnException)
			{
				try
				{
					var authorizationGroups = cachedResult.AuthorizationGroups.Value;

					switch (role)
					{
						case WindowsBuiltInRole.Administrator:
							return IsAdministratorNoCache(authorizationGroups);
						case WindowsBuiltInRole.BackupOperator:
							return IsBackupOperatorNoCache(authorizationGroups);
						default:
							throw new NotSupportedException(role.ToString());
					}
				}
				catch (Exception e)
				{
					logWarnException("Could not determine whatever user is admin or not, assuming not", e);
					return false;
				}
			}

			private IList<Principal> GetUserAuthorizationGroups(string username)
			{
				var ctx = GeneratePrincipalContext();
				var up = UserPrincipal.FindByIdentity(ctx, IdentityType.SamAccountName, username);
				if (up != null)
				{
					PrincipalSearchResult<Principal> authGroups = up.GetAuthorizationGroups();
					return authGroups.ToList();
				}

				return new List<Principal>();
			}

			private static bool IsAdministratorNoCache(IEnumerable<Principal> authorizationGroups)
			{
				return authorizationGroups.Any(principal =>
											principal.Sid.IsWellKnown(WellKnownSidType.BuiltinAdministratorsSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountDomainAdminsSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountAdministratorSid) ||
											principal.Sid.IsWellKnown(WellKnownSidType.AccountEnterpriseAdminsSid));
			}

			private static bool IsBackupOperatorNoCache(IEnumerable<Principal> authorizationGroups)
			{
				return authorizationGroups.Any(principal =>
											   principal.Sid.IsWellKnown(WellKnownSidType.BuiltinBackupOperatorsSid));
			}/*
		public static bool IsAdministrator(this IPrincipal principal, AnonymousUserAccessMode mode)
		{
			return principal.IsInRole(mode, WindowsBuiltInRole.Administrator);
		}

		public static bool IsBackupOperator(this IPrincipal principal, AnonymousUserAccessMode mode)
		{
			return principal.IsInRole(mode, WindowsBuiltInRole.BackupOperator);
		}
		*/
			

			private static bool? useLocalMachine;
			private static PrincipalContext GeneratePrincipalContext()
			{
				if (useLocalMachine == true)
					return new PrincipalContext(ContextType.Machine);
				try
				{
					if (useLocalMachine == null)
					{
						Domain.GetComputerDomain();
						useLocalMachine = false;
					}
					try
					{
						return new PrincipalContext(ContextType.Domain);
					}
					catch (PrincipalServerDownException)
					{
						// can't access domain, check local machine instead 
						return new PrincipalContext(ContextType.Machine);
					}
				}
				catch(ActiveDirectoryOperationException)
				//catch (ActiveDirectoryObjectNotFoundException)
				{
					useLocalMachine = true;
					// not in a domain
					return new PrincipalContext(ContextType.Machine);
				}
			}
		}
	}
}
