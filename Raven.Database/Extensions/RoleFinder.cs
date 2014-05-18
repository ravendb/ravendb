using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.DirectoryServices.AccountManagement;
using System.DirectoryServices.ActiveDirectory;
using System.Linq;
using System.Security.Principal;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Database.Extensions
{
	public static class RoleFinder
	{
		private static readonly CachingRoleFinder cachingRoleFinder = new CachingRoleFinder();

		public static bool IsInRole(this IPrincipal principal, AnonymousUserAccessMode mode, WindowsBuiltInRole role)
		{
			if (principal == null || principal.Identity == null | principal.Identity.IsAuthenticated == false)
			{
				if (mode == AnonymousUserAccessMode.Admin)
					return true;
				return false;
			}

			var databaseAccessPrincipal = principal as PrincipalWithDatabaseAccess;
			var windowsPrincipal = databaseAccessPrincipal == null ? principal as WindowsPrincipal : databaseAccessPrincipal.Principal;

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
				return cachingRoleFinder.IsInRole(windowsIdentity, role);
			}

			return principal.IsInRole(WindowsBuiltInRoleToGroupConverter(role));
		}

		private static string WindowsBuiltInRoleToGroupConverter(WindowsBuiltInRole role)
		{
			switch (role)
			{
				case WindowsBuiltInRole.Administrator:
					return "Administrators";
				case WindowsBuiltInRole.BackupOperator:
					return "BackupOperators";
				default:
					throw new NotSupportedException(role.ToString());
			}
		}

		public static bool IsAdministrator(this IPrincipal principal, AnonymousUserAccessMode mode)
		{
			return IsInRole(principal, mode, WindowsBuiltInRole.Administrator);
		}

		public static bool IsBackupOperator(this IPrincipal principal, AnonymousUserAccessMode mode)
		{
			return IsInRole(principal, mode, WindowsBuiltInRole.BackupOperator);
		}

		public class CachingRoleFinder
		{
			private static readonly ILog log = LogManager.GetCurrentClassLogger();

			private class CachedResult
			{
				public int Usage;
				public DateTime Timestamp;
				public Lazy<IList<Principal>> AuthorizationGroups;
			}

			private const int CacheMaxSize = 1024;
			private static readonly TimeSpan maxDuration = TimeSpan.FromMinutes(15);

			private readonly ConcurrentDictionary<SecurityIdentifier, CachedResult> cache = new ConcurrentDictionary<SecurityIdentifier, CachedResult>();

			public bool IsInRole(WindowsIdentity windowsIdentity, WindowsBuiltInRole role)
			{
				CachedResult value;
				if (cache.TryGetValue(windowsIdentity.User, out value) && (SystemTime.UtcNow - value.Timestamp) <= maxDuration)
				{
					Interlocked.Increment(ref value.Usage);
					return IsInRole(value, role);
				}

				var cachedResult = new CachedResult
				{
					Usage = value == null ? 1 : value.Usage + 1,
					AuthorizationGroups = new Lazy<IList<Principal>>(() => GetUserAuthorizationGroups(windowsIdentity.Name)),
					Timestamp = SystemTime.UtcNow
				};

				cache.AddOrUpdate(windowsIdentity.User, cachedResult, (_, __) => cachedResult);
				if (cache.Count > CacheMaxSize)
				{
					foreach (var source in cache
							.Where(x => (SystemTime.UtcNow - x.Value.Timestamp) > maxDuration))
					{
						CachedResult ignored;
						cache.TryRemove(source.Key, out ignored);
						log.Debug("Removing expired {0} from cache", source.Key);
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
							log.Debug("Removing least used {0} from cache", source.Key);
						}
					}
				}

				return IsInRole(cachedResult, role);
			}

			private bool IsInRole(CachedResult cachedResult, WindowsBuiltInRole role)
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
					log.WarnException("Could not determine whatever user is admin or not, assuming not", e);
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
			}

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
				catch (ActiveDirectoryObjectNotFoundException)
				{
					useLocalMachine = true;
					// not in a domain
					return new PrincipalContext(ContextType.Machine);
				}
			}
		}

		public static bool IsAdministrator(this IPrincipal principal, DocumentDatabase database)
		{
			var name = database.Name ?? "<system>";
			return IsAdministrator(principal, name);
		}

		public static bool IsAdministrator(this IPrincipal principal, string databaseNane)
		{
			var databaseAccessPrincipal = principal as PrincipalWithDatabaseAccess;
			if (databaseAccessPrincipal != null)
			{
				if (databaseAccessPrincipal.AdminDatabases.Any(name => name == "*")
					&& databaseNane != null && databaseNane != Constants.SystemDatabase)
					return true;
				if (databaseAccessPrincipal.AdminDatabases.Any(name => string.Equals(name, databaseNane, StringComparison.InvariantCultureIgnoreCase)))
					return true;
				if (databaseNane == null &&
					databaseAccessPrincipal.AdminDatabases.Any(
						name => string.Equals(name, Constants.SystemDatabase, StringComparison.InvariantCultureIgnoreCase)))
					return true;
				return false;
			}

			var oauthPrincipal = principal as OAuthPrincipal;
			if (oauthPrincipal != null)
			{
				foreach (var dbAccess in oauthPrincipal.TokenBody.AuthorizedDatabases.Where(x => x.Admin))
				{
					if (dbAccess.TenantId == "*" && databaseNane != null && databaseNane != Constants.SystemDatabase)
						return true;
					if (string.Equals(dbAccess.TenantId, databaseNane, StringComparison.InvariantCultureIgnoreCase))
						return true;
					if (databaseNane == null &&
						string.Equals(dbAccess.TenantId, Constants.SystemDatabase, StringComparison.InvariantCultureIgnoreCase))
						return false;
				}
			}


			return false;
		}
	}
}
