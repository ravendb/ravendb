using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

using Raven.SpecificPlatform.Windows;


// Raven.Database.Extensions.RoleFinder interface to Raven.SpecificPlatform.Windows as Raven should NOT reference System.DirectoryServices.AccountManagement
namespace Raven.Database.Extensions
{	
	public static class RoleFinder
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger(); 

		public class PrimitiveParams
		{
			public PrimitiveParams(IPrincipal principal)
			{
				var oauthPrincipal = principal as OAuthPrincipal;
				if ( oauthPrincipal == null )
					IsOAuthNull = true;
				else
					IsGlobalAdmin = oauthPrincipal.IsGlobalAdmin();
			}
			public bool IsOAuthNull = false;
			public bool IsGlobalAdmin = false;
		}

		public static bool IsInRole(this IPrincipal principal, Raven.Database.Server.AnonymousUserAccessMode mode, WindowsBuiltInRole role)
		{
			var primitiveParameters = new PrimitiveParams(principal);
			if (EnvironmentUtils.RunningOnPosix == false) {			
				bool isModeAdmin = (mode == Raven.Database.Server.AnonymousUserAccessMode.Admin);	

				if (principal == null || principal.Identity == null | principal.Identity.IsAuthenticated == false)
				{
					return isModeAdmin;
				}
				var databaseAccessPrincipal = principal as PrincipalWithDatabaseAccess;
				var windowsPrincipal = databaseAccessPrincipal == null ? principal as WindowsPrincipal : databaseAccessPrincipal.Principal;

				return Raven.SpecificPlatform.Windows.RoleFinder.IsInRole (windowsPrincipal, isModeAdmin, role, SystemTime.UtcNow, log.Debug,
					primitiveParameters.IsOAuthNull,
					primitiveParameters.IsGlobalAdmin,
					log.WarnException);
			} else
				throw new FeatureNotSupportedOnPosixException ("IsInRole is not supported when running on posix");
		}
			
		public static bool IsAdministrator(this IPrincipal principal, Raven.Database.Server.AnonymousUserAccessMode mode)
		{
			var primitiveParameters = new PrimitiveParams(principal);
			if (EnvironmentUtils.RunningOnPosix == false) {
				bool isModeAdmin = (mode == Raven.Database.Server.AnonymousUserAccessMode.Admin);	

				if (principal == null || principal.Identity == null | principal.Identity.IsAuthenticated == false)
				{
					return isModeAdmin;
				}
				var databaseAccessPrincipal = principal as PrincipalWithDatabaseAccess;
				var windowsPrincipal = databaseAccessPrincipal == null ? principal as WindowsPrincipal : databaseAccessPrincipal.Principal;

				return Raven.SpecificPlatform.Windows.RoleFinder.IsInRole (windowsPrincipal, isModeAdmin, WindowsBuiltInRole.Administrator, SystemTime.UtcNow, log.Debug,
					primitiveParameters.IsOAuthNull,
					primitiveParameters.IsGlobalAdmin,
					log.WarnException);					
			}
			else
				throw new FeatureNotSupportedOnPosixException ("IsInRole is not supported when running on posix");
		}

		public static bool IsBackupOperator(this IPrincipal principal, Raven.Database.Server.AnonymousUserAccessMode mode)
		{
			var primitiveParameters = new PrimitiveParams(principal);
			if (EnvironmentUtils.RunningOnPosix == false) {
				bool isModeAdmin = (mode == Raven.Database.Server.AnonymousUserAccessMode.All);

				if (principal == null || principal.Identity == null | principal.Identity.IsAuthenticated == false)
				{
					return isModeAdmin;
				}
				var databaseAccessPrincipal = principal as PrincipalWithDatabaseAccess;
				var windowsPrincipal = databaseAccessPrincipal == null ? principal as WindowsPrincipal : databaseAccessPrincipal.Principal;

				return Raven.SpecificPlatform.Windows.RoleFinder.IsInRole (windowsPrincipal, isModeAdmin, WindowsBuiltInRole.BackupOperator, SystemTime.UtcNow, log.Debug,
					primitiveParameters.IsOAuthNull,
					primitiveParameters.IsGlobalAdmin,
					log.WarnException);					
			}
			else
				throw new FeatureNotSupportedOnPosixException ("IsInRole is not supported when running on posix");
		}

		public class CachingRoleFinder
		{
			private Raven.SpecificPlatform.Windows.RoleFinder.CachingRoleFinder cachingRoleFinder = new Raven.SpecificPlatform.Windows.RoleFinder.CachingRoleFinder ();
			private static readonly ILog log = LogManager.GetCurrentClassLogger(); 

			public bool IsInRole(WindowsIdentity windowsIdentity, WindowsBuiltInRole role)
			{
				if (EnvironmentUtils.RunningOnPosix == false)
					return cachingRoleFinder.IsInRole (windowsIdentity, role, SystemTime.UtcNow, log.Debug,
						false,
						false,
						log.WarnException);
				else
					throw new FeatureNotSupportedOnPosixException ("IsInRole is not supported when running on posix");
			}
		}

		public static bool IsAdministrator(this IPrincipal principal, DocumentDatabase database)
		{
			var name = database.Name ?? "<system>";
			return IsAdministrator(principal, name);
		}

		private static bool isDbAccessAdmin(IPrincipal principal, string databaseNane)
		{
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

		public static bool IsAdministrator(this IPrincipal principal, string databaseNane)
		{
			if (EnvironmentUtils.RunningOnPosix == false) {
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

				return isDbAccessAdmin(principal, databaseNane);
			}
			else
				throw new FeatureNotSupportedOnPosixException ("IsInRole is not supported when running on posix");
		}
	}
}
