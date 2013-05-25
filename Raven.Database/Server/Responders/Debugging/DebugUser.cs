// -----------------------------------------------------------------------
//  <copyright file="DebugUser.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Security.Principal;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Database.Server.Security.OAuth;
using System.Linq;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugUser : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/user-info$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var principal = context.User;
			if (principal == null || principal.Identity == null || principal.Identity.IsAuthenticated == false)
			{
				context.WriteJson(new
				{
					Remark = "Using anonymous user",
					AnonymousAreAdmins = server.SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin
				});
				return;
			}
			var windowsPrincipal = principal as WindowsPrincipal;
			if (windowsPrincipal != null)
			{
				context.WriteJson(new
				{
					Remark = "Using windows auth",
					User = windowsPrincipal.Identity.Name,
					IsAdmin = windowsPrincipal.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode)
				});
				return;
			}

			var principalWithDatabaseAccess = principal as PrincipalWithDatabaseAccess;
			if (principalWithDatabaseAccess != null)
			{
				context.WriteJson(new
				{
					Remark = "Using windows auth",
					User = principalWithDatabaseAccess.Identity.Name,
					IsAdminGlobal = principalWithDatabaseAccess.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode),
					IsAdminCurrentDb = principalWithDatabaseAccess.IsAdministrator(Database),
					Databases = principalWithDatabaseAccess.AdminDatabases.Concat(principalWithDatabaseAccess.ReadOnlyDatabases).Concat(principalWithDatabaseAccess.ReadWriteDatabases)
						.Select(db => new
						{
							Database = db,
							IsAdmin = principal.IsAdministrator(db)
						}),
					principalWithDatabaseAccess.AdminDatabases,
					principalWithDatabaseAccess.ReadOnlyDatabases,
					principalWithDatabaseAccess.ReadWriteDatabases
				});
				return;
			}

			var oAuthPrincipal = principal as OAuthPrincipal;
			if (oAuthPrincipal != null)
			{
				context.WriteJson(new
				{
					Remark = "Using OAuth",
					User = oAuthPrincipal.Name,
					IsAdminGlobal = oAuthPrincipal.IsAdministrator(server.SystemConfiguration.AnonymousUserAccessMode),
					IsAdminCurrentDb = oAuthPrincipal.IsAdministrator(Database),
					Databases = oAuthPrincipal.TokenBody.AuthorizedDatabases
						.Select(db => new
						{
							Database = db.TenantId,
							IsAdmin = principal.IsAdministrator(db.TenantId)
						}),
					oAuthPrincipal.TokenBody,
				});
				return;
			}


			context.WriteJson(new
			{
				Remark = "Unknown auth",
				Principal = principal
			});
		}
	}
}