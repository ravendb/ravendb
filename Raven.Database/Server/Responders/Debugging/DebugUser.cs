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
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugUser : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/user-info"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "POST" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var principal = context.User;
			if (principal == null)
			{
				context.WriteJson(new
				{
					Remark = "Using anonymous user"
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
					IsAdmin = windowsPrincipal.IsAdministrator()
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
					IsAdmin = principalWithDatabaseAccess.IsAdministrator(),
					principalWithDatabaseAccess.AdminDatabases,
					principalWithDatabaseAccess.ReadOnlyDatabases,
					principalWithDatabaseAccess.ReadWriteDatabases,
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
					IsAdmin = oAuthPrincipal.IsAdministrator(),
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